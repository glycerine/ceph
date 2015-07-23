#include <sstream>
#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread/thread.hpp>
#include <common/Clock.h>
#include <limits>
#include <sstream>
#include "include/rados/librados.hpp"

#define PRINT_COVERING_SET

typedef std::numeric_limits< double > dbl;

namespace po = boost::program_options;

static std::string cmd(int seq, std::string pool)
{
  std::string seqstr = boost::lexical_cast<std::string>(seq);
  return std::string("{\"var\": \"lua_class\", \"prefix\": \"osd pool set\", ") +
         std::string("\"val\": \"\nfunction run(input, output)\n  output:append('" + \
             seqstr + "' .. ',' .. cls.clock())\nend\ncls.register(run)\n\", ") +
         std::string("\"pool\": \"" + pool + "\"}");
}

static boost::mutex load_lock;
static boost::condition_variable load_cond;
static boost::condition_variable next_seq_cond;

/*
 * outstanding_ios[idx] = outstanding ios for idx
 * objnames[idx] = objname for idx
 * delays[seq][idx] = min delay for seq for idx
 */
static int num_osds;
static std::vector<int> outstanding_ios;
static std::vector<std::string> objnames;
static std::map<int, std::map<int, double> > delays;

struct ExecCompletion {
  librados::AioCompletion *c;
  ceph::bufferlist outbl;
  int idx;
};

static void exec_safe_cb(librados::completion_t cb, void *arg)
{
  ExecCompletion *c = (ExecCompletion*)arg;
  int rv = c->c->get_return_value();
  assert(rv == 0);

  std::string result(c->outbl.c_str(), c->outbl.length());
  size_t split = result.find(",");
  int seq = boost::lexical_cast<int>(result.substr(0, split));
  double time = boost::lexical_cast<double>(result.substr(split+1));

  boost::unique_lock<boost::mutex> l(load_lock);

  assert(--outstanding_ios[c->idx] >= 0);

  std::map<int, std::map<int, double> >::iterator it = delays.find(seq);
  if (it == delays.end()) {
    delays[seq][c->idx] = time;
    it = delays.find(seq);
    assert(it != delays.end());
  } else {
    std::map<int, double>::iterator it2 = it->second.find(c->idx);
    if (it2 == it->second.end())
      it->second[c->idx] = time;
    else
      it2->second = std::min(time, it2->second);
  }

  std::cout << seq << " " << time << std::endl;

  c->c->release();
  delete c;

  // only notify once per seq?
  if (it->second.size() == num_osds)
    next_seq_cond.notify_one();

  load_cond.notify_one();
}

static void load_thread_func(librados::Rados *cluster, librados::IoCtx& ioctx,
    const std::map<int, std::string>& covering_set, int queue_depth)
{
  assert(queue_depth > 0);

  for (std::map<int, std::string>::const_iterator it = covering_set.begin();
      it != covering_set.end(); it++) {
    objnames.push_back(it->second);
    outstanding_ios.push_back(0);
  }
  assert(objnames.size() == num_osds);
  assert(outstanding_ios.size() == num_osds);

  boost::unique_lock<boost::mutex> l(load_lock);

  for (;;) {
    for (int i = 0; i < num_osds; i++) {
      while (outstanding_ios[i] < queue_depth) {
        ExecCompletion *c = new ExecCompletion;
        c->idx = i;
        c->c = cluster->aio_create_completion(c, NULL, exec_safe_cb);
        assert(c->c);

        ceph::bufferlist inbl;
        int ret = ioctx.aio_exec(objnames[i], c->c, "lua", "run", inbl, &c->outbl);
        assert(ret == 0);

        outstanding_ios[i]++;
      }
    }
    load_cond.wait(l);
  }
}

int main(int argc, char **argv)
{
  std::string pool;
  int queue_depth;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("pool", po::value<std::string>(&pool)->required(), "Pool name")
    ("qdepth", po::value<int>(&queue_depth)->default_value(1), "Queue depth")
  ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  cluster.conf_parse_env(NULL);
  int ret = cluster.connect();
  assert(ret == 0);

  // open pool i/o context
  librados::IoCtx ioctx;
  ret = cluster.ioctx_create(pool.c_str(), ioctx);
  assert(ret == 0);

  /*
   * Compute covering set of objects
   */
  num_osds = cluster.num_osds();
  assert(num_osds > 0);

  int counter = 0;
  std::map<int, std::string> covering_set;
  while (covering_set.size() != num_osds) {
    std::stringstream ss;
    ss << "obj." << counter++;
    std::string objname = ss.str();
    int osd = cluster.primary_osd(pool.c_str(), objname.c_str());
    assert(osd >= 0);
    std::map<int, std::string>::const_iterator it = covering_set.find(osd);
    if (it == covering_set.end()) {
      covering_set[osd] = objname;
      continue;
    }
    if (counter > (num_osds * 10)) {
      std::cout << "failing to find covering set. consider increasing pg_num/pgp_num" << std::endl;
      ioctx.close();
      cluster.shutdown();
      return 1;
    }
  }

#ifdef PRINT_COVERING_SET
  for (std::map<int, std::string>::const_iterator it = covering_set.begin();
       it != covering_set.end(); it++) {
    assert(it->first == cluster.primary_osd(pool.c_str(), it->second.c_str()));
    std::cout << it->first << ": " << it->second << std::endl;
  }
#endif

  cout.precision(dbl::digits10);

  boost::thread thread(load_thread_func, &cluster, ioctx, covering_set, queue_depth);

  for (int seq = 0; 1; seq++) {
    {
      boost::unique_lock<boost::mutex> l(load_lock);
      next_seq_cond.wait(l);
    }

    ceph::bufferlist inbl, outbl;
    std::string outstring;
    ret = cluster.mon_command(cmd(seq, pool), inbl, &outbl, &outstring);
    assert(ret == 0);
  }

  thread.join();

  ioctx.close();
  cluster.shutdown();
  return 0;

#if 0
  /*
   * Use num_osds and primary_osd above to compute a set of object names that
   * map to a minimal covering set of the osds.
   *
   * Create a thread that keeps multiple outstanding operations on each of the
   * objects in the covering set computed above. We want multiple per so that
   * we minimize the delay between a change to the lua interface and a new
   * request that notices the change.
   *
   * we keep a mapping between sequence number of the smallest delay for each
   * OSD. this is constantly updated by the outstanding requests as they
   * complete.
   *
   * each time a new sequence number is generated and lua script is installed
   * we block until we've got a delay reading for each osd. then run another
   * experiment. how long should we wait to make sure we have the minimal
   * value for each osd? we _probably_ only need on reading reasoning that
   * things happen in order, but maybe there is a race. it is probably safe
   * to just wait for 2 or 3 responses.
   */

  //
  int seq = 0;
  for (;;) {
    ceph::bufferlist inbl, outbl;
    std::string outstring;
    double before, after;

    // to remove noise, we might want to take the timestamps on the monitor
    // and parse them out from the outstring here. we can do this if there is
    // actually a lot of noise. its probably quite minimal.

    before = ceph_clock_now(NULL);
    ret = cluster.mon_command(cmd(seq), inbl, &outbl, &outstring);
    after = ceph_clock_now(NULL);
    assert(ret == 0);
    std::cout << seq << "," << before << "," << after << std::endl;
    seq++;
  }

  return 0;
#endif
}
