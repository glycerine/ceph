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
static boost::condition_variable load_cond; // fill aio queue
static boost::condition_variable next_seq_cond; // generate new seq
static int curr_seq;
static boost::condition_variable load_thread_started; // load thread started

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

  //std::cout << seq << " " << time << std::endl;

  c->c->release();
  delete c;

  // wait for 2-3 responses for each osd?
  it = delays.find(curr_seq);
  if (it != delays.end() && it->second.size() == num_osds) {
    //std::cout << "notifying complete for seq: " << curr_seq << std::endl;
    next_seq_cond.notify_one();
  }

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
  load_thread_started.notify_one();

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

/*
 * Make sure an initial script is installed on each OSD
 */
static void prime_osds(librados::Rados& cluster, librados::IoCtx& ioctx,
    const std::map<int, std::string>& covering_set, int seq, std::string pool)
{
  ceph::bufferlist inbl, outbl;
  std::string outstring;
  int ret = cluster.mon_command(cmd(seq, pool), inbl, &outbl, &outstring);
  assert(ret == 0);

  std::vector<std::string> objnames;
  for (std::map<int, std::string>::const_iterator it = covering_set.begin();
      it != covering_set.end(); it++) {
    objnames.push_back(it->second);
  }

  // loop until the script is installed
  while (1) {
    if (objnames.empty())
      break;

    std::random_shuffle(objnames.begin(), objnames.end());
    std::string objname = objnames.back();

    ceph::bufferlist inbl, outbl;
    int ret = ioctx.exec(objname, "lua", "run", inbl, outbl);
    if (ret < 0) {
      if (ret == -EOPNOTSUPP) {
        continue;
      } else
        assert(0);
    }
    objnames.pop_back();
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
  {
    boost::unique_lock<boost::mutex> l(load_lock);
    num_osds = cluster.num_osds();
    assert(num_osds > 0);
  }

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

  /*
   * Ensure that a script is initially installed on all osds so that we don't
   * need a special case for dealing with errors during start-up.
   */
  const int initial_seq = 0;
  curr_seq = -1;
  prime_osds(cluster, ioctx, covering_set, initial_seq, pool);

  /*
   * Start the load generator
   */
  boost::thread thread;
  {
    boost::unique_lock<boost::mutex> l(load_lock);
    thread = boost::thread(load_thread_func, &cluster, ioctx, covering_set, queue_depth);
    load_thread_started.wait(l);
  }

  /*
   * Generate new sequences in lock step.
   */
  for (int seq = initial_seq+1; 1; seq++) {
    {
      boost::unique_lock<boost::mutex> l(load_lock);
      curr_seq = seq;
    }

    // setup script for @seq number
    //std::cout << "installed new seq: " << seq << std::endl;
    ceph::bufferlist inbl, outbl;
    std::string outstring;
    ret = cluster.mon_command(cmd(seq, pool), inbl, &outbl, &outstring);
    assert(ret == 0);

    size_t pos1 = outstring.find("before_prop=");
    size_t pos2 = outstring.find("after_prop=");
    uint64_t before_prop = boost::lexical_cast<uint64_t>(outstring.substr(pos1+12, pos2-(pos1+13)));
    uint64_t after_prop = boost::lexical_cast<uint64_t>(outstring.substr(pos2+11));

    // this races with notify_one in the callback, but it is safe because once
    // the current sequence is complete the callback will continue to issue
    // notifies until the sequence number is changed.
    std::vector<double> seq_delays;
    {
      boost::unique_lock<boost::mutex> l(load_lock);
      //std::cout << "waiting on seq: " << seq << std::endl;
      next_seq_cond.wait(l);

      // grab a copy of the delays for this sequence
      std::map<int, std::map<int, double> >::const_iterator it = delays.find(seq);
      assert(it != delays.end());
      assert(it->second.size() == num_osds);
      for (std::map<int, double>::const_iterator it2 = it->second.begin();
           it2 != it->second.end(); it2++) {
        seq_delays.push_back(it2->second);
      }
    }

    // we now have a timestamp for every osd. we can compute the time delays
    // and then install a new sequence number
    double min, max, sum = 0.0;
    assert(seq_delays.size() == num_osds);
    for (unsigned i = 0; i < seq_delays.size(); i++) {
      double val = seq_delays[i];
      if (i == 0) {
        min = val;
        max = val;
      }
      min = std::min(min, val);
      max = std::max(max, val);
      sum += val;
    }
    double avg = sum / (double)seq_delays.size();
    std::cout << seq << " " << 
      ((double)before_prop / (double)1000000000.0L) << " " <<
      ((double)after_prop / (double)1000000000.0L) << " " <<
      min << " " << max << " " << avg << std::endl;
  }

  thread.join();

  ioctx.close();
  cluster.shutdown();
  return 0;
}
