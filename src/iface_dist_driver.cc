#include <sstream>
#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>
#include <common/Clock.h>
#include <limits>
#include "include/rados/librados.hpp"

typedef std::numeric_limits< double > dbl;

namespace po = boost::program_options;

static std::string cmd(int seq)
{
  std::string seqstr = boost::lexical_cast<std::string>(seq);
  return std::string("{\"var\": \"lua_class\", \"prefix\": \"osd pool set\", ") +
         std::string("\"val\": \"\nfunction run(input, output)\n  output:append('" + \
             seqstr + "' .. ',' .. cls.clock())\nend\ncls.register(run)\n\", ") +
         std::string("\"pool\": \"rbd\"}");
}

int main(int argc, char **argv)
{
  std::string pool;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("pool", po::value<std::string>(&pool)->required(), "Pool name")
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

  cout.precision(dbl::digits10);

  std::cout << cluster.num_osds() << std::endl;
  std::cout << cluster.primary_osd(pool.c_str(), "asdf5") << std::endl;

  ioctx.close();
  cluster.shutdown();
  return 0;

  //
  int seq = 0;
  for (;;) {
    ceph::bufferlist inbl, outbl;
    std::string outstring;
    double before, after;
    before = ceph_clock_now(NULL);
    ret = cluster.mon_command(cmd(seq), inbl, &outbl, &outstring);
    after = ceph_clock_now(NULL);
    assert(ret == 0);
    std::cout << seq << "," << before << "," << after << std::endl;
    seq++;
  }

  return 0;
}
