#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <sstream>
#include <iostream>
#include <random>
#include "include/rados/librados.hpp"
#include "cls/tabular/cls_tabular_client.h"

/*
 *  * Convert value into zero-padded string for omap comparisons.
 *   */
static inline std::string u64tostr(uint64_t value)
{
  std::stringstream ss;
  ss << std::setw(20) << std::setfill('0') << value;
  return ss.str();
}

static void open_ioctx(std::string &pool, librados::Rados &rados, librados::IoCtx &ioctx)
{
  int ret = rados.init(NULL);
  assert(ret == 0);

  rados.conf_read_file(NULL);
  assert(ret == 0);

  rados.connect();
  assert(ret == 0);

  ret = rados.ioctx_create(pool.c_str(), ioctx);
  assert(ret == 0);
}

static void usage(const char *e)
{
  fprintf(stdout, "%s -p pool\n", e);
}

int main(int argc, char **argv)
{
  std::string pool, objname;

  while (1) {
    int c = getopt(argc, argv, "p:o:");
    if (c == -1)
      break;
    switch (c) {
      case 'p':
        pool = std::string(optarg);
        break;
      case 'o':
        objname = std::string(optarg);
        break;
      default:
        usage(argv[0]);
        exit(1);
    }
  }

  fprintf(stdout, "conf: objname=%s\n",
      objname.c_str());

  // connect to rados
  librados::Rados rados;
  librados::IoCtx ioctx;
  open_ioctx(pool, rados, ioctx);

  std::default_random_engine generator;
  std::uniform_int_distribution<uint64_t> distribution(0, 1ULL<<32);

  while (1) {
    std::vector<std::string> entries;
    for (unsigned i = 0; i < 1; i++) {
      uint64_t key = distribution(generator);
      entries.push_back(u64tostr(key));
    }

    librados::ObjectWriteOperation op;
    cls_tabular_put(op, entries);

    int ret = ioctx.operate(objname, &op);
    assert(ret == 0);
  }

  ioctx.close();
  rados.shutdown();

  return 0;
}
