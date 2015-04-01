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
#include <thread>
#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.
#include "include/types.h"
#include "include/rados/librados.hpp"
#include "cls/tabular/cls_tabular_client.h"

struct table_split {
  std::string oid;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(oid, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(oid, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(table_split)

struct table_state {
  int seq;
  std::string unique_id;
  std::vector<table_split> splits;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(unique_id, bl);
    ::encode(splits, bl);
    ::encode(seq, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(unique_id, bl);
    ::decode(splits, bl);
    ::decode(seq, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(table_state)

struct table {
  librados::IoCtx& ioctx;
  std::string head;
  table_state state;

  /*
   * poll for new splits. this should be replaced by watch/notify.
   */
  std::thread *watch;
  void watch_func() {
    while (1) {
      sleep(1);

      bufferlist bl;
      int ret = ioctx.read(head, bl, 0, 0);
      assert(ret > 0);

      librados::bufferlist::iterator iter = bl.begin();

      table_state new_state;
      ::decode(new_state, iter);

      if (state.seq == new_state.seq) {
        // copy over
        for (unsigned i = 0; i < new_state.splits.size(); i++) {
          std::cout << "table " << head << ": split " << i << ": " << new_state.splits[i].oid << std::endl;
        }
      }
    }
  }

  table(librados::IoCtx& ioctx, std::string head) :
    ioctx(ioctx), head(head) {

      bufferlist bl;
      int ret = ioctx.read(head, bl, 0, 0);
      assert(ret > 0);

      librados::bufferlist::iterator iter = bl.begin();
      ::decode(state, iter);

      for (unsigned i = 0; i < state.splits.size(); i++) {
        std::cout << "table " << head << ": split " << i << ": " << state.splits[i].oid << std::endl;
      }

      watch = new std::thread(&table::watch_func, *this);
  }

  int put(std::string entry) {
    std::vector<std::string> entries;
    entries.push_back(entry);
    return put(entries);
  }

  int put(std::vector<std::string>& entries) {
    librados::ObjectWriteOperation op;
    cls_tabular_put(op, entries);

    int ret = ioctx.operate(head, &op);
    if (ret < 0)
      fprintf(stderr, "ret=%d e=%s\n", ret, strerror(-ret));
    assert(ret == 0);
    return 0;
  }
};

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
  bool create = false;
  bool splitter = false;

  while (1) {
    int c = getopt(argc, argv, "p:o:cs");
    if (c == -1)
      break;
    switch (c) {
      case 'p':
        pool = std::string(optarg);
        break;
      case 'o':
        objname = std::string(optarg);
        break;
      case 'c':
        create = true;
        break;
      case 's':
        splitter = true;
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

  if (create) {
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    std::stringstream uuid_ss;
    uuid_ss << uuid;

    ioctx.remove(objname);
    ioctx.create(objname, true);

    table_state s;
    s.seq = 0;
    s.unique_id = uuid_ss.str();

    uuid = boost::uuids::random_generator()();
    uuid_ss.str("");
    uuid_ss << s.unique_id << "." << uuid;
    table_split split;
    split.oid = uuid_ss.str();
    s.splits.push_back(split);

    bufferlist bl;
    ::encode(s, bl);
    int ret = ioctx.write_full(objname, bl);
    assert(ret == 0);

    return 0;
  }

  std::default_random_engine generator;
  std::uniform_int_distribution<uint64_t> distribution(0, 1ULL<<32);

  table t(ioctx, objname);

  while (1) {
    std::vector<std::string> entries;
    for (unsigned i = 0; i < 1; i++) {
      uint64_t key = distribution(generator);
      entries.push_back(u64tostr(key));

      int ret = t.put(entries);
      assert(ret == 0);
    }

  }

  ioctx.close();
  rados.shutdown();

  return 0;
}
