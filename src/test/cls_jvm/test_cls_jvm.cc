#include <iostream>
#include <errno.h>

#include "include/rados/librados.hpp"
#include "test/librados/test.h"
#include "gtest/gtest.h"

using namespace librados;

TEST(ClsJvm, TestSimple) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;
  ASSERT_EQ(0, ioctx.write_full("obj", in));
  ioctx.exec("obj", "jvm", "java_route", in, out);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
