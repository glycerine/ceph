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

  bufferlist p_in, p_out;
  p_in.append("this is my input");

  int ret = ioctx.exec("obj", "jvm", "java_route", p_in, p_out);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(p_in, p_out);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
