#include <iostream>
#include <errno.h>

#include "include/rados/librados.hpp"
#include "test/librados/test.h"
#include "gtest/gtest.h"

using namespace librados;

TEST(ClsJvm, EchoParams) {
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

TEST(ClsJvm, EchoWrite) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  const char *data = "this is my input";

  // write data into object
  bufferlist in;
  in.append(data);
  ASSERT_EQ(0, ioctx.write_full("obj", in));

  // read data from object via java handler
  bufferlist p_in, p_out;
  int ret = ioctx.exec("obj", "jvm", "java_route", p_in, p_out);
  ASSERT_EQ(ret, 0);

  // check echo
  in.append(data); // write_full stole contents
  ASSERT_EQ(p_out, in);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsJvm, EchoRead) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  const char *data = "this is my input";

  // read data from object via java handler
  bufferlist p_in, p_out;
  p_in.append(data);
  int ret = ioctx.exec("obj", "jvm", "java_route", p_in, p_out);
  ASSERT_EQ(ret, 0);

  // write data into object
  bufferlist out;
  ASSERT_EQ(p_in.length(), ioctx.read("obj", out, 0, 0));

  // check echo
  ASSERT_EQ(p_in, out);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
