#include "include/types.h"
#include "cls/tabular/cls_tabular_ops.h"
#include "cls/tabular/cls_tabular_client.h"
#include "include/rados/librados.hpp"

void cls_tabular_put(librados::ObjectWriteOperation& op,
    std::vector<std::string>& entries)
{
  librados::bufferlist in;
  cls_tabular_put_op call;
  call.entries = entries;
  ::encode(call, in);
  op.exec("tabular", "put", in);
}
