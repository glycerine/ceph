#ifndef CLS_TABULAR_CLIENT_H
#define CLS_TABULAR_CLIENT_H

#include <vector>
#include <string>

#include "include/rados/librados.hpp"

void cls_tabular_put(librados::ObjectWriteOperation& op,
    std::vector<std::string>& entries);

#endif
