/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/ExecutionContext.h"

namespace nebula {
namespace graph {

ExecutionContext::~ExecutionContext() {
    if (nullptr != sm_) {
        sm_ = nullptr;
    }

    if (nullptr != storage_) {
        storage_ = nullptr;
    }

    if (nullptr != metaClient_) {
        metaClient_ = nullptr;
    }
}

}   // namespace graph
}   // namespace nebula
