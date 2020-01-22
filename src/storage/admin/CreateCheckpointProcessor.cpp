/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/CreateCheckpointProcessor.h"

namespace nebula {
namespace storage {

void CreateCheckpointProcessor::process(const cpp2::CreateCPRequest& req) {
    CHECK_NOTNULL(kvstore_);
    auto spaceId = req.get_space_id();
    auto& name = req.get_name();
    auto code = kvstore_->createCheckpoint(spaceId, std::move(name));
    this->pushResultCode(code);
    onFinished();
}

}  // namespace storage
}  // namespace nebula


