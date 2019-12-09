/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/GetProcessor.h"
#include "base/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

void GetProcessor::process(const cpp2::GetRequest& req) {
    space_ = req.get_space_id();
    std::vector<folly::Future<std::pair<PartitionID, kvstore::ResultCode>>> results;
    for (auto& part : req.get_parts()) {
        results.emplace_back(asyncProcess(part.first, std::move(part.second)));
    }

    folly::collectAll(results).via(executor_)
        .thenValue([&] (const TryPartitionCodes& tries) mutable {
        for (const auto& t : tries) {
            auto ret = t.value();
            auto part = std::get<0>(ret);
            auto resultCode = std::get<1>(ret);
            if (resultCode == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                this->handleLeaderChanged(space_, part);
            } else {
                this->pushResultCode(this->to(resultCode), part);
            }
        }

        resp_.set_values(std::move(pairs_));
        this->onFinished();
    });
}

folly::Future<PartitionCode>
GetProcessor::asyncProcess(PartitionID part, std::vector<std::string> keys) {
    folly::Promise<std::pair<PartitionID, kvstore::ResultCode>> promise;
    auto future = promise.getFuture();
    std::vector<std::string> generalKeys;
    generalKeys.reserve(keys.size());
    std::transform(keys.begin(), keys.end(), std::back_inserter(generalKeys),
                   [part](const auto& key) { return NebulaKeyUtils::generalKey(part, key); });

    executor_->add([this, p = std::move(promise), part, keys = std::move(keys),
                    generalKeys = std::move(generalKeys)] () mutable {
        std::vector<std::string> values;
        auto ret = this->kvstore_->multiGet(space_, part, generalKeys, &values);
        if (ret == kvstore::ResultCode::SUCCEEDED) {
            std::lock_guard<std::mutex> lg(this->lock_);
            for (int32_t i = 0; i < static_cast<int32_t>(keys.size()); i++) {
                pairs_.emplace(keys[i], values[i]);
            }
        }
        p.setValue(std::make_pair(part, ret));
    });
    return future;
}

}  // namespace storage
}  // namespace nebula
