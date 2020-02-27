/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/SimpleConcurrentJobExecutor.h"
#include <bits/stdint-intn.h>
#include "meta/MetaServiceUtils.h"
#include "meta/processors/Common.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

SimpleConcurrentJobExecutor::
SimpleConcurrentJobExecutor(nebula::cpp2::AdminCmd cmd,
                            std::vector<std::string> params) :
                            cmd_(cmd),
                            params_(params) {
}

ErrorOr<nebula::kvstore::ResultCode, std::map<HostAddr, Status>>
SimpleConcurrentJobExecutor::execute(int spaceId,
                                     int jobId,
                                     const std::vector<std::string>& jobParas,
                                     nebula::kvstore::KVStore* kvStore,
                                     nebula::thread::GenericThreadPool* pool) {
    UNUSED(pool);
    if (jobParas.empty()) {
        LOG(ERROR) << "SimpleConcurrentJob should have a para";
        return nebula::kvstore::ResultCode::ERR_INVALID_ARGUMENT;
    }

    std::unique_ptr<kvstore::KVIterator> iter;
    auto partPrefix = MetaServiceUtils::partPrefix(spaceId);
    auto rc = kvStore->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
    if (rc != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Fetch Parts Failed";
        return rc;
    }

    // use vector not set because this can convient for next step
    std::vector<HostAddr> hosts;
    while (iter->valid()) {
        for (auto& host : MetaServiceUtils::parsePartVal(iter->val())) {
            hosts.emplace_back(std::make_pair(host.get_ip(), host.get_port()));
        }
        iter->next();
    }
    std::sort(hosts.begin(), hosts.end());
    auto last = std::unique(hosts.begin(), hosts.end());
    hosts.erase(last, hosts.end());

    std::vector<folly::SemiFuture<Status>> futures;
    std::unique_ptr<AdminClient> client(new AdminClient(kvStore));
    int taskId = 0;
    std::vector<PartitionID> parts;
    for (auto& host : hosts) {
        auto future = client->addTask(cmd_, jobId, taskId++, spaceId,
                                      {host}, 0, parts);
        futures.push_back(std::move(future));
    }

    std::vector<Status> results;
    nebula::Status errorStatus;
    folly::collectAll(std::move(futures))
        .thenValue([&](const std::vector<folly::Try<Status>>& tries) {
            Status status;
            for (const auto& t : tries) {
                if (t.hasException()) {
                    LOG(ERROR) << "admin Failed: " << t.exception();
                    results.push_back(nebula::Status::Error());
                } else {
                    results.push_back(t.value());
                }
            }
        }).thenError([&](auto&& e) {
            LOG(ERROR) << "admin Failed: " << e.what();
            errorStatus = Status::Error(e.what());
        }).wait();

    if (hosts.size() != results.size()) {
        LOG(ERROR) << "return ERR_UNKNOWN: hosts.size()=" << hosts.size()
                   << ", results.size()=" << results.size();
        return nebula::kvstore::ResultCode::ERR_UNKNOWN;
    }

    std::map<HostAddr, Status> ret;
    for (size_t i = 0; i != hosts.size(); ++i) {
        ret.insert(std::make_pair(hosts[i], results[i]));
    }
    return ret;
}

void SimpleConcurrentJobExecutor::stop() {
}

}  // namespace meta
}  // namespace nebula
