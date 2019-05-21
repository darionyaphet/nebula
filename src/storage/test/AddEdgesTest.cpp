/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/AddEdgesProcessor.h"
#include "storage/KeyUtils.h"


namespace nebula {
namespace storage {

TEST(AddEdgesTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    auto* processor = AddEdgesProcessor::instance(kv.get(), nullptr);
    LOG(INFO) << "Build AddEdgesRequest...";
    cpp2::AddEdgesRequest req;
    req.space_id = 0;
    req.over_writable = true;
    // partId => List<Edge>
    // Edge => {EdgeKey, props}
    for (auto partId = 0; partId < 3; partId++) {
        std::vector<cpp2::Edge> edges;
        for (auto srcId = partId * 10; srcId < 10 * (partId + 1); srcId++) {
            std::vector<std::string> names;
            names.emplace_back(folly::stringPrintf("column_%d", srcId));
            std::vector<cpp2::PropValue> values;
            values.resize(1);
            values[0].set_string_val(folly::stringPrintf("%d_%d", partId, srcId));
            edges.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                               cpp2::EdgeKey(apache::thrift::FragileConstructor::FRAGILE,
                                             srcId, srcId*100 + 1, srcId*100 + 2, srcId*100 + 3),
                               std::move(names), std::move(values));
        }
        req.parts.emplace(partId, std::move(edges));
    }

    LOG(INFO) << "Test AddEdgesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_codes.size());

    LOG(INFO) << "Check data in kv store...";
    for (auto partId = 0; partId < 3; partId++) {
        for (auto srcId = 10 * partId; srcId < 10 * (partId + 1); srcId++) {
            auto prefix = KeyUtils::prefix(partId, srcId, srcId*100 + 1);
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, partId, prefix, &iter));
            int num = 0;
            while (iter->valid()) {
                nebula::RowWriter writer;
                writer << folly::stringPrintf("%d_%d", partId, srcId);
                EXPECT_EQ(writer.encode(), iter->val());
                num++;
                iter->next();
            }
            EXPECT_EQ(1, num);
        }
    }
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


