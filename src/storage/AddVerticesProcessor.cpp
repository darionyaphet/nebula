/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "storage/AddVerticesProcessor.h"
#include <algorithm>
#include <limits>
#include "time/TimeUtils.h"
#include "storage/KeyUtils.h"

namespace nebula {
namespace storage {

void AddVerticesProcessor::process(const cpp2::AddVerticesRequest& req) {
    VLOG(3) << "Receive AddVerticesRequest...";
    auto now = std::numeric_limits<int64_t>::max() - time::TimeUtils::nowInUSeconds();
    const auto& partVertices = req.get_parts();
    auto spaceId = req.get_space_id();
    callingNum_ = partVertices.size();
    CHECK_NOTNULL(kvstore_);
    std::for_each(partVertices.begin(), partVertices.end(), [&](auto& pv) {
        auto partId = pv.first;
        const auto& vertices = pv.second;
        std::vector<kvstore::KV> data;
        std::for_each(vertices.begin(), vertices.end(), [&](auto& v){
            const auto& tags = v.get_tags();
            std::for_each(tags.begin(), tags.end(), [&](auto& tag) {
                auto key = KeyUtils::vertexKey(partId, v.get_id(),
                                               tag.get_tag_id(), now);
                data.emplace_back(std::move(key), std::move(tag.get_props()));
            });
        });
        doPut(spaceId, partId, std::move(data));
    });
}

}  // namespace storage
}  // namespace nebula
