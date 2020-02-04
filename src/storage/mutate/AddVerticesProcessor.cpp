/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/AddVerticesProcessor.h"
#include "base/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>
#include "time/WallClock.h"

DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {

void AddVerticesProcessor::process(const cpp2::AddVerticesRequest& req) {
    CHECK_NOTNULL(kvstore_);
    auto version =
        std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
    // Switch version to big-endian, make sure the key is in ordered.
    version = folly::Endian::big(version);

    const auto& partVertices = req.get_parts();
    spaceId_ = req.get_space_id();
    callingNum_ = partVertices.size();
    auto iRet = indexMan_->getTagIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = iRet.value();
    }

    std::for_each(partVertices.begin(), partVertices.end(), [&](auto& pv) {
        auto partId = pv.first;
        const auto& vertices = pv.second;

        if (indexes_.empty()) {
            std::vector<kvstore::KV> data;
            std::for_each(vertices.begin(), vertices.end(), [&](auto& v) {
                const auto& tags = v.get_tags();
                std::for_each(tags.begin(), tags.end(), [&](auto& tag) {
                    VLOG(3) << "PartitionID: " << partId << ", VertexID: " << v.get_id()
                            << ", TagID: " << tag.get_tag_id() << ", TagVersion: " << version;
                    auto key = NebulaKeyUtils::vertexKey(partId, v.get_id(),
                                                         tag.get_tag_id(), version);
                    data.emplace_back(std::move(key), std::move(tag.get_props()));
                    if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                        vertexCache_->evict(std::make_pair(v.get_id(), tag.get_tag_id()), partId);
                        VLOG(3) << "Evict cache for vId " << v.get_id()
                                << ", tagId " << tag.get_tag_id();
                    }
                });
            });
            doPut(spaceId_, partId, std::move(data));
        } else {
            auto atomic = [&]() -> std::string {
                return addVertices(version, partId, vertices);
            };
            auto callback = [partId, this](kvstore::ResultCode code) {
                handleAsync(spaceId_, partId, code);
            };
            this->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
        }
    });
}

std::string AddVerticesProcessor::addVertices(int64_t version, PartitionID partId,
                                              const std::vector<cpp2::Vertex>& vertices) {
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    std::for_each(vertices.begin(), vertices.end(), [&](auto& v) {
        auto vId = v.get_id();
        const auto& tags = v.get_tags();
        std::for_each(tags.begin(), tags.end(), [&](auto& tag) {
            auto tagId = tag.get_tag_id();
            auto prop = tag.get_props();
            VLOG(3) << "PartitionID: " << partId << ", VertexID: " << vId
                    << ", TagID: " << tagId << ", TagVersion: " << version;

            auto key = NebulaKeyUtils::vertexKey(partId, vId, tagId, version);
            batchHolder->put(std::move(key), std::move(prop));
            if (FLAGS_enable_vertex_cache && this->vertexCache_ != nullptr) {
                this->vertexCache_->evict(std::make_pair(vId, tagId), partId);
                VLOG(3) << "Evict cache for vId " << vId << ", tagId " << tagId;
            }

            // auto value = findObsoleteIndex(partId, vId, tagId);
            // std::unique_ptr<RowReader> nReader;
            auto original = findOriginalValue(partId, vId, tagId);
            if (original.ok()) {
                auto originalReader = RowReader::getTagPropReader(this->schemaMan_,
                                                                  original,
                                                                  spaceId_,
                                                                  tagId);
                auto newReader = RowReader::getTagPropReader(this->schemaMan_,
                                                             prop,
                                                             spaceId_,
                                                             tagId);
                

                for (auto& index : indexes_) {
                    auto originalIndexKey = makeIndexKey(partId, vId, originalReader.get(), index);
                    auto newIndexKey = makeIndexKey(partId, vId, newReader.get(), index);
                    if (!originalIndexKey.empty() &&
                        originalIndexKey != newIndexKey) {
                        batchHolder->remove(std::move(originalIndexKey));
                        batchHolder->put(std::move(newIndexKey), "");
                    }
                }
            }

            for (auto& index : indexes_) {
                
                auto current = makeCurrentIndexValue();
                if (!original.empty() && (original != current)) {
                    auto reader = RowReader::getTagPropReader(this->schemaMan_,
                                                              original,
                                                              spaceId_,
                                                              tagId);
                    auto oi = makeIndexKey(partId, vId, reader.get(), index);
                    if (!oi.empty()) {
                        batchHolder->remove(std::move(oi));
                    }
                    
                    batchHolder->put(std::move(ni), "");
                }

                batchHolder->put(std::move(key), std::move(prop));
            }
        });
    });
    return encodeBatchValue(batchHolder->getBatch());
}

StatusOr<std::string> AddVerticesProcessor::findOriginalValue(PartitionID partId,
                                                              VertexID vId,
                                                              TagID tagId) {
    auto prefix = NebulaKeyUtils::vertexPrefix(partId, vId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(this->spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret)
                   << ", spaceId " << this->spaceId_;
        return Status::TagNotFound();
    }
    if (iter && iter->valid()) {
        return iter->val().str();
    }
    return Status::TagNotFound();
}

std::string AddVerticesProcessor::makeIndexKey(PartitionID partId,
                                               VertexID vId,
                                               RowReader* reader,
                                               std::shared_ptr<nebula::cpp2::IndexItem> index) {
    auto values = collectIndexValues(reader, index->get_fields());
    return NebulaKeyUtils::vertexIndexKey(partId,
                                          index->get_index_id(),
                                          vId, values);
}

}  // namespace storage
}  // namespace nebula
