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
    spaceId_ = req.get_space_id();
    auto version =
        std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
    // Switch version to big-endian, make sure the key is in ordered.
    version = folly::Endian::big(version);

    callingNum_ = req.get_parts().size();
    auto iRet = indexMan_->getTagIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    std::for_each(req.get_parts().begin(), req.get_parts().end(), [&](auto& partVertices) {
        auto part = partVertices.first;
        const auto& vertices = partVertices.second;

        if (indexes_.empty()) {
            std::vector<kvstore::KV> data;
            std::for_each(vertices.begin(), vertices.end(), [&](auto& v) {
                const auto& tags = v.get_tags();
                std::for_each(tags.begin(), tags.end(), [&](auto& tag) {
                    VLOG(3) << "PartitionID: " << part << ", VertexID: " << v.get_id()
                            << ", TagID: " << tag.get_tag_id() << ", TagVersion: " << version;
                    auto key = NebulaKeyUtils::vertexKey(part, v.get_id(),
                                                         tag.get_tag_id(), version);
                    data.emplace_back(std::move(key), std::move(tag.get_props()));
                    if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                        vertexCache_->evict(std::make_pair(v.get_id(), tag.get_tag_id()), part);
                        VLOG(3) << "Evict cache for vId " << v.get_id()
                                << ", tagId " << tag.get_tag_id();
                    }
                });
            });
            doPut(spaceId_, part, std::move(data));
        } else {
            auto atomic = [version, part, vertices = std::move(vertices), this]() -> std::string {
                return addVertices(version, part, vertices);
            };
            auto callback = [part, this](kvstore::ResultCode code) {
                handleAsync(spaceId_, part, code);
            };
            this->kvstore_->asyncAtomicOp(spaceId_, part, atomic, callback);
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

            if (FLAGS_enable_vertex_cache && this->vertexCache_ != nullptr) {
                this->vertexCache_->evict(std::make_pair(vId, tagId), partId);
                VLOG(3) << "Evict cache for vId " << vId << ", tagId " << tagId;
            }

            auto newReader = RowReader::getTagPropReader(schemaMan_,
                                                         prop,
                                                         spaceId_,
                                                         tagId);

            bool updated = false;
            if (!FLAGS_ignore_index_check_pre_insert) {
                auto original = findOriginalValue(partId, vId, tagId);
                if (original.ok() && !original.value().empty()) {
                    auto originalReader = RowReader::getTagPropReader(schemaMan_,
                                                                      original.value(),
                                                                      spaceId_,
                                                                      tagId);
                    for (auto& index : indexes_) {
                        if (tagId != index->get_schema_id().get_tag_id()) {
                            continue;
                        }
                        auto originalIndexKey = makeIndexKey(partId,
                                                             vId,
                                                             originalReader.get(),
                                                             index);
                        auto newIndexKey = makeIndexKey(partId, vId, newReader.get(), index);
                        if (!originalIndexKey.empty() &&
                            originalIndexKey != newIndexKey) {
                            batchHolder->remove(std::move(originalIndexKey));
                            batchHolder->put(std::move(newIndexKey), "");
                        }
                    }
                    updated = true;
                }
            }

            if (!updated) {
                for (auto& index : indexes_) {
                    if (tagId != index->get_schema_id().get_tag_id()) {
                        continue;
                    }
                    auto newIndexKey = makeIndexKey(partId, vId, newReader.get(), index);
                    batchHolder->put(std::move(newIndexKey), "");
                }
            }
            auto key = NebulaKeyUtils::vertexKey(partId, vId, tagId, version);
            batchHolder->put(std::move(key), std::move(prop));
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
    return "";
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
