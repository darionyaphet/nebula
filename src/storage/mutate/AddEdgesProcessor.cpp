/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/AddEdgesProcessor.h"
#include "base/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>
#include "time/WallClock.h"

namespace nebula {
namespace storage {

void AddEdgesProcessor::process(const cpp2::AddEdgesRequest& req) {
    CHECK_NOTNULL(kvstore_);
    spaceId_ = req.get_space_id();
    auto version =
        std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
    // Switch version to big-endian, make sure the key is in ordered.
    version = folly::Endian::big(version);

    callingNum_ = req.get_parts().size();
    auto iRet = indexMan_->getEdgeIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = iRet.value();
    }

    std::for_each(req.get_parts().begin(), req.get_parts().end(), [&](auto& partEdges) {
        auto partId = partEdges.first;
        const auto &edges = partEdges.second;

        if (indexes_.empty()) {
            std::vector<kvstore::KV> data;
            std::for_each(edges.begin(), edges.end(), [&](auto& edge) {
                VLOG(3) << "PartitionID: " << partId << ", VertexID: " << edge.key.src
                        << ", EdgeType: " << edge.key.edge_type
                        << ", EdgeRanking: " << edge.key.ranking
                        << ", VertexID: " << edge.key.dst
                        << ", EdgeVersion: " << version;
                auto key = NebulaKeyUtils::edgeKey(partId, edge.key.src, edge.key.edge_type,
                                                   edge.key.ranking, edge.key.dst, version);
                data.emplace_back(std::move(key), std::move(edge.get_props()));
            });
        } else {
            auto atomic = [version, partId, edges = std::move(edges), this]() -> std::string {
                return addEdges(version, partId, edges);
            };
            auto callback = [partId, this](kvstore::ResultCode code) {
                handleAsync(spaceId_, partId, code);
            };
            this->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
        }
    });
}

std::string AddEdgesProcessor::addEdges(int64_t version, PartitionID partId,
                                        const std::vector<cpp2::Edge>& edges) {
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();

    std::for_each(edges.begin(), edges.end(), [&](auto& edge) {
        auto prop = edge.get_props();
        auto type = edge.key.edge_type;
        auto srcId = edge.key.src;
        auto rank = edge.key.ranking;
        auto dstId = edge.key.dst;
        VLOG(3) << "PartitionID: " << partId << ", VertexID: " << srcId
                << ", EdgeType: " << type << ", EdgeRanking: " << rank
                << ", VertexID: " << dstId << ", EdgeVersion: " << version;
        auto key = NebulaKeyUtils::edgeKey(partId, srcId, type, rank, dstId, version);

        auto newReader = RowReader::getEdgePropReader(this->schemaMan_,
                                                      prop,
                                                      spaceId_,
                                                      type);
        bool updated = false;
        if (!FLAGS_ignore_index_check_pre_insert) {
            auto original = findOriginalValue(partId, key);
            if (original.ok()) {
                auto originalReader = RowReader::getEdgePropReader(this->schemaMan_,
                                                                   original.value(),
                                                                   spaceId_,
                                                                   type);
                for (auto& index : indexes_) {
                    auto originalIndexKey = makeIndexKey(partId, originalReader.get(), original.value(), index);
                    auto newIndexKey = makeIndexKey(partId, newReader.get(), prop, index);
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
                auto newIndexKey = makeIndexKey(partId, newReader.get(), prop, index);
                batchHolder->put(std::move(newIndexKey), "");
            }
        }
        batchHolder->put(std::move(key), std::move(prop));
    });
    return encodeBatchValue(batchHolder->getBatch());
}

StatusOr<std::string> AddEdgesProcessor::findOriginalValue(PartitionID partId,
                                                           const folly::StringPiece& rawKey) {
    auto prefix = NebulaKeyUtils::edgePrefix(partId,
                                             NebulaKeyUtils::getSrcId(rawKey),
                                             NebulaKeyUtils::getEdgeType(rawKey),
                                             NebulaKeyUtils::getRank(rawKey),
                                             NebulaKeyUtils::getDstId(rawKey));
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(this->spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret)
                   << ", spaceId " << this->spaceId_;
        return Status::EdgeNotFound();
    }
    if (iter && iter->valid()) {
        return iter->val().str();
    }
    return Status::EdgeNotFound();
}

std::string AddEdgesProcessor::makeIndexKey(PartitionID partId,
                                            RowReader* reader,
                                            const folly::StringPiece& rawKey,
                                            std::shared_ptr<nebula::cpp2::IndexItem> index) {
    auto values = collectIndexValues(reader, index->get_fields());
    return NebulaKeyUtils::edgeIndexKey(partId,
                                        index->get_index_id(),
                                        NebulaKeyUtils::getSrcId(rawKey),
                                        NebulaKeyUtils::getRank(rawKey),
                                        NebulaKeyUtils::getDstId(rawKey),
                                        values);
}

}  // namespace storage
}  // namespace nebula
