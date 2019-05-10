/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/ScanKeyProcessor.h"

namespace nebula {
namespace meta {

void ScanKeyProcessor::process(const cpp2::ScanReq& req) {
    CHECK_SEGMENT(req.get_segment());
    auto start   = MetaServiceUtils::assembleSegmentKey(req.get_segment(), req.get_start());
    auto end     = MetaServiceUtils::assembleSegmentKey(req.get_segment(), req.get_end());
    auto result  = doScanKey(start, end);

    if (!result.ok()) {
        LOG(ERROR) << "Scan Key Failed from " << start
                   << " to " << end << " " << result.status();
        resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        return;
    }
    LOG(INFO) << "Scan Key from " << start << " to " << end;
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_values(std::move(result.value()));
    onFinished();
}

}  // namespace meta
}  // namespace nebula
