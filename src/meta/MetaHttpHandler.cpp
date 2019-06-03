/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <hdfs/hdfs.h>
#include "meta/MetaHttpHandler.h"
#include "webservice/Common.h"
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>

namespace nebula {
namespace meta {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::UpgradeProtocol;
using proxygen::ResponseBuilder;

void MetaHttpHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }

    if (headers->hasQueryParam("returnjson")) {
        returnJson_ = true;
    }

    if (headers->hasQueryParam("method")) {
        method = headers->getQueryParam("method");
    }

    auto* statusStr = headers->getQueryParamPtr("daemon");
    if (statusStr != nullptr) {
        folly::split(",", *statusStr, statusNames_, true);
    }
}


void MetaHttpHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}


void MetaHttpHandler::onEOM() noexcept {
    switch (err_) {
        case HttpCode::E_UNSUPPORTED_METHOD:
            ResponseBuilder(downstream_)
                .status(405, "Method Not Allowed")
                .sendWithEOM();
            return;
        default:
            break;
    }

    // read meta daemon status
    if (method == "status") {
        folly::dynamic vals = getStatus();
        if (returnJson_) {
            ResponseBuilder(downstream_)
                .status(200, "OK")
                .body(folly::toJson(vals))
                .sendWithEOM();
        } else {
            ResponseBuilder(downstream_)
                .status(200, "OK")
                .body(toString(vals))
                .sendWithEOM();
        }
    } else if (method == "download") {
        auto flag = dispatchSSTFiles("127.0.0.1", 9000, "/data");
        UNUSED(flag);
        ResponseBuilder(downstream_)
            .status(200, "OK")
            .body("download")
            .sendWithEOM();
    } else {
    }
}


void MetaHttpHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
}


void MetaHttpHandler::requestComplete() noexcept {
    delete this;
}


void MetaHttpHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Web service MetaHttpHandler got error : "
               << proxygen::getErrorString(error);
}


void MetaHttpHandler::addOneStatus(folly::dynamic& vals,
                                   const std::string& statusName,
                                   const std::string& statusValue) const {
    folly::dynamic status = folly::dynamic::object();
    status["name"] = statusName;
    status["value"] = statusValue;
    vals.push_back(std::move(status));
}


std::string MetaHttpHandler::readValue(std::string& statusName) {
    folly::toLowerAscii(statusName);
    if (statusName == "status") {
        return "running";
    } else {
        return "unknown";
    }
}


void MetaHttpHandler::readAllValue(folly::dynamic& vals) {
    for (auto& sn : statusAllNames_) {
        auto statusValue = readValue(sn);
        addOneStatus(vals, sn, statusValue);
    }
}


folly::dynamic MetaHttpHandler::getStatus() {
    auto status = folly::dynamic::array();
    if (statusNames_.empty()) {
        // Read all status
        readAllValue(status);
    } else {
        for (auto& sn : statusNames_) {
            auto statusValue = readValue(sn);
            addOneStatus(status, sn, statusValue);
        }
    }
    return status;
}


std::string MetaHttpHandler::toString(folly::dynamic& vals) const {
    std::stringstream ss;
    for (auto& counter : vals) {
        auto& val = counter["value"];
        ss << counter["name"].asString() << "="
           << val.asString()
           << "\n";
    }
    return ss.str();
}

bool MetaHttpHandler::dispatchSSTFiles(const std::string& url, int port,
                                       const std::string& path) {
    auto client = hdfsConnect(url.c_str(), port);
    if (!client) {
        LOG(ERROR) << "Connect to HDFS " << url << ":" << port << " Failed";
        return false;
    }

    LOG(INFO) << "Connect to HDFS " << url << ":" << port << " Successfully";
    if (hdfsExists(client, path.c_str()) == -1) {
        LOG(ERROR) << "SST Files " << path << " not exist";
        return false;
    }

    int size;
    auto *info = hdfsListDirectory(client, path.c_str(), &size);
    for (auto i = 0; i < size; i++) {
        LOG(INFO) << info[i].mName;
    }
    return true;
}

}  // namespace meta
}  // namespace nebula
