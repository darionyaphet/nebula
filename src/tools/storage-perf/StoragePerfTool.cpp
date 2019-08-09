/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "thread/GenericWorker.h"
#include "time/Duration.h"
#include "storage/client/StorageClient.h"
#include <string>

DEFINE_int32(threads, 2, "Total threads for perf");
DEFINE_int32(qps, 1000, "Total qps for the perf tool");
DEFINE_int32(totalReqs, 10000, "Total requests during this perf test");
DEFINE_int32(io_threads, 10, "Client io threads");
DEFINE_string(method, "getNeighbors", "method type being tested,"
                                      "such as getNeighbors, addVertices, addEdges, getVertices");
DEFINE_string(meta_server_addrs, "", "meta server address");
DEFINE_int32(min_vertex_id, 1, "The smallest vertex Id");
DEFINE_int32(max_vertex_id, 10000, "The biggest vertex Id");
DEFINE_int32(size, 1000, "The data's size per request");
DEFINE_int64(default_space_id, 1, "Default spaceid");
DEFINE_int64(default_tag_id, 2, "Default tag id");
DEFINE_int64(default_edge_type, 3, "Default edge type");

namespace nebula {
namespace storage {

class Perf {
public:
    int run() {
        uint32_t qpsPerThread = FLAGS_qps / FLAGS_threads;
        uint32_t interval = 1;
        if (qpsPerThread < 1000) {
            interval = 1000 / qpsPerThread;
        }

        auto messagePerRequest = qpsPerThread / 1000;
        LOG(INFO) << "Total threads " << FLAGS_threads
                  << ", qpsPerThread " << qpsPerThread
                  << ", task interval ms " << interval
                  << ", send " << messagePerRequest << " message per request";
        auto metaAddrsRet = nebula::network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
        if (!metaAddrsRet.ok() || metaAddrsRet.value().empty()) {
            LOG(ERROR) << "Can't get metaServer address, status:" << metaAddrsRet.status()
                       << ", FLAGS_meta_server_addrs:" << FLAGS_meta_server_addrs;
            return -1;
        }

        std::vector<std::unique_ptr<thread::GenericWorker>> threads;
        for (int32_t i = 0; i < FLAGS_threads; i++) {
            auto t = std::make_unique<thread::GenericWorker>();
            threads.emplace_back(std::move(t));
        }
        threadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_io_threads);
        mClient_ = std::make_unique<meta::MetaClient>(threadPool_, metaAddrsRet.value());
        CHECK(mClient_->waitForMetadReady());
        client_ = std::make_unique<StorageClient>(threadPool_, mClient_.get());
        time::Duration duration;
        for (auto& t : threads) {
            CHECK(t->start("TaskThread"));
            if (FLAGS_method == "getNeighbors") {
                t->addRepeatTask(interval, &Perf::getNeighborsTask, this);
            } else if (FLAGS_method == "addVertices") {
                t->addRepeatTask(interval, &Perf::addVerticesTask, this, messagePerRequest);
            } else if (FLAGS_method == "addEdges") {
                t->addRepeatTask(interval, &Perf::addEdgesTask, this, messagePerRequest);
            } else if (FLAGS_method == "getVertices") {
                t->addRepeatTask(interval, &Perf::getVerticesTask, this);
            } else {
                LOG(FATAL) << "Unknown method name " << FLAGS_method;
            }
        }
        while (finishedRequests_ < FLAGS_totalReqs) {
            if (finishedRequests_ % 1000 == 0) {
                LOG(INFO) << "Total " << FLAGS_totalReqs << ", finished " << finishedRequests_;
            }
            usleep(1000 * 30);
        }
        for (auto& t : threads) {
            t->stop();
        }
        for (auto& t : threads) {
            t->wait();
        }
        LOG(INFO) << "Total time cost " << duration.elapsedInMSec() << "ms, "
                  << "total requests " << finishedRequests_;
        return 0;
    }

private:
    std::vector<VertexID> randomVertices() {
        return {FLAGS_min_vertex_id
                + folly::Random::rand32(FLAGS_max_vertex_id - FLAGS_min_vertex_id)};
    }

    std::vector<storage::cpp2::PropDef> randomCols() {
        std::vector<storage::cpp2::PropDef> props;
        {
            storage::cpp2::PropDef prop;
            prop.set_name(folly::stringPrintf("tag_%ld_col_1", FLAGS_default_tag_id));
            prop.set_owner(storage::cpp2::PropOwner::SOURCE);
            prop.set_tag_id(FLAGS_default_tag_id);
            props.emplace_back(std::move(prop));
        }
        {
            storage::cpp2::PropDef prop;
            prop.set_name("col_1");
            prop.set_owner(storage::cpp2::PropOwner::EDGE);
            props.emplace_back(std::move(prop));
        }
        return props;
    }

    std::string genData(int32_t size) {
        return std::string(size, ' ');
    }

    std::vector<storage::cpp2::Vertex> genVertices(uint32_t number) {
        std::vector<storage::cpp2::Vertex> vertices;
        static VertexID vId = FLAGS_min_vertex_id;
        for (uint32_t i = 0; i < number; i++) {
            storage::cpp2::Vertex v;
            v.set_id(vId++);
            decltype(v.tags) tags;
            storage::cpp2::Tag tag;
            tag.set_tag_id(FLAGS_default_tag_id);
            auto props = genData(FLAGS_size);
            tag.set_props(std::move(props));
            tags.emplace_back(std::move(tag));
            v.set_tags(std::move(tags));
            vertices.emplace_back(std::move(v));
        }
        return vertices;
    }

    std::vector<storage::cpp2::Edge> genEdges(uint32_t number) {
        std::vector<storage::cpp2::Edge> edges;
        static VertexID vId = FLAGS_min_vertex_id;
        for (uint32_t i = 0; i< number; i++) {
            storage::cpp2::Edge edge;
            storage::cpp2::EdgeKey eKey;
            eKey.set_src(vId);
            eKey.set_edge_type(FLAGS_default_edge_type);
            eKey.set_dst(vId + 1);
            eKey.set_ranking(0);
            edge.set_key(std::move(eKey));
            auto props = genData(FLAGS_size);
            edge.set_props(std::move(props));
            edges.emplace_back(std::move(edge));
        }
        return edges;
    }

    void getNeighborsTask() {
        auto* evb = threadPool_->getEventBase();
        auto f = client_->getNeighbors(FLAGS_default_space_id, randomVertices(),
                                       FLAGS_default_edge_type, true, "", randomCols())
                            .via(evb).then([this](auto&& resps) {
                                if (!resps.succeeded()) {
                                    LOG(ERROR) << "Request failed!";
                                } else {
                                    VLOG(3) << "request successed!";
                                }
                                this->finishedRequests_++;
                                VLOG(3) << "request successed!";
                             }).onError([](folly::FutureException&) {
                                LOG(ERROR) << "request failed!";
                             });
    }

    void addVerticesTask(uint32_t number) {
        auto* evb = threadPool_->getEventBase();
        auto f = client_->addVertices(FLAGS_default_space_id, genVertices(number), true)
                    .via(evb).then([this](auto&& resps) {
                        if (!resps.succeeded()) {
                            LOG(ERROR) << "Request failed!";
                        } else {
                            VLOG(3) << "request successed!";
                        }
                        this->finishedRequests_++;
                     }).onError([](folly::FutureException&) {
                        LOG(ERROR) << "Request failed!";
                     });
    }

    void addEdgesTask(uint32_t number) {
        auto* evb = threadPool_->getEventBase();
        auto f = client_->addEdges(FLAGS_default_space_id, genEdges(number), true)
                    .via(evb).then([this](auto&& resps) {
                        if (!resps.succeeded()) {
                            LOG(ERROR) << "Request failed!";
                        } else {
                            VLOG(3) << "request successed!";
                        }
                        this->finishedRequests_++;
                        VLOG(3) << "request successed!";
                     }).onError([](folly::FutureException&) {
                        LOG(ERROR) << "Request failed!";
                     });
    }

    void getVerticesTask() {
        auto* evb = threadPool_->getEventBase();
        auto f = client_->getVertexProps(FLAGS_default_space_id, randomVertices(), randomCols())
                    .via(evb).then([this](auto&& resps) {
                        if (!resps.succeeded()) {
                            LOG(ERROR) << "Request failed!";
                        } else {
                            VLOG(3) << "request successed!";
                        }
                        this->finishedRequests_++;
                        VLOG(3) << "request successed!";
                     }).onError([](folly::FutureException&) {
                        LOG(ERROR) << "Request failed!";
                     });
    }

private:
    std::atomic_long finishedRequests_{0};
    std::unique_ptr<StorageClient> client_;
    std::unique_ptr<meta::MetaClient> mClient_;
    std::shared_ptr<folly::IOThreadPoolExecutor> threadPool_;
};

}  // namespace storage
}  // namespace nebula

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    nebula::storage::Perf perf;
    return perf.run();
}

