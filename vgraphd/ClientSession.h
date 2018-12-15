/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef VGRAPHD_CLIENTSESSION_H_
#define VGRAPHD_CLIENTSESSION_H_

#include "base/Base.h"
#include "time/Duration.h"

/**
 * A ClientSession holds the context informations of a session opened by a client.
 */

namespace vesoft {
namespace vgraph {

class ClientSession final {
public:
    int64_t id() const {
        return id_;
    }

    void setId(int64_t id) {
        id_ = id;
    }

    const std::string& space() const {
        return space_;
    }

    void setSpace(std::string space) {
        space_ = std::move(space);
    }

    uint64_t idleSeconds() const;

    const std::string& user() const {
        return user_;
    }

    void setUser(std::string user) {
        user_ = std::move(user);
    }

    void charge();

private:
    // ClientSession could only be created via SessionManager
    friend class SessionManager;
    ClientSession() = default;
    explicit ClientSession(int64_t id);

    static std::shared_ptr<ClientSession> create(int64_t id);


private:
    int64_t             id_{0};
    time::Duration      idleDuration_;
    std::string         space_;
    std::string         user_;
};

}   // namespace vgraph
}   // namespace vesoft

#endif  // VGRAPHD_CLIENTSESSION_H_
