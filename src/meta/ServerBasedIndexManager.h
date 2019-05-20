/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_SERVERBASEDINDEXMANAGER_H
#define META_SERVERBASEDINDEXMANAGER_H

#include "base/Base.h"
#include "meta/IndexManager.h"
#include "meta/client/MetaClient.h"

namespace nebula {
namespace meta {

class ServerBasedIndexManager : public IndexManager {
public:
    void init(MetaClient *client) override;

private:
    MetaClient             *client_{nullptr};
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SERVERBASEDINDEXMANAGER_H
