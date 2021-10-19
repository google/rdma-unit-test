/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "random_walk/internal/loopback_update_dispatcher.h"

#include <cstdint>
#include <memory>
#include <utility>

#include "glog/logging.h"
#include "absl/container/flat_hash_map.h"
#include "absl/types/optional.h"
#include "public/map_util.h"
#include "random_walk/internal/client_update_service.pb.h"
#include "random_walk/internal/inbound_update_interface.h"

namespace rdma_unit_test {
namespace random_walk {

void LoopbackUpdateDispatcher::RegisterRemote(
    uint32_t client_id, std::weak_ptr<InboundUpdateInterface> client) {
  map_util::InsertOrDie(remotes_, client_id, client);
}

void LoopbackUpdateDispatcher::DispatchUpdate(const ClientUpdate& update) {
  if (update.has_destination_id()) {
    auto maybe_remote = remotes_.at(update.destination_id()).lock();
    CHECK(maybe_remote) << "Remote client destroyed.";  // Crash ok
    maybe_remote->PushInboundUpdate(update);
  } else {
    for (const auto& remote : remotes_) {
      auto maybe_remote = remote.second.lock();
      CHECK(maybe_remote) << "Remote client destroyed.";  // Crash ok
      maybe_remote->PushInboundUpdate(update);
    }
  }
}

}  // namespace random_walk
}  // namespace rdma_unit_test
