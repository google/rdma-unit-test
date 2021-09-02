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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_UPDATE_REORDER_QUEUE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_UPDATE_REORDER_QUEUE_H_

#include <cstdint>

#include "absl/container/flat_hash_map.h"
#include "absl/types/optional.h"
#include "random_walk/internal/client_update_service.pb.h"
namespace rdma_unit_test {
namespace random_walk {

// A queue for reordering out of order ClientUpdates. It provides the following
// methods:
// Push(): Push a new ClientUpdate to the queue.
// Pull(): Pull the next ClientUpdate in order. If the expected ClientUpdate
// has not yet been Push()-ed, return absl::nullopt.
// The ordering is done according to the sequence_number() of ClientUpdate.
// It is up to the UpdateDispatcher to guarantee the sequence of ClientUpdate
// sent via a particular channel (dispatcher-handler pair) goes contiguously
// starting from 0.
class UpdateReorderQueue {
 public:
  UpdateReorderQueue() = default;
  // Movable but not copyable.
  UpdateReorderQueue(UpdateReorderQueue&& queue) = default;
  UpdateReorderQueue& operator=(UpdateReorderQueue&& queue) = default;
  UpdateReorderQueue(const UpdateReorderQueue& queue) = delete;
  UpdateReorderQueue& operator=(const UpdateReorderQueue& queue) = default;
  ~UpdateReorderQueue() = default;

  // Pushes a new ClientUpdate to the queue.
  void Push(uint32_t sequence_number, const ClientUpdate& update);

  // Pulles the next expected ClientUpdate from the queue. If the ClientUpdate
  // has not yet been Push()-ed yet, return absl::nullopt.
  // The first expected ClientUpdate has sequence_number() = 0.
  absl::optional<ClientUpdate> Pull();

 private:
  uint32_t next_expected_sequence_number_ = 0;
  absl::flat_hash_map<uint32_t, ClientUpdate> reorder_queue_;
};

}  // namespace random_walk
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_UPDATE_REORDER_QUEUE_H_
