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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_INVALIDATE_OPS_TRACKER_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_INVALIDATE_OPS_TRACKER_H_

#include <cstdint>

#include "absl/container/flat_hash_map.h"
#include "random_walk/internal/types.h"

namespace rdma_unit_test {
namespace random_walk {

// The class tracks "Send with Invalidate" ops issued by a RandomWalkClient.
// This allows the RandomWalkClient to retrieve information about the
// corresponding WR when its completion get polled.
class InvalidateOpsTracker {
 public:
  // Minimum struct that encapsulate metadata for an invalidate op.
  struct InvalidateWr {
    ClientId client_id;
    uint32_t rkey;
  };

  InvalidateOpsTracker() = default;
  // Moveable but not copyable..
  InvalidateOpsTracker(InvalidateOpsTracker&& tracker) = default;
  InvalidateOpsTracker& operator=(InvalidateOpsTracker&& tracker) = default;
  InvalidateOpsTracker(const InvalidateOpsTracker& tracker) = delete;
  InvalidateOpsTracker& operator=(const InvalidateOpsTracker& tracker) = delete;
  ~InvalidateOpsTracker() = default;

  // Pushes a invalidate MW into the tracker.
  void PushInvalidate(uint64_t wr_id, uint32_t rkey, ClientId client_id);

  // Retrieves and erases the invalidate op information according to its wr_id.
  InvalidateWr ExtractInvalidateWr(uint64_t wr_id);

 private:
  absl::flat_hash_map<uint64_t, InvalidateWr> invalidate_wrs_;
};

}  // namespace random_walk
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_INVALIDATE_OPS_TRACKER_H_
