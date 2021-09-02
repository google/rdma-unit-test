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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_BIND_OPS_TRACKER_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_BIND_OPS_TRACKER_H_

#include <cstdint>

#include "absl/container/flat_hash_map.h"
#include "absl/types/optional.h"
#include "infiniband/verbs.h"

namespace rdma_unit_test {
namespace random_walk {

// The class tracks bind ops (type 1 and type 2 MW bind) issued
// by the RandomWalkClient. This allows the RandomWalkClient to retrieve
// information about the corresponding WR when its completion get polled.
class BindOpsTracker {
 public:
  // Minimum struct that encapsulate information for a bind ops, type 1 or
  // type 2.
  struct BindWr {
    ibv_mw* mw;
    ibv_mw_bind_info bind_info;
    absl::optional<uint32_t> rkey = absl::nullopt;  // For type 2 MWs.
  };

  BindOpsTracker() = default;
  // Movable but not copyable..
  BindOpsTracker(BindOpsTracker&& tracker) = default;
  BindOpsTracker& operator=(BindOpsTracker&& tracker) = default;
  BindOpsTracker(const BindOpsTracker& tracker) = delete;
  BindOpsTracker& operator=(const BindOpsTracker& tracker) = delete;
  ~BindOpsTracker() = default;

  // Pushes a type 1 MW bind op into the tracker.
  void PushType1MwBind(ibv_mw* mw, ibv_mw_bind bind_wr);

  // Pushes a type 2 MW bind op into the tracker.
  void PushType2MwBind(ibv_send_wr bind_wr);

  // Retrieves and erases the bind op information according its wr_id.
  BindWr ExtractBindWr(uint64_t wr_id);

 private:
  absl::flat_hash_map<uint64_t, BindWr> bind_wrs_;
};

}  // namespace random_walk
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_BIND_OPS_TRACKER_H_
