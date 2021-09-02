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

#include "random_walk/internal/update_reorder_queue.h"

#include "absl/types/optional.h"
#include "public/map_util.h"
#include "random_walk/internal/client_update_service.pb.h"

namespace rdma_unit_test {
namespace random_walk {

void UpdateReorderQueue::Push(uint32_t sequence_number,
                              const ClientUpdate& update) {
  map_util::InsertOrDie(reorder_queue_, sequence_number, update);
}

absl::optional<ClientUpdate> UpdateReorderQueue::Pull() {
  auto iter = reorder_queue_.find(next_expected_sequence_number_);
  if (iter == reorder_queue_.end()) {
    return absl::nullopt;
  }
  ++next_expected_sequence_number_;
  ClientUpdate update = iter->second;
  reorder_queue_.erase(iter);
  return update;
}

}  // namespace random_walk
}  // namespace rdma_unit_test
