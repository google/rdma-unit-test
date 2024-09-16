// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_COMPLETION_PROFILE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_COMPLETION_PROFILE_H_

#include <cstddef>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "infiniband/verbs.h"
#include "random_walk/internal/types.h"

namespace rdma_unit_test {
namespace random_walk {

// A class for profiling RDMA op completions. It offers methods for storing
// and summarizing RDMA ops and its completion status.
class CompletionProfile {
 public:
  CompletionProfile() = default;
  CompletionProfile(const CompletionProfile& other) = delete;
  CompletionProfile& operator=(const CompletionProfile& other) = delete;
  CompletionProfile(CompletionProfile&& other) = default;
  CompletionProfile& operator=(CompletionProfile&& other) = default;
  ~CompletionProfile() = default;

  // Registers a completion entry.
  void RegisterCompletion(const ibv_wc& completion);

  // Return a string representing the completion profile of all completions
  // registered so far.
  std::string DumpStats() const;

 private:
  // An action profile represents the count of each completion status.
  using ActionProfile = absl::flat_hash_map<ibv_wc_status, size_t>;

  // Stores the action profile (see type ActionProfile) for each action.
  absl::flat_hash_map<Action, ActionProfile> profiles_;
};

}  // namespace random_walk
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_COMPLETION_PROFILE_H_
