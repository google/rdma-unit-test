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

#include "random_walk/internal/completion_profile.h"

#include <ostream>
#include <sstream>
#include <string>

#include "absl/container/flat_hash_map.h"
#include <magic_enum.hpp>
#include "infiniband/verbs.h"
#include "random_walk/internal/types.h"

namespace rdma_unit_test {
namespace random_walk {

void CompletionProfile::RegisterCompletion(const ibv_wc& completion) {
  ++profiles_[DecodeAction(completion.wr_id)][completion.status];
}

std::string CompletionProfile::DumpStats() const {
  std::stringstream sstream;
  sstream << "Dumping completion profile:" << '\n';
  sstream << "---------------------------------------------------" << '\n';
  for (const auto& [action, profile] : profiles_) {
    sstream << "Action : " << magic_enum::enum_name(action) << '\n';
    for (const auto& [status, count] : profile) {
      if (count > 0) {
        sstream << "  " << ibv_wc_status_str(status) << " : " << count << '\n';
      }
    }
    sstream << "---------------------------------------------------" << '\n';
    sstream << '\n';
  }

  return sstream.str();
}

}  // namespace random_walk
}  // namespace rdma_unit_test
