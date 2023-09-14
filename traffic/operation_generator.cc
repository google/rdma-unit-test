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

#include "traffic/operation_generator.h"

#include <time.h>

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <random>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/repeated_field.h"
#include "public/status_matchers.h"
#include "traffic/config.pb.h"
#include "traffic/op_types.h"

ABSL_FLAG(uint32_t, random_seed, time(nullptr),
          "The seed that initializes the random generator for "
          "the sequence of operations.");

namespace rdma_unit_test {

RandomizedOperationGenerator::RandomizedOperationGenerator(
    const Config::OperationProfile& op_profile)
    : random_engine_{absl::GetFlag(FLAGS_random_seed)},
      op_type_lookup_{OpTypes::kWrite, OpTypes::kRead, OpTypes::kSend,
                      OpTypes::kFetchAdd, OpTypes::kCompSwap},
      op_type_distribution_{
          op_profile.rc_op_profile().op_type_proportions().write_proportion(),
          op_profile.rc_op_profile().op_type_proportions().read_proportion(),
          op_profile.rc_op_profile().op_type_proportions().send_proportion(),
          op_profile.rc_op_profile()
              .op_type_proportions()
              .fetch_add_proportion(),
          op_profile.rc_op_profile()
              .op_type_proportions()
              .comp_swap_proportion()},
      op_size_lookup_(op_profile.op_size_proportions().size()),
      op_size_distribution_() {
  if (op_profile.has_ud_op_profile()) {
    // All generated UD operations are sends.
    op_type_distribution_ = std::discrete_distribution<int>({0, 0, 1, 0, 0});
  }

  // Initialize op_size_lookup_ and op_size_distribution_ from configuration
  std::transform(
      op_profile.op_size_proportions().begin(),
      op_profile.op_size_proportions().end(),
      std::back_inserter(op_size_lookup_),
      [](const Config::OperationProfile::OpSizeProportion& op_size_proportion)
          -> int32_t { return op_size_proportion.size_bytes(); });

  std::vector<float> proportions(op_profile.op_size_proportions().size());
  std::transform(
      op_profile.op_size_proportions().begin(),
      op_profile.op_size_proportions().end(), std::back_inserter(proportions),
      [](const Config::OperationProfile::OpSizeProportion& op_size_proportion)
          -> float { return op_size_proportion.proportion(); });
  op_size_distribution_ =
      std::discrete_distribution<int>(proportions.begin(), proportions.end());
}

OperationGenerator::OpAttributes RandomizedOperationGenerator::NextOp() {
  return {op_type_lookup_[op_type_distribution_(random_engine_)],
          op_size_lookup_[op_size_distribution_(random_engine_)]};
}

}  // namespace rdma_unit_test
