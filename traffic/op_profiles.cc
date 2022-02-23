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

#include "traffic/op_profiles.h"

#include "absl/flags/flag.h"
#include "traffic/config.pb.h"

// TODO(b/187874559): Remove this flag once atomics are working.
ABSL_FLAG(bool, generate_atomics, false,
          "When true, MixedRcOpGenerator will include atomic operations. When "
          "false, it will include all op types except for atomics.");

namespace rdma_unit_test {

// The mixture of operation sizes to use for operation size mixture tests.
constexpr int kMixedOpSizesBytes[5] = {32, 128, 1024, 2 * 1024, 4 * 1024};

// Returns an OperationGenerator with UD queue pairs and a even mixture of
// operations sizes that we most commonly test.
Config::OperationProfile MixedSizeUdOpProfile() {
  Config::OperationProfile op_profile;
  float ratio = 1. / 5.;
  for (const int &op_size : kMixedOpSizesBytes) {
    Config::OperationProfile::OpSizeProportion *op_size_proportion =
        op_profile.add_op_size_proportions();
    op_size_proportion->set_size_bytes(op_size);
    op_size_proportion->set_proportion(ratio);
  }
  // Sets the type to UD.
  op_profile.mutable_ud_op_profile();
  return op_profile;
}

// Returns an OperationProfile with RC queue pairs and a even mixture of all
// operation types and operation sizes that we most commonly test.
Config::OperationProfile MixedRcOpProfile() {
  Config::OperationProfile op_profile;
  float size_ratio = 1. / sizeof(kMixedOpSizesBytes);
  for (const int &op_size : kMixedOpSizesBytes) {
    Config::OperationProfile::OpSizeProportion *op_size_proportion =
        op_profile.add_op_size_proportions();
    op_size_proportion->set_size_bytes(op_size);
    op_size_proportion->set_proportion(size_ratio);
  }
  float type_ratio = 1. / 5.;  // Each op type should be equally likely.
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_read_proportion(type_ratio);
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_write_proportion(type_ratio);
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_send_proportion(type_ratio);
  if (absl::GetFlag(FLAGS_generate_atomics)) {
    op_profile.mutable_rc_op_profile()
        ->mutable_op_type_proportions()
        ->set_fetch_add_proportion(type_ratio);
    op_profile.mutable_rc_op_profile()
        ->mutable_op_type_proportions()
        ->set_comp_swap_proportion(type_ratio);
  }
  return op_profile;
}

}  // namespace rdma_unit_test
