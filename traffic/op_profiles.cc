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

namespace rdma_unit_test {

// The mixture of operation sizes to use for operation size mixture tests. Note
// that UD ops must be less than max MTU size of 4k.
constexpr int kUdMixedOpSizesBytes[4] = {32, 256, 1024, 4 * 1024};
constexpr int kRcMixedOpSizesBytes[5] = {32, 256, 1024, 4 * 1024, 16 * 1024};

Config::OperationProfile MixedSizeUdOpProfile() {
  Config::OperationProfile op_profile;
  float ratio = 1. / sizeof(kRcMixedOpSizesBytes);
  for (const int &op_size : kUdMixedOpSizesBytes) {
    Config::OperationProfile::OpSizeProportion *op_size_proportion =
        op_profile.add_op_size_proportions();
    op_size_proportion->set_size_bytes(op_size);
    op_size_proportion->set_proportion(ratio);
  }
  // Sets the type to UD.
  op_profile.mutable_ud_op_profile();
  return op_profile;
}

Config::OperationProfile MixedRcOpProfile() {
  Config::OperationProfile op_profile;
  float size_ratio = 1. / sizeof(kRcMixedOpSizesBytes);
  for (const int &op_size : kRcMixedOpSizesBytes) {
    Config::OperationProfile::OpSizeProportion *op_size_proportion =
        op_profile.add_op_size_proportions();
    op_size_proportion->set_size_bytes(op_size);
    op_size_proportion->set_proportion(size_ratio);
  }
  float type_ratio = 0.33;  // Each op type should be equally likely.
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_read_proportion(type_ratio);
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_write_proportion(type_ratio);
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_send_proportion(type_ratio);
  return op_profile;
}

Config::OperationProfile MixedRcOpProfileWithAtomics() {
  Config::OperationProfile op_profile;
  float size_ratio = 1. / sizeof(kRcMixedOpSizesBytes);
  for (const int &op_size : kRcMixedOpSizesBytes) {
    Config::OperationProfile::OpSizeProportion *op_size_proportion =
        op_profile.add_op_size_proportions();
    op_size_proportion->set_size_bytes(op_size);
    op_size_proportion->set_proportion(size_ratio);
  }
  float type_ratio = 0.2;  // Each op type should be equally likely.
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_read_proportion(type_ratio);
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_write_proportion(type_ratio);
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_send_proportion(type_ratio);
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_fetch_add_proportion(type_ratio);
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_comp_swap_proportion(type_ratio);
  return op_profile;
}

}  // namespace rdma_unit_test
