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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_OPERATION_GENERATOR_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_OPERATION_GENERATOR_H_

#include <algorithm>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "google/protobuf/repeated_field.h"
#include "absl/flags/declare.h"
#include "absl/memory/memory.h"
#include "traffic/config.pb.h"
#include "traffic/op_types.h"

ABSL_DECLARE_FLAG(uint32_t, random_seed);

namespace rdma_unit_test {

class OperationGenerator {
 public:
  struct OpAttributes {
    OpTypes op_type;
    uint32_t op_size_bytes;
  };

  OperationGenerator() {}
  virtual ~OperationGenerator() {}

  // Returns the properties for the next operation that is issued.
  virtual OpAttributes NextOp() = 0;

  // Returns the maximum operation size that this generator will generate.
  virtual uint32_t MaxOpSize() = 0;
};

class RandomizedOperationGenerator : public OperationGenerator {
 public:
  // Constructs an OperationGenerator that will generate operation properties at
  // the ratios provided in op_profile. The random generator is initialized with
  // the random_seed command line argument.
  explicit RandomizedOperationGenerator(
      const Config::OperationProfile& op_profile);
  ~RandomizedOperationGenerator() override {}

  // Generates the properties of an operation according to the proportions
  // provided in at initialization.
  OpAttributes NextOp() override;

  // Returns the sum of the number of qps from all op_profiles.
  static int32_t TotalQps(
      const google::protobuf::RepeatedPtrField<Config::OperationProfile>&
          op_profiles) {
    int32_t max_qps_per_client = 0;
    for (auto& profile : op_profiles) {
      if (profile.has_rc_op_profile()) {
        max_qps_per_client += profile.rc_op_profile().num_qps();
      } else if (profile.has_ud_op_profile()) {
        if (profile.ud_op_profile().has_num_one_to_one_qps()) {
          max_qps_per_client += profile.ud_op_profile().num_one_to_one_qps();
        } else if (profile.ud_op_profile().has_multiplexed_qps()) {
          max_qps_per_client +=
              std::max(profile.ud_op_profile().multiplexed_qps().num_src_qps(),
                       profile.ud_op_profile().multiplexed_qps().num_dst_qps());
        }
      }
    }
    return max_qps_per_client;
  }

  // Returns the maximum op size from any of the op_profiles.
  static int32_t MaxOpSize(
      const google::protobuf::RepeatedPtrField<Config::OperationProfile>&
          op_profiles) {
    int32_t max = 0;
    for (auto& profile : op_profiles) {
      for (auto& elem : profile.op_size_proportions()) {
        max = std::max(elem.size_bytes(), max);
      }
    }
    return max;
  }

  uint32_t MaxOpSize() override {
    return *std::max_element(op_size_lookup_.begin(), op_size_lookup_.end());
  }

 protected:
  std::default_random_engine random_engine_;

  const std::vector<OpTypes> op_type_lookup_;
  std::discrete_distribution<int> op_type_distribution_;

  std::vector<uint32_t> op_size_lookup_;
  std::discrete_distribution<int> op_size_distribution_;
};

class ConstantRcOperationGenerator : public OperationGenerator {
 public:
  ConstantRcOperationGenerator(OpTypes op_type, uint32_t op_size_bytes)
      : op_type_(op_type), op_size_bytes_(op_size_bytes) {}
  ~ConstantRcOperationGenerator() override {}

  OpAttributes NextOp() override { return {op_type_, op_size_bytes_}; }

  uint32_t MaxOpSize() override { return op_size_bytes_; }

 private:
  OpTypes op_type_;
  uint32_t op_size_bytes_;
};

class ConstantUdOperationGenerator : public ConstantRcOperationGenerator {
 public:
  explicit ConstantUdOperationGenerator(uint32_t op_size_bytes)
      : ConstantRcOperationGenerator(OpTypes::kSend, op_size_bytes) {}
  ~ConstantUdOperationGenerator() override {}
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_OPERATION_GENERATOR_H_
