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

#include <algorithm>
#include <climits>
#include <memory>
#include <string>
#include <tuple>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "public/status_matchers.h"
#include "public/verbs_util.h"
#include "traffic/op_profiles.h"
#include "traffic/op_types.h"
#include "traffic/rdma_stress_fixture.h"

namespace rdma_unit_test {
namespace {

using verbs_util::VerbsMtuToInt;

class BasicUdTest : public RdmaStressFixture {
 public:
  static constexpr int kMaxInflightOps = 16;
  static constexpr int kBatchPerQp = 4;
  void SetUp() override { ConfigureLatencyMeasurements(OpTypes::kSend); }

 protected:
  void FinalizeStateAndCheckLatencies(Client &initiator, Client &target) {
    HaltExecution(initiator);
    HaltExecution(target);
    CollectClientLatencyStats(initiator);
    CheckLatencies();
    EXPECT_OK(validation_->PostTestValidation());
  }
};

class UdConstantOpTest
    : public BasicUdTest,
      public testing::WithParamInterface<std::tuple<
          /*num_qps*/ int, /*ops_size*/ int, /*num_ops*/ int>> {};

// This test verifies the correctness of Send/Recv ops on UD qps connected to
// each other in 1:1 configuration with varying number of qps and op sizes.
TEST_P(UdConstantOpTest, OneToOne) {
  const int kNumQps = std::get<0>(GetParam());
  const int kOpSize = std::min(std::get<1>(GetParam()),
                               VerbsMtuToInt(port_attr().attr.active_mtu));
  const int kNumOps = std::get<2>(GetParam());
  const Client::Config client_config = {
      .max_op_size = kOpSize,
      .max_outstanding_ops_per_qp = kMaxInflightOps,
      .max_qps = kNumQps};

  Client initiator(/*client_id=*/0, context(), port_attr(), client_config),
      target(/*client_id=*/1, context(), port_attr(), client_config);

  CreateSetUpOneToOneUdQps(initiator, target, kNumQps);

  auto op_generator = ConstantUdOperationGenerator(kOpSize);
  for (int qp_id = 0; qp_id < kNumQps; ++qp_id) {
    initiator.qp_state(qp_id)->set_op_generator(&op_generator);
  }

  int ops_per_qp = std::max(1, kNumOps / kNumQps);
  initiator.ExecuteOps(target, kNumQps, ops_per_qp, kBatchPerQp,
                       kMaxInflightOps, kMaxInflightOps * kNumQps);
  FinalizeStateAndCheckLatencies(initiator, target);
}

INSTANTIATE_TEST_SUITE_P(
    BasicUdTest, UdConstantOpTest,
    testing::Combine(
        /*num_qps=*/testing::Values(1, 100, 1000),
        /*op_size=*/testing::Values(4, 512, INT_MAX),  // Largest size is capped
                                                       // to active MTU by the
                                                       // test.
        /*num_ops=*/testing::Values(10, 1000000)),
    [](const testing::TestParamInfo<UdConstantOpTest::ParamType> &info) {
      const int num_qps = std::get<0>(info.param);
      const int op_size = std::get<1>(info.param);
      const int num_ops = std::get<2>(info.param);
      return absl::StrFormat("%dQps%dByteOp%dOps", num_qps, op_size, num_ops);
    });

class UdMixedOpTest : public BasicUdTest,
                      public testing::WithParamInterface<
                          std::tuple</*num_qps*/ int, /*num_ops*/ int>> {};

// This test verifies the correctness of Send/Recv ops on UD qps connected to
// each other in 1:1 configuration with varying number of qps and mixed op size.
TEST_P(UdMixedOpTest, OneToOne) {
  const int kNumQps = std::get<0>(GetParam());
  const int kNumOps = std::get<1>(GetParam());
  const Client::Config client_config = {
      .max_op_size = VerbsMtuToInt(port_attr().attr.active_mtu),
      .max_outstanding_ops_per_qp = kMaxInflightOps,
      .max_qps = kNumQps};

  Client initiator(/*client_id=*/0, context(), port_attr(), client_config),
      target(/*client_id=*/1, context(), port_attr(), client_config);

  CreateSetUpOneToOneUdQps(initiator, target, kNumQps);

  RandomizedOperationGenerator op_generator(MixedSizeUdOpProfile());
  for (int qp_id = 0; qp_id < kNumQps; ++qp_id) {
    initiator.qp_state(qp_id)->set_op_generator(&op_generator);
  }

  int ops_per_qp = std::max(1, kNumOps / kNumQps);
  initiator.ExecuteOps(target, kNumQps, ops_per_qp, kBatchPerQp,
                       kMaxInflightOps, kMaxInflightOps * kNumQps);
  FinalizeStateAndCheckLatencies(initiator, target);
}

INSTANTIATE_TEST_SUITE_P(
    BasicUdTest, UdMixedOpTest,
    testing::Combine(/*num_qps=*/testing::Values(1, 100, 1000),
                     /*num_ops=*/testing::Values(10, 1000000)),
    [](const testing::TestParamInfo<UdMixedOpTest::ParamType> &info) {
      const int num_qps = std::get<0>(info.param);
      const int num_ops = std::get<1>(info.param);
      return absl::StrFormat("%dQps%dOps", num_qps, num_ops);
    });

// Parameters for multiplexed UD test cases.
// Test_type can be one of the following:
// OneToMany  :  1 initiator,  N targets.
// ManyToOne  :  N initiators, 1 target.
// ManyToMany :  M initiators, N targets. Shared AddressHandle among initiators.
// ManyToManyIndependent : Same as above, but independent AddressHandles.
struct MultiplexedTestParams {
  std::string test_type;
  uint16_t initiator_qps;
  uint16_t target_qps;
  bool shared_ah_mapping =
      true;  // True means each initiator shares the AddressHandle of the
             // target. False means each initiator has a separate independent
             // AddressHandle.
};

class MultiplexedUdTest
    : public BasicUdTest,
      public testing::WithParamInterface<
          std::tuple<MultiplexedTestParams, /*num_ops*/ int>> {};

// This test verifies the correctness of Send/Recv ops on UD qps connected to
// each other in various M:N configuration with shared/independent AHs.
TEST_P(MultiplexedUdTest, FixedOpSize) {
  const MultiplexedTestParams &test_case = std::get<0>(GetParam());
  const int kNumOps = std::get<1>(GetParam());
  const int kOpSize = 32;

  ConstantUdOperationGenerator op_generator(kOpSize);

  Client initiator(
      /*client_id=*/0, context(), port_attr(),
      Client::Config{.max_op_size = static_cast<int>(op_generator.MaxOpSize()),
                     .max_outstanding_ops_per_qp = kMaxInflightOps,
                     .max_qps = test_case.initiator_qps}),
      target(/*client_id=*/1, context(), port_attr(),
             Client::Config{
                 .max_op_size = static_cast<int>(op_generator.MaxOpSize()),
                 .max_outstanding_ops_per_qp =
                     kMaxInflightOps * test_case.initiator_qps,
                 .max_qps = test_case.target_qps});

  CreateSetUpMultiplexedUdQps(
      initiator, target, test_case.initiator_qps, test_case.target_qps,
      test_case.shared_ah_mapping ? AddressHandleMapping::kShared
                                  : AddressHandleMapping::kIndependent);

  for (uint32_t qp_id = 0; qp_id < test_case.initiator_qps; ++qp_id) {
    initiator.qp_state(qp_id)->set_op_generator(&op_generator);
  }

  int ops_per_qp = kNumOps / test_case.initiator_qps;
  initiator.ExecuteOps(target, test_case.initiator_qps, ops_per_qp, kBatchPerQp,
                       kMaxInflightOps,
                       kMaxInflightOps * test_case.initiator_qps);
  FinalizeStateAndCheckLatencies(initiator, target);
}

INSTANTIATE_TEST_SUITE_P(
    BasicUdTest, MultiplexedUdTest,
    testing::Combine(testing::ValuesIn<MultiplexedTestParams>(
                         {{"OneToMany", /*initiator_qps=*/1, /*target_qps=*/10},
                          {"ManyToOne", /*initiator_qps=*/10, /*target_qps=*/1},
                          {"ManyToMany", /*initiator_qps=*/10,
                           /*target_qps=*/10,
                           /*shared_ah_mapping=*/true},
                          {"ManyToManyIndependentAh", /*initiator_qps=*/10,
                           /*target_qps=*/10,
                           /*shared_ah_mapping=*/false}}),
                     /*num_ops*/ testing::Values(10, 1000000)),
    [](const testing::TestParamInfo<MultiplexedUdTest::ParamType> &info) {
      const MultiplexedTestParams params = std::get<0>(info.param);
      const int num_ops = std::get<1>(info.param);
      return absl::StrFormat("%s%dOps", params.test_type, num_ops);
    });

class UdMultiplexedMixedOpsTest
    : public BasicUdTest,
      public testing::WithParamInterface</*num_ops*/ int> {};

// This test verifies the correctness of Send/Recv ops with mixed sizes on UD
// qps connected to each other in a multiplexed configuration with shared AHs.
TEST_P(UdMultiplexedMixedOpsTest, MultiplexedMixedOpsShared) {
  const int kNumQps = 10;
  const int kNumOps = GetParam();
  const Client::Config kInitiatorConfig = {
      .max_op_size = VerbsMtuToInt(port_attr().attr.active_mtu),
      .max_outstanding_ops_per_qp = kMaxInflightOps,
      .max_qps = kNumQps};
  const Client::Config kTargetConfig = {
      .max_op_size = VerbsMtuToInt(port_attr().attr.active_mtu),
      .max_outstanding_ops_per_qp = kMaxInflightOps * kNumQps,
      .max_qps = kNumQps};

  Client initiator(/*client_id=*/0, context(), port_attr(), kInitiatorConfig),
      target(/*client_id=*/1, context(), port_attr(), kTargetConfig);

  CreateSetUpMultiplexedUdQps(initiator, target, kNumQps, kNumQps,
                              AddressHandleMapping::kShared);

  RandomizedOperationGenerator op_generator(MixedSizeUdOpProfile());
  for (int qp_id = 0; qp_id < kNumQps; ++qp_id) {
    initiator.qp_state(qp_id)->set_op_generator(&op_generator);
  }

  int ops_per_qp = kNumOps / kNumQps;
  initiator.ExecuteOps(target, kNumQps, ops_per_qp, kBatchPerQp,
                       kMaxInflightOps, kMaxInflightOps * kNumQps);
  FinalizeStateAndCheckLatencies(initiator, target);
}

INSTANTIATE_TEST_SUITE_P(
    BasicUdTest, UdMultiplexedMixedOpsTest,
    /*num_ops=*/testing::Values(10, 1000000),
    [](const testing::TestParamInfo<UdMultiplexedMixedOpsTest::ParamType>
           &info) { return absl::StrFormat("%dOps", info.param); });

}  // namespace
}  // namespace rdma_unit_test
