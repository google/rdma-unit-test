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
#include <memory>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/str_format.h"
#include "public/status_matchers.h"
#include "traffic/client.h"
#include "traffic/op_profiles.h"
#include "traffic/op_types.h"
#include "traffic/operation_generator.h"
#include "traffic/rdma_stress_fixture.h"

namespace rdma_unit_test {
namespace {

class MixedQpModeTest : public RdmaStressFixture,
                        public testing::WithParamInterface</*num_ops*/ int> {
 protected:
  void SetUp() override {
    // Configure for multiple op types
    ConfigureLatencyMeasurements(OpTypes::kInvalid);
  }

  void TearDown() override { CheckLatencies(); }
};

// Test a single RC qp co-existing with a single UD qp.
TEST_P(MixedQpModeTest, BasicTest) {
  constexpr int kMaxInflightOps = 10;
  constexpr int kBatchPerQp = 1;
  constexpr int kOpBytes = 32;

  // Create two clients for the test.
  constexpr Client::Config kClientConfig{
      .max_op_size = kOpBytes,
      .max_outstanding_ops_per_qp = kMaxInflightOps,
      .max_qps = 2};
  Client initiator(/*client_id=*/0, context(), port_attr(), kClientConfig),
      target(/*client_id=*/1, context(), port_attr(), kClientConfig);
  LOG(INFO) << "initiator id: " << initiator.client_id()
            << ", target id: " << target.client_id();

  CreateSetUpRcQps(initiator, target, /*qps_per_client=*/1);
  ConstantRcOperationGenerator rc_op_generator(OpTypes::kWrite, kOpBytes);
  initiator.qp_state(0)->set_op_generator(&rc_op_generator);

  CreateSetUpOneToOneUdQps(initiator, target, /*qps_per_client=*/1);
  ConstantUdOperationGenerator ud_op_generator(kOpBytes);
  initiator.qp_state(1)->set_op_generator(&ud_op_generator);

  int ops_per_qp = GetParam();
  initiator.ExecuteOps(target, /*num_qps=*/2, ops_per_qp, kBatchPerQp,
                       kMaxInflightOps, kMaxInflightOps * 2);

  HaltExecution(initiator);
  HaltExecution(target);
  EXPECT_OK(validation_->PostTestValidation());
}

// Multiplexed UD QPs + RC QPs. UD QPs issue mixed op sizes, and RC QPs issue a
// mixture of op types and sizes.
TEST_P(MixedQpModeTest, StressTest) {
  RandomizedOperationGenerator ud_op_generator =
      RandomizedOperationGenerator(MixedSizeUdOpProfile());
  RandomizedOperationGenerator rc_op_generator =
      RandomizedOperationGenerator(MixedRcOpProfile());
  const int max_op_bytes =
      std::max(ud_op_generator.MaxOpSize(), rc_op_generator.MaxOpSize());

  const int kNumRcQps = 50;
  const int kNumUdQps = 50;
  const int kTotalQps = kNumRcQps + kNumUdQps;
  const int kMaxInflightOps = 32;
  const int kBatchPerQp = 8;
  const int kNumOps = GetParam();

  // Create two clients for the test.
  const Client::Config kInitiatorConfig{
      .max_op_size = max_op_bytes,
      .max_outstanding_ops_per_qp = kMaxInflightOps,
      .max_qps = kTotalQps};
  const Client::Config kTargetConfig{
      .max_op_size = max_op_bytes,
      .max_outstanding_ops_per_qp = kMaxInflightOps * kNumUdQps,
      .max_qps = kTotalQps};
  Client initiator(/*client_id=*/0, context(), port_attr(), kInitiatorConfig),
      target(/*client_id=*/1, context(), port_attr(), kTargetConfig);
  LOG(INFO) << "initiator id: " << initiator.client_id()
            << ", target id: " << target.client_id();

  CreateSetUpMultiplexedUdQps(initiator, target, kNumUdQps, kNumUdQps,
                              AddressHandleMapping::kShared);
  CreateSetUpRcQps(initiator, target, kNumRcQps);

  for (uint32_t qp_id = 0; qp_id < initiator.num_qps(); ++qp_id) {
    if (initiator.qp_state(0)->is_rc()) {
      initiator.qp_state(qp_id)->set_op_generator(&rc_op_generator);
    } else {
      initiator.qp_state(qp_id)->set_op_generator(&ud_op_generator);
    }
  }

  const int ops_per_qp = kNumOps / kTotalQps;
  initiator.ExecuteOps(target, kTotalQps, ops_per_qp, kBatchPerQp,
                       kMaxInflightOps, kMaxInflightOps * kTotalQps);

  HaltExecution(initiator);
  HaltExecution(target);
  EXPECT_OK(validation_->PostTestValidation());
}

INSTANTIATE_TEST_SUITE_P(
    MixedQpModeTest, MixedQpModeTest,
    /*num_ops=*/testing::Values(100, 1000000),
    [](const testing::TestParamInfo<MixedQpModeTest::ParamType>& info) {
      return absl::StrFormat("%dOps", info.param);
    });

}  // namespace
}  // namespace rdma_unit_test
