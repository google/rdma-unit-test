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

#include <memory>
#include <tuple>
#include <utility>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_format.h"
#include "public/status_matchers.h"
#include "traffic/client.h"
#include "traffic/op_types.h"
#include "traffic/operation_generator.h"
#include "traffic/rdma_stress_fixture.h"
#include "traffic/test_op.h"

namespace rdma_unit_test {
namespace {

class QpOversubscriptionTest
    : public RdmaStressFixture,
      public testing::WithParamInterface<
          std::tuple</*max_qps*/ int, /*op_size*/ int, OpTypes>> {
 public:
  static constexpr int kQpsEachStep = 500;
};

TEST_P(QpOversubscriptionTest, StressTest) {
  const int kMaxQps = std::get<0>(GetParam());
  const int kOpSize = std::get<1>(GetParam());
  const OpTypes KOpType = std::get<2>(GetParam());
  const int kOpsPerQp = std::min(
      RdmaStressFixture::LimitNumOps(kOpSize, 1000000) / kMaxQps, 1000);

  const int kMaxInflightOps = 32;
  const int kBatchPerQp = 8;

  ConfigureLatencyMeasurements(KOpType);
  const Client::Config kClientConfig = {
      .max_op_size = kOpSize,
      .max_outstanding_ops_per_qp = kMaxInflightOps,
      .max_qps = kMaxQps};
  Client initiator(/*client_id=*/0, context(), port_attr(), kClientConfig),
      target(/*client_id=*/1, context(), port_attr(), kClientConfig);
  LOG(INFO) << "initiator id: " << initiator.client_id()
            << ", target id: " << target.client_id();

  // Issue ops on a set of qps in incremental steps; increasing the size of
  // the set by a `qps_each_step` at each step. The goal is to ensure that ops
  // issued on new qps can make progress as the number of qps increases in
  // successive steps. Qps created in each set are assigned a unique stat
  // index to track latencies, and the size of each step is determined by this
  // flag.
  int qps_each_step = kQpsEachStep;
  if (qps_each_step > kMaxQps) {  // Limit qps_each_step to max number of qps.
    qps_each_step = kMaxQps;
  }
  int total_qps = 0;
  int stat_set_counter = 0;
  auto op_generator = ConstantRcOperationGenerator(KOpType, kOpSize);
  do {
    // Create a set of qp_each_step qps and assign them to the same stat_set.
    for (int i = 0; i < qps_each_step; ++i) {
      absl::StatusOr<uint32_t> initiator_qp_id =
          initiator.CreateQp(/*is_rc=*/true);
      absl::StatusOr<uint32_t> target_qp_id = target.CreateQp(/*is_rc=*/true);
      ASSERT_OK(initiator_qp_id);  // Crash OK
      ASSERT_OK(target_qp_id);     // Crash OK

      // Set up the connection in both directions.
      EXPECT_OK(SetUpRcClientsQPs(&initiator, initiator_qp_id.value(), &target,
                                  target_qp_id.value()));
      EXPECT_OK(SetUpRcClientsQPs(&target, target_qp_id.value(), &initiator,
                                  initiator_qp_id.value()));

      initiator.qp_state(initiator_qp_id.value())
          ->set_op_generator(&op_generator);
    }
    total_qps += qps_each_step;
    stat_set_counter += 2;

    int ops_completed =
        initiator.ExecuteOps(target, total_qps, kOpsPerQp, kBatchPerQp,
                             kMaxInflightOps, kMaxInflightOps * total_qps);
    EXPECT_EQ(ops_completed, total_qps * kOpsPerQp);

    HaltExecution(initiator);
    HaltExecution(target);
    CollectClientLatencyStats(initiator);
  } while (total_qps < kMaxQps);

  CheckLatencies();
  EXPECT_THAT(validation_->PostTestValidation(
                  RdmaStressFixture::AllowRetxCheck(kMaxQps)),
              IsOk());
}

INSTANTIATE_TEST_SUITE_P(
    Basic, QpOversubscriptionTest,
    testing::Combine(
        /*max_qps=*/testing::Values(10, 1000),
        /*op_size=*/testing::Values(32, 1024, 16 * 1024),
        testing::Values(OpTypes::kWrite, OpTypes::kRead, OpTypes::kSend)),
    [](const testing::TestParamInfo<QpOversubscriptionTest::ParamType>& info) {
      const int max_qps = std::get<0>(info.param);
      const int op_size = std::get<1>(info.param);
      const OpTypes op_type = std::get<2>(info.param);
      return absl::StrFormat("%dQps%dByteOp%s", max_qps, op_size,
                             TestOp::ToString(op_type));
    });

}  // namespace
}  // namespace rdma_unit_test
