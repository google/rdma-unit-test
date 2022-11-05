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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "public/status_matchers.h"
#include "traffic/client.h"
#include "traffic/op_types.h"
#include "traffic/operation_generator.h"
#include "traffic/qp_state.h"
#include "traffic/rdma_stress_fixture.h"

namespace rdma_unit_test {
namespace {

struct EventDrivenCompletionsTestCase {
  std::string test_name;
  Client::CompletionMethod completion_method;
  int num_qps;
  int num_ops;
  int op_size_bytes;
};

class EventDrivenCompletionsTest
    : public RdmaStressFixture,
      public testing::WithParamInterface<EventDrivenCompletionsTestCase> {
 public:
  const int kMaxInflightOps = 32;
  const int kBatchPerQp = 8;

 protected:
  void SetUp() override { ConfigureLatencyMeasurements(OpTypes::kSend); }

  void FinalizeStateAndCheckLatencies(Client& initiator, Client& target) {
    HaltExecution(initiator);
    HaltExecution(target);
    CollectClientLatencyStats(initiator);
    CheckLatencies();
    EXPECT_OK(validation_->PostTestValidation());
  }
};

// This test creates a single qp, and issues a single op on that qp. It then
// verifies the completion is received using an event driven method. Although
// the same test can be run changing the parameters of `ManyOpsAreSuccessful`,
// this test is useful for debugging because the code paths are simpler.
TEST_P(EventDrivenCompletionsTest, OneOpIsSuccessful) {
  const EventDrivenCompletionsTestCase& kTestCase = GetParam();

  Client::Config config = {.max_op_size = kTestCase.op_size_bytes,
                           .max_outstanding_ops_per_qp = kMaxInflightOps,
                           .max_qps = kTestCase.num_qps};
  Client initiator(/*client_id=*/0, context(), port_attr(), config),
      target(/*client_id=*/1, context(), port_attr(), config);

  CreateSetUpOneToOneUdQps(initiator, target, /*qps_per_client=*/1);
  initiator.PrepareSendCompletionChannel(kTestCase.completion_method);
  target.PrepareRecvCompletionChannel(kTestCase.completion_method);

  // The single qp that is created will be at index 0.
  constexpr uint16_t kQpId = 0;
  QpState* qp_state = initiator.qp_state(kQpId);
  QpState::UdDestination remote_qp = qp_state->random_ud_destination();
  Client::OpAttributes recv_attributes = {
      .op_type = OpTypes::kRecv,
      .op_bytes = kTestCase.op_size_bytes,
      .num_ops = 1,
      .initiator_qp_id = remote_qp.qp_state->qp_id()};
  ASSERT_OK(target.PostOps(recv_attributes));
  Client::OpAttributes send_attributes = {.op_type = OpTypes::kSend,
                                          .op_bytes = kTestCase.op_size_bytes,
                                          .num_ops = 1,
                                          .initiator_qp_id = kQpId};
  send_attributes.ud_send_attributes = {
      .remote_qp = remote_qp.qp_state,
      .remote_op_id = remote_qp.qp_state->GetLastOpId(),
      .remote_ah = remote_qp.ah};
  ASSERT_OK(initiator.PostOps(send_attributes));

  const absl::Time kTimeout =
      absl::Now() + absl::GetFlag(FLAGS_completion_timeout_s);
  int initiator_completed = 0;
  int target_completed = 0;
  while (absl::Now() < kTimeout &&
         (initiator_completed == 0 || target_completed == 0)) {
    initiator_completed += initiator.TryPollSendCompletionsEventDriven();
    target_completed += target.TryPollRecvCompletionsEventDriven();
    absl::SleepFor(absl::Milliseconds(10));
  }
  EXPECT_EQ(initiator_completed, 1);
  EXPECT_EQ(target_completed, 1);

  FinalizeStateAndCheckLatencies(initiator, target);
}

// This test creates multiple qps, and issues multiple ops on them. It verifies
// the completions are received using an event driven method.
TEST_P(EventDrivenCompletionsTest, ManyOpsAreSuccessful) {
  const EventDrivenCompletionsTestCase& kTestCase = GetParam();

  Client::Config config = {.max_op_size = kTestCase.op_size_bytes,
                           .max_outstanding_ops_per_qp = kMaxInflightOps,
                           .max_qps = kTestCase.num_qps};
  Client initiator(/*client_id=*/0, context(), port_attr(), config),
      target(/*client_id=*/1, context(), port_attr(), config);

  CreateSetUpOneToOneUdQps(initiator, target, kTestCase.num_qps);
  initiator.PrepareSendCompletionChannel(kTestCase.completion_method);
  target.PrepareRecvCompletionChannel(kTestCase.completion_method);

  ConstantUdOperationGenerator op_generator =
      ConstantUdOperationGenerator(kTestCase.op_size_bytes);
  for (uint32_t qp_id = 0; qp_id < initiator.num_qps(); ++qp_id) {
    initiator.qp_state(qp_id)->set_op_generator(&op_generator);
  }

  initiator.ExecuteOps(target, kTestCase.num_qps, kTestCase.num_ops,
                       kBatchPerQp, kMaxInflightOps,
                       kMaxInflightOps * kTestCase.num_qps,
                       kTestCase.completion_method);

  FinalizeStateAndCheckLatencies(initiator, target);
}

INSTANTIATE_TEST_SUITE_P(
    VaryCompletionMethod, EventDrivenCompletionsTest,
    testing::ValuesIn<EventDrivenCompletionsTestCase>({
        {"Blocking", Client::CompletionMethod::kEventDrivenBlocking,
         /*num_qps=*/100, /*num_ops=*/100, /*op_size_bytes=*/32},
        {"NonBlocking", Client::CompletionMethod::kEventDrivenNonBlocking,
         /*num_qps=*/100, /*num_ops=*/100, /*op_size_bytes=*/32},
    }),
    [](const testing::TestParamInfo<EventDrivenCompletionsTest::ParamType>&
           info) { return info.param.test_name; });

}  // namespace
}  // namespace rdma_unit_test
