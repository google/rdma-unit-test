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
#include <cstdint>
#include <memory>
#include <tuple>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/strings/str_format.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"

#include "public/status_matchers.h"
#include "traffic/client.h"
#include "traffic/op_types.h"
#include "traffic/qp_state.h"
#include "traffic/rdma_stress_fixture.h"

namespace rdma_unit_test {
namespace {

class UdBurstTest : public RdmaStressFixture,
                    public testing::WithParamInterface</*burst_size*/ int> {
 protected:
  // Gets a valid remote queue pair counterpart for the initiator queue pair
  // with index initiator_qp_id. Makes sure that the remote queue pair has
  // available space in its queues. For most of the experiment, the first random
  // remote queue pair will have enough space. However, towards the end of the
  // experiment the remote queue pairs will be at capacity. When this is true,
  // iterate through the remote queue pairs to find the first available with
  // space remaining. Because remote queue pair queues should be sized
  // correctly, this function should always return a valid remote queue pair.
  QpState::UdDestination GetUdQpAhWithSpaceLeft(Client& initiator,
                                                Client& target,
                                                uint32_t initiator_qp_id,
                                                int max_outstanding) {
    QpState* initiator_qp_state = initiator.qp_state(initiator_qp_id);
    QpState::UdDestination remote_qp =
        initiator_qp_state->random_ud_destination();
    // Make sure that the remote qp has space left.
    if (remote_qp.qp_state->outstanding_ops_count() ==
        static_cast<uint64_t>(max_outstanding)) {
      for (const QpState::UdDestination& rqp :
           *initiator_qp_state->ud_destinations()) {
        if (rqp.qp_state->outstanding_ops_count() <
            static_cast<uint64_t>(max_outstanding)) {
          remote_qp = rqp;
          break;
        }
      }
      initiator_qp_state->random_ud_destination();
    }
    return remote_qp;
  }

  void PostOps(Client& initiator, Client& target, int num_qps, int ops_per_qp,
               int op_size) {

    std::tuple<uint64_t, uint64_t> transactions;
    auto time_since_last_post = absl::InfinitePast();
    int posted_transactions = 0;
    for (int i = 0; i < ops_per_qp; ++i) {
      for (uint32_t qp_id = 0; qp_id < static_cast<uint32_t>(num_qps);
           ++qp_id) {
        QpState::UdDestination remote_qp =
            GetUdQpAhWithSpaceLeft(initiator, target, qp_id, ops_per_qp);
        ASSERT_OK(target.PostOps(Client::OpAttributes{
            .op_type = OpTypes::kRecv,
            .op_bytes = op_size,
            .num_ops = 1,
            .initiator_qp_id = remote_qp.qp_state->qp_id()}));

        time_since_last_post = absl::Now();
        posted_transactions++;
        // First, sleep for as long as we need to
        auto inter_op_delay = absl::GetFlag(FLAGS_inter_op_delay_us);
        if (inter_op_delay > absl::ZeroDuration()) {
          auto elapsed_time = absl::Now() - time_since_last_post;
          while (elapsed_time < inter_op_delay) {
            const auto sleep_time = inter_op_delay - elapsed_time;
            absl::SleepFor(sleep_time);
            elapsed_time = absl::Now() - time_since_last_post;
          }
        }

        ASSERT_OK(initiator.PostOps(Client::OpAttributes{
            .op_type = OpTypes::kSend,
            .op_bytes = op_size,
            .num_ops = 1,
            .initiator_qp_id = qp_id,
            .ud_send_attributes = Client::UdSendAttributes{
                .remote_qp = remote_qp.qp_state,
                .remote_op_id = remote_qp.qp_state->GetLastOpId(),
                .remote_ah = remote_qp.ah}}));
      }
    }
  }
};

// This test generates a burst of UD packets on multiple QPs. The expectation is
// that a large enough burst will cause UD packets to be dropped. In such a
// scenario, we still expect to receive successful send completions on the
// initiator for all posted ops, and potentially fewer recv completions on the
// target depending on how many packets were dropped.
TEST_P(UdBurstTest, TrafficOkAfterBurst) {
  const int kBurstSize = GetParam();  // Number of ops that will be posted.
  const int kOpsPerQp = 128;          // Should be less than send_queue_size.
  const int kNumQps = kBurstSize / kOpsPerQp;
  const int kOpSize = 1024;
  const int kOpsPerQpPostBurst = 10;

  const Client::Config kClientConfig = {.max_op_size = kOpSize,
                                        .max_outstanding_ops_per_qp = kOpsPerQp,
                                        .max_qps = kNumQps};
  Client initiator(/*client_id=*/0, context(), port_attr(), kClientConfig),
      target(/*client_id=*/1, context(), port_attr(), kClientConfig);

  CreateSetUpOneToOneUdQps(initiator, target, kNumQps);

  PostOps(initiator, target, kNumQps, kOpsPerQp, kOpSize);

  // Check how many send completions we received at the initiator and how many
  // recv completions at the target.
  int recv_completions = 0;
  int send_completions = 0;
  int new_recv_completions = 0;
  int new_send_completions = 0;
  do {
    // It is important to run "long enough" after the burst to ensure all
    // resources were correctly recovered. Keep polling completions until there
    // has been at least 1 second since we last received a completion.
    absl::SleepFor(absl::Seconds(1));

    new_recv_completions = target.TryPollRecvCompletions(kBurstSize);
    recv_completions += new_recv_completions;
    new_send_completions = initiator.TryPollSendCompletions(kBurstSize);
    send_completions += new_send_completions;
  } while (new_recv_completions > 0 || new_send_completions > 0);

  LOG(INFO) << "With UD burst of " << kBurstSize << " ops over " << kNumQps
            << " qps with " << kOpSize << "B size -"
            << " Recv Completions: " << recv_completions
            << " Send Completions: " << send_completions;

  // Number of send completions should be equal to number of ops posted.
  EXPECT_EQ(send_completions, kBurstSize);
  // Number of recv completions is less than or equal to number of ops posted,
  // because some UD packets might have been dropped.
  EXPECT_LE(recv_completions, kBurstSize);

  EXPECT_OK(initiator.ValidateCompletions(recv_completions));

  // Make sure that subsequent traffic is successful after the burst.
  PostOps(initiator, target, kNumQps, kOpsPerQpPostBurst, kOpSize);
  const int kTotalOpsPostBurst = kOpsPerQpPostBurst * kNumQps;
  EXPECT_OK(initiator.PollSendCompletions(kTotalOpsPostBurst));
  EXPECT_OK(target.PollRecvCompletions(kTotalOpsPostBurst));
  EXPECT_OK(initiator.ValidateCompletions(kTotalOpsPostBurst));

  HaltExecution(initiator);
  HaltExecution(target);
  EXPECT_OK(validation_->TransportSnapshot());
}

INSTANTIATE_TEST_SUITE_P(
    UdBurstTest, UdBurstTest,
    /*burst_size=*/testing::Values(1024, 8192, 65536),
    [](const testing::TestParamInfo<UdBurstTest::ParamType>& info) {
      return absl::StrFormat("%dBurstSize", info.param);
    });

}  // namespace
}  // namespace rdma_unit_test
