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
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "public/status_matchers.h"
#include "traffic/op_types.h"
#include "traffic/rdma_stress_fixture.h"

namespace rdma_unit_test {
namespace {

struct CqTestParam {
  int send_cq_size;
  int recv_cq_size;
};

class CqOverrunTest : public RdmaStressFixture,
                      public testing::WithParamInterface<CqTestParam> {};

// Test for overrun of send or recv completion queue as follows:
// 1. Create two sets of initiator/target clients A and B. Client A has a small
// completion queue size where as client B has a sufficiently large completion
// queue.
// 2. Post a large number of ops on both clients, at least thrice the size of
// the completion queue.
// 3. Not all ops should finish on client A, and we should see AEs.
// 4. All ops should should finish successfully on client B.
TEST_P(CqOverrunTest, Basic) {
  const int kSendCqSize = GetParam().send_cq_size;
  const int kRecvCqSize = GetParam().recv_cq_size;
  // Number of posted ops is 3 times the size of the smaller completion queue.
  // This is because in some HW implementations, a single 64B CQ entry can store
  // two 32B completions.
  const int kNumOps = 3 * std::min(kSendCqSize, kRecvCqSize);
  const int kLargeCqSize = kNumOps;
  const int kQpsPerClient = 1;
  const int kOpSize = 32;

  // Create a pair of clients A which have a small completion queue.
  Client::Config config = {.max_op_size = kOpSize,
                           .max_outstanding_ops_per_qp = kNumOps,
                           .max_qps = kQpsPerClient,
                           .send_cq_size = kSendCqSize,
                           .recv_cq_size = kRecvCqSize};
  Client initiatorA(/*client_id=*/0, context(), port_attr(), config),
      targetA(/*client_id=*/1, context(), port_attr(), config);

  // Create a pair of clients B which have a large completion queue.
  config.send_cq_size = kLargeCqSize;
  config.recv_cq_size = kLargeCqSize;
  Client initiatorB(/*client_id=*/0, context(), port_attr(), config),
      targetB(/*client_id=*/1, context(), port_attr(), config);

  // Create a qp on both pairs of clients.
  CreateSetUpRcQps(initiatorA, targetA, kQpsPerClient);
  CreateSetUpRcQps(initiatorB, targetB, kQpsPerClient);

  Client::OpAttributes attributes = {
      .op_bytes = kOpSize, .num_ops = 1, .initiator_qp_id = 0};

  // Post a batch of kNumOps ops on both clients A and B. These will fill up the
  // either the send or recv completion queue on client A
  for (int i = 0; i < kNumOps; ++i) {
    attributes.op_type = OpTypes::kRecv;
    ASSERT_OK(targetA.PostOps(attributes));
    ASSERT_OK(targetB.PostOps(attributes));

    attributes.op_type = OpTypes::kSend;
    ASSERT_OK(initiatorA.PostOps(attributes));
    ASSERT_OK(initiatorB.PostOps(attributes));
  }
  LOG(INFO) << "Number of ops posted on each initiator: " << kNumOps;

  // Wait for a sufficiently long time for all ops to finish.
  absl::SleepFor(absl::Seconds(15));

  // Expect that all ops did not successfully finish for client pair A.
  int send_completions_A = initiatorA.TryPollSendCompletions(kNumOps);
  int recv_completions_A = targetA.TryPollRecvCompletions(kNumOps);
  LOG(INFO) << "Send completions on initiatorA: " << send_completions_A;
  LOG(INFO) << "Recv completions on targetA: " << recv_completions_A;
  EXPECT_LE(send_completions_A, kNumOps);
  EXPECT_LE(recv_completions_A, kNumOps);

  // Expect that there are some AEs for client pair A.
  EXPECT_GE(initiatorA.HandleAsyncEvents(), 0);

  // Expect that all ops did finish successfully for client pair B.
  int send_completions_B = initiatorB.TryPollSendCompletions(kNumOps);
  int recv_completions_B = targetB.TryPollRecvCompletions(kNumOps);
  LOG(INFO) << "Send completions on initiatorB: " << send_completions_B;
  LOG(INFO) << "Recv completions on targetB: " << recv_completions_B;
  EXPECT_EQ(send_completions_B, kNumOps);
  EXPECT_EQ(recv_completions_B, kNumOps);

  EXPECT_OK(validation_->PostTestValidation());
}

INSTANTIATE_TEST_SUITE_P(
    CqOverrun, CqOverrunTest,
    testing::ValuesIn<CqTestParam>(
        {{/*send_cq_size=*/128, /*recv_cq_size=*/32},    // Recv CQ overflow.
         {/*send_cq_size=*/32, /*recv_cq_size=*/128}}),  // Send CQ overflow.
    [](const testing::TestParamInfo<CqOverrunTest::ParamType> &info) {
      return absl::StrFormat("%dSendCq%dRecvCq", info.param.recv_cq_size,
                             info.param.send_cq_size);
    });

}  // namespace
}  // namespace rdma_unit_test
