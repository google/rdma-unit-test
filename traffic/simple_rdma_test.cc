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

#include <climits>
#include <cstdint>
#include <cstring>
#include <list>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "absl/status/statusor.h"
#include "infiniband/verbs.h"
#include "public/status_matchers.h"
#include "traffic/config.pb.h"
#include "traffic/rdma_stress_fixture.h"

namespace rdma_unit_test {
namespace {

class SimpleRdmaTest : public RdmaStressFixture {
 public:
  static constexpr int kDefaultSmallQpsPerClient = 5;
  static constexpr int kDefaultLargeQpsPerClient = 10;
  static constexpr int kDefaultSmallOpsPerQp = 10;
  static constexpr int kDefaultLargeOpsPerQp = 100;
  static constexpr int kDefaultMaxInflightOps = 10;
  static constexpr int kDefaultOpBytes = 32;
  static constexpr Client::Config kClientConfig{
      .max_op_size = 4096, .max_outstanding_ops_per_qp = 60, .max_qps = 10};
};

TEST_F(SimpleRdmaTest, Write) {
  Client initiator(/*client_id=*/0, context(), port_attr(), kClientConfig),
      target(/*client_id=*/1, context(), port_attr(), kClientConfig);
  CreateSetUpRcQps(initiator, target, kDefaultSmallQpsPerClient);

  // Post operations on all clients.
  Client::OpAttributes attributes = {.op_type = OpTypes::kWrite,
                                     .op_bytes = kDefaultOpBytes,
                                     .num_ops = kDefaultSmallOpsPerQp};
  for (uint32_t qp_id = 0; qp_id < kDefaultSmallQpsPerClient; ++qp_id) {
    attributes.initiator_qp_id = qp_id;
    ASSERT_THAT(initiator.PostOps(attributes), IsOk());
  }
  // Poll completions
  absl::StatusOr<int> completions = initiator.PollSendCompletions(
      kDefaultSmallOpsPerQp * kDefaultSmallQpsPerClient);

  EXPECT_THAT(validation_->PostTestValidation(), IsOk());
  EXPECT_THAT(PollAndAckAsyncEvents(), IsOk());

  // Check if test succeeded.
  EXPECT_THAT(completions, IsOk());
  if (completions.ok()) {
    EXPECT_EQ(kDefaultSmallOpsPerQp * kDefaultSmallQpsPerClient, *completions);
    EXPECT_THAT(initiator.ValidateCompletions(*completions), IsOk());
  }
  HaltExecution(initiator);
  HaltExecution(target);
}

TEST_F(SimpleRdmaTest, Read) {
  Client initiator(/*client_id=*/0, context(), port_attr(), kClientConfig),
      target(/*client_id=*/1, context(), port_attr(), kClientConfig);
  CreateSetUpRcQps(initiator, target, kDefaultSmallQpsPerClient);

  // Post operations on all clients.
  Client::OpAttributes attributes = {.op_type = OpTypes::kRead,
                                     .op_bytes = kDefaultOpBytes,
                                     .num_ops = kDefaultSmallOpsPerQp};
  for (uint32_t qp_id = 0; qp_id < kDefaultSmallQpsPerClient; ++qp_id) {
    attributes.initiator_qp_id = qp_id;
    ASSERT_THAT(initiator.PostOps(attributes), IsOk());
  }

  // Poll completions
  absl::StatusOr<int> completions = initiator.PollSendCompletions(
      kDefaultSmallOpsPerQp * kDefaultSmallQpsPerClient);

  EXPECT_THAT(validation_->PostTestValidation(), IsOk());
  EXPECT_THAT(PollAndAckAsyncEvents(), IsOk());

  // Check if test succeeded.
  EXPECT_THAT(completions, IsOk());
  if (completions.ok()) {
    EXPECT_EQ(kDefaultSmallOpsPerQp * kDefaultSmallQpsPerClient, *completions);
    EXPECT_THAT(initiator.ValidateCompletions(*completions), IsOk());
  }
  HaltExecution(initiator);
  HaltExecution(target);
}

TEST_F(SimpleRdmaTest, Batching) {
  // Tests that batching works when inflight_ops is not a multiple of
  // batch_per_qp

  constexpr int kMaxInflightOps = 42;    // Not a multiple of batch_per_qp
  constexpr int kMaxInflightPerQp = 10;  // Not a multiple of batch_per_qp
  constexpr OpTypes kOpType = OpTypes::kSend;
  constexpr int kBatchPerQp = 4;

  Client initiator(/*client_id=*/0, context(), port_attr(), kClientConfig),
      target(/*client_id=*/1, context(), port_attr(), kClientConfig);
  CreateSetUpRcQps(initiator, target, kDefaultLargeQpsPerClient);
  ConstantRcOperationGenerator op_generator(kOpType, kDefaultOpBytes);
  for (uint32_t qp_id = 0; qp_id < kDefaultLargeQpsPerClient; ++qp_id) {
    initiator.qp_state(qp_id)->set_op_generator(&op_generator);
  }

  int ops_completed = initiator.ExecuteOps(target, kDefaultLargeQpsPerClient,
                                           kDefaultLargeOpsPerQp, kBatchPerQp,
                                           kMaxInflightPerQp, kMaxInflightOps);
  EXPECT_EQ(ops_completed, kDefaultLargeQpsPerClient * kDefaultLargeOpsPerQp);

  EXPECT_THAT(validation_->PostTestValidation(), IsOk());
  EXPECT_THAT(PollAndAckAsyncEvents(), IsOk());

  HaltExecution(initiator);
  HaltExecution(target);
}

TEST_F(SimpleRdmaTest, MixedOpType) {
  // Create a mixture of 100 Read/Write/Send ops on 10Qpairs, with 1/3
  // proportionality of each op type.

  Client initiator(/*client_id=*/0, context(), port_attr(), kClientConfig),
      target(/*client_id=*/1, context(), port_attr(), kClientConfig);
  CreateSetUpRcQps(initiator, target, kDefaultLargeQpsPerClient);

  constexpr float kRatio = 1. / 5.;
  Config::OperationProfile op_profile;
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_write_proportion(kRatio);
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_read_proportion(kRatio);
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_send_proportion(kRatio);
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_fetch_add_proportion(kRatio);
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_comp_swap_proportion(kRatio);

  Config::OperationProfile::OpSizeProportion *op_size_proportion =
      op_profile.add_op_size_proportions();
  op_size_proportion->set_size_bytes(kDefaultOpBytes);
  op_size_proportion->set_proportion(1);

  RandomizedOperationGenerator op_generator(op_profile);
  for (uint32_t qp_id = 0; qp_id < kDefaultLargeQpsPerClient; ++qp_id) {
    initiator.qp_state(qp_id)->set_op_generator(&op_generator);
  }

  int ops_completed = initiator.ExecuteOps(
      target, kDefaultLargeQpsPerClient, kDefaultLargeOpsPerQp,
      /*batch_per_qp=*/1, /*max_inflight_per_qp=*/INT_MAX,
      kDefaultMaxInflightOps);
  EXPECT_EQ(ops_completed, kDefaultLargeQpsPerClient * kDefaultLargeOpsPerQp);

  EXPECT_THAT(validation_->PostTestValidation(), IsOk());
  EXPECT_THAT(PollAndAckAsyncEvents(), IsOk());

  HaltExecution(initiator);
  HaltExecution(target);
}

TEST_F(SimpleRdmaTest, MixedOpSize) {
  // Create a mixture of 100 Write ops on 10Qpairs, with 1/3
  // proportionality of op sizes 32, 64, and 128 bytes.

  Client initiator(/*client_id=*/0, context(), port_attr(), kClientConfig),
      target(/*client_id=*/1, context(), port_attr(), kClientConfig);
  CreateSetUpRcQps(initiator, target, kDefaultLargeQpsPerClient);

  Config::OperationProfile op_profile;
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_write_proportion(1);
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_read_proportion(0);
  op_profile.mutable_rc_op_profile()
      ->mutable_op_type_proportions()
      ->set_send_proportion(0);

  constexpr float kRatio = 1. / 3.;
  const int kOpSizesBytes[3] = {32, 64, 128};
  for (const int &op_size : kOpSizesBytes) {
    Config::OperationProfile::OpSizeProportion *op_size_proportion =
        op_profile.add_op_size_proportions();
    op_size_proportion->set_size_bytes(op_size);
    op_size_proportion->set_proportion(kRatio);
  }

  RandomizedOperationGenerator op_generator(op_profile);
  for (uint32_t qp_id = 0; qp_id < kDefaultLargeQpsPerClient; ++qp_id) {
    initiator.qp_state(qp_id)->set_op_generator(&op_generator);
  }

  int ops_completed = initiator.ExecuteOps(
      target, kDefaultLargeQpsPerClient, kDefaultLargeOpsPerQp,
      /*batch_per_qp=*/1, kDefaultMaxInflightOps, kDefaultMaxInflightOps);
  EXPECT_EQ(ops_completed, kDefaultLargeQpsPerClient * kDefaultLargeOpsPerQp);

  EXPECT_THAT(validation_->PostTestValidation(), IsOk());
  EXPECT_THAT(PollAndAckAsyncEvents(), IsOk());

  HaltExecution(initiator);
  HaltExecution(target);
}

TEST_F(SimpleRdmaTest, RcSend) {
  Client initiator(/*client_id=*/0, context(), port_attr(), kClientConfig),
      target(/*client_id=*/1, context(), port_attr(), kClientConfig);
  CreateSetUpRcQps(initiator, target, kDefaultSmallQpsPerClient);

  // Post operations on all clients.
  constexpr uint32_t kOpsPerQp = 11;
  Client::OpAttributes attributes = {.op_type = OpTypes::kRecv,
                                     .op_bytes = kDefaultOpBytes,
                                     .num_ops = kOpsPerQp};
  for (uint32_t qp_id = 0; qp_id < kDefaultSmallQpsPerClient; ++qp_id) {
    attributes.initiator_qp_id = qp_id;
    ASSERT_THAT(target.PostOps(attributes), IsOk());
  }
  attributes.op_type = OpTypes::kSend;
  for (uint32_t qp_id = 0; qp_id < kDefaultSmallQpsPerClient; ++qp_id) {
    attributes.initiator_qp_id = qp_id;
    ASSERT_THAT(initiator.PostOps(attributes), IsOk());
  }

  // Verify that the fixture can indeed handle asynchronous arrival of
  // Send/Recv completions. To that end, we validate completions in smaller
  // kNumOpsPerQP batches across multiple iteration. Further we break this batch
  // into smaller un-equal sub-batches for validating send/recv completions.
  constexpr int kCompletionBatch = kOpsPerQp;
  int ops_validated = 0;
  while (ops_validated != kOpsPerQp * kDefaultSmallQpsPerClient) {
    int send_first_attempt = kCompletionBatch / 2;
    absl::StatusOr<int> send_completions =
        initiator.PollSendCompletions(send_first_attempt);
    ASSERT_THAT(send_completions, IsOk());
    EXPECT_EQ(send_first_attempt, *send_completions);
    ops_validated += initiator.ValidateOrDeferCompletions();

    int recv_first_attempt = 2 * kCompletionBatch / 3;
    absl::StatusOr<int> recv_completions =
        target.PollRecvCompletions(recv_first_attempt);
    ASSERT_THAT(recv_completions, IsOk());
    EXPECT_EQ(recv_first_attempt, *recv_completions);
    ops_validated += initiator.ValidateOrDeferCompletions();

    send_completions =
        initiator.PollSendCompletions(kCompletionBatch - send_first_attempt);
    ASSERT_THAT(send_completions, IsOk());
    EXPECT_EQ(kCompletionBatch - send_first_attempt, *send_completions);
    ops_validated += initiator.ValidateOrDeferCompletions();

    recv_completions =
        target.PollRecvCompletions(kCompletionBatch - recv_first_attempt);
    ASSERT_THAT(recv_completions, IsOk());
    EXPECT_EQ(kCompletionBatch - recv_first_attempt, *recv_completions);
    ops_validated += initiator.ValidateOrDeferCompletions();
  }
  EXPECT_THAT(validation_->PostTestValidation(), IsOk());
  EXPECT_THAT(PollAndAckAsyncEvents(), IsOk());
  HaltExecution(initiator);
  HaltExecution(target);
}

TEST_F(SimpleRdmaTest, UdSend) {
  Client initiator(/*client_id=*/0, context(), port_attr(), kClientConfig),
      target(/*client_id=*/1, context(), port_attr(), kClientConfig);

  ASSERT_THAT(initiator.CreateQp(/*is_rc=*/false), IsOk());
  ASSERT_THAT(target.CreateQp(/*is_rc=*/false), IsOk());

  ibv_ah *ah = initiator.CreateAh(port_attr());

  constexpr int kOpSize = 256;  // smaller than MTU
  Client::OpAttributes attributes = {.op_type = OpTypes::kRecv,
                                     .op_bytes = kOpSize,
                                     .num_ops = 1,
                                     .initiator_qp_id = 0};
  ASSERT_THAT(target.PostOps(attributes), IsOk());
  attributes.op_type = OpTypes::kSend;
  attributes.ud_send_attributes = {
      .remote_qp = target.qp_state(0), .remote_op_id = 0, .remote_ah = ah};
  ASSERT_THAT(initiator.PostOps(attributes), IsOk());

  // Poll and validate the completion
  std::unique_ptr<TestOp> sender_op = nullptr;
  std::unique_ptr<TestOp> receiver_op = nullptr;

  EXPECT_THAT(initiator.PollSendCompletions(/*count*/ 1), IsOk());
  QpState *sender_qp = initiator.qp_state(0);
  EXPECT_EQ(sender_qp->unchecked_initiated_ops().size(), 1);
  if (!sender_qp->unchecked_initiated_ops().empty()) {
    sender_op = std::move(sender_qp->unchecked_initiated_ops().front());
    sender_qp->unchecked_initiated_ops().pop_front();
    EXPECT_EQ(sender_op->status, IBV_WC_SUCCESS);
  }

  EXPECT_THAT(target.PollRecvCompletions(/*count*/ 1), IsOk());
  QpState *receiver_qp = target.qp_state(0);
  EXPECT_EQ(receiver_qp->unchecked_received_ops().size(), 1);
  if (!receiver_qp->unchecked_received_ops().empty()) {
    receiver_op = std::move(receiver_qp->unchecked_received_ops().front());
    receiver_qp->unchecked_received_ops().pop_front();
    EXPECT_EQ(receiver_op->status, IBV_WC_SUCCESS);
  }

  if (sender_op && receiver_op) {
    EXPECT_EQ(sender_op->length + sizeof(ibv_grh), receiver_op->length);
    EXPECT_EQ(0, std::memcmp(sender_op->src_addr,
                             receiver_op->dest_addr + sizeof(ibv_grh),
                             sender_op->length));
  }

  EXPECT_THAT(validation_->PostTestValidation(), IsOk());
}

TEST_F(SimpleRdmaTest, FetchAdd) {
  Client initiator(/*client_id=*/0, context(), port_attr(), kClientConfig),
      target(/*client_id=*/1, context(), port_attr(), kClientConfig);
  CreateSetUpRcQps(initiator, target, kDefaultSmallQpsPerClient);

  // Post operations on all clients.
  constexpr uint32_t kOpBytes = kAtomicWordSize;
  Client::OpAttributes attributes = {.op_type = OpTypes::kFetchAdd,
                                     .op_bytes = kOpBytes,
                                     .num_ops = kDefaultSmallOpsPerQp};
  for (uint32_t qp_id = 0; qp_id < kDefaultSmallQpsPerClient; ++qp_id) {
    attributes.initiator_qp_id = qp_id;
    attributes.add = static_cast<uint64_t>(qp_id + 1);
    ASSERT_THAT(initiator.PostOps(attributes), IsOk());  // Crash OK
  }

  // Poll completions
  absl::StatusOr<int> completions = initiator.PollSendCompletions(
      kDefaultSmallOpsPerQp * kDefaultSmallQpsPerClient);

  EXPECT_THAT(validation_->PostTestValidation(), IsOk());
  EXPECT_THAT(PollAndAckAsyncEvents(), IsOk());

  // Check if test succeeded.
  EXPECT_THAT(completions, IsOk());
  if (completions.ok()) {
    EXPECT_EQ(kDefaultSmallOpsPerQp * kDefaultSmallQpsPerClient, *completions);
    EXPECT_THAT(initiator.ValidateCompletions(*completions), IsOk());
  }
  for (uint32_t qp_id = 0; qp_id < kDefaultSmallQpsPerClient; ++qp_id) {
    EXPECT_EQ(initiator.qp_state(qp_id)->TotalOpsCompleted(),
              kDefaultSmallOpsPerQp);
  }
}

TEST_F(SimpleRdmaTest, CompareSwap) {
  Client initiator(/*client_id=*/0, context(), port_attr(), kClientConfig),
      target(/*client_id=*/1, context(), port_attr(), kClientConfig);
  CreateSetUpRcQps(initiator, target, kDefaultSmallQpsPerClient);

  // Post compare&swap with no compare value specified, resulting in successful
  // swap.
  constexpr uint32_t kOpBytes = kAtomicWordSize;
  Client::OpAttributes attributes = {.op_type = OpTypes::kCompSwap,
                                     .op_bytes = kOpBytes,
                                     .num_ops = kDefaultSmallOpsPerQp,
                                     .compare = std::nullopt};
  for (uint32_t qp_id = 0; qp_id < kDefaultSmallQpsPerClient; ++qp_id) {
    attributes.initiator_qp_id = qp_id;
    attributes.swap = static_cast<uint64_t>(qp_id + 1);
    ASSERT_THAT(initiator.PostOps(attributes), IsOk());
  }

  // Poll completions
  absl::StatusOr<int> completions = initiator.PollSendCompletions(
      kDefaultSmallOpsPerQp * kDefaultSmallQpsPerClient);

  EXPECT_THAT(validation_->PostTestValidation(), IsOk());
  EXPECT_THAT(PollAndAckAsyncEvents(), IsOk());

  // Check if test succeeded.
  EXPECT_THAT(completions, IsOk());
  if (completions.ok()) {
    EXPECT_EQ(kDefaultSmallOpsPerQp * kDefaultSmallQpsPerClient, *completions);
    EXPECT_THAT(initiator.ValidateCompletions(*completions), IsOk());
  }

  // Post compare&swap ops with compare value specified, resulting in failed
  // swap.
  attributes.compare = 10000;
  for (uint32_t qp_id = 0; qp_id < kDefaultSmallQpsPerClient; ++qp_id) {
    attributes.initiator_qp_id = qp_id;
    attributes.swap = static_cast<uint64_t>(qp_id + 1);
    ASSERT_THAT(initiator.PostOps(attributes), IsOk());
  }

  // Poll completions
  completions = initiator.PollSendCompletions(kDefaultSmallOpsPerQp *
                                              kDefaultSmallQpsPerClient);

  EXPECT_THAT(validation_->PostTestValidation(), IsOk());
  EXPECT_THAT(PollAndAckAsyncEvents(), IsOk());

  // Check if test succeeded.
  EXPECT_THAT(completions, IsOk());
  if (completions.ok()) {
    EXPECT_EQ(kDefaultSmallOpsPerQp * kDefaultSmallQpsPerClient, *completions);
    EXPECT_THAT(initiator.ValidateCompletions(*completions), IsOk());
  }
  for (uint32_t qp_id = 0; qp_id < kDefaultSmallQpsPerClient; ++qp_id) {
    EXPECT_EQ(initiator.qp_state(qp_id)->TotalOpsCompleted(),
              kDefaultSmallOpsPerQp * 2);
  }
}

}  // namespace
}  // namespace rdma_unit_test
