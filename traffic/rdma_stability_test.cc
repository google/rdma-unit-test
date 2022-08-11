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

#include <chrono>  // NOLINT
#include <climits>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <future>  // NOLINT
#include <list>
#include <memory>
#include <optional>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "infiniband/verbs.h"
#include "public/status_matchers.h"
#include "traffic/config.pb.h"
#include "traffic/op_types.h"
#include "traffic/rdma_stress_fixture.h"

// TODO(author5) Add canned configs via TEST_P to replace the flags.
ABSL_FLAG(rdma_unit_test::OpTypes, op_type, rdma_unit_test::OpTypes::kRead,
          "The op type used in the test.");
ABSL_FLAG(absl::Duration, test_duration, absl::Seconds(10),
          "The duration that the test lasts.");
ABSL_FLAG(int, outstanding_ops, 10, "The number of outstainding ops per QP.");
ABSL_FLAG(int, qps, 10, "The number of QPs created by the test.");
ABSL_FLAG(int, op_size, 8, "The op size in bytes.");

namespace rdma_unit_test {
namespace {

constexpr int kMaxOpSize = 4096;
constexpr int kMaxOutstandingOps = 100;
constexpr int kMaxQps = 100;

// Command to run the test:
// --op_size=8 --test_duration=1m --outstanding_ops=100 --qps=100
// --op_type=write/read/send

// The RDMA stability fixture aims to test the RDMA datapath stabilities by
// varying various factors (op size, op type, number of qps and number of
// outstanding ops).Specifically, given a test configuration, the stability
// means (1) how long RDMA datapath can issue ops without errors; (2) how long
// RDMA datapath can repeatedly allocate and deallocate resources without
// errors.
class RdmaStabilityTest : public RdmaStressFixture {
 public:
  static constexpr Client::Config kClientConfig{
      .max_op_size = kMaxOpSize,
      .max_outstanding_ops_per_qp = kMaxOutstandingOps,
      .max_qps = kMaxQps};
};

// For one-sided RC Read/Write completions, this function is expected to
// validate them all at once.
absl::Status PollOneSidedOpsCompletions(Client& initiator, int poll_count) {
  ASSIGN_OR_RETURN(auto completions, initiator.PollSendCompletions(poll_count));
  if (int remaining_ops = completions - poll_count; remaining_ops) {
    LOG(ERROR) << "Initiator has remaining outstanding ops: " << remaining_ops;
    return absl::InternalError(absl::StrCat(
        "Initiator has remaining outstanding ops: ", remaining_ops));
  }
  return initiator.ValidateCompletions(completions);
}

// The Recv/Send completions are async, i.e., the completion on the other side
// may not arrive yet. This function poll completion by considering the async
// behavior.
absl::Status PollTwoSidedOpsCompletions(Client& initiator, Client& target,
                                        int poll_count,
                                        int completions_per_iteration) {
  int ops_validated = 0;
  while (ops_validated < poll_count) {
    ASSIGN_OR_RETURN(auto send_completions,
                     initiator.PollSendCompletions(completions_per_iteration));
    if (int remaining_ops = send_completions - completions_per_iteration;
        remaining_ops) {
      LOG(ERROR) << "Initiator has remaining outstanding ops: "
                 << remaining_ops;
      return absl::InternalError(absl::StrCat(
          "Initiator has remaining outstanding ops: ", remaining_ops));
    }
    ops_validated += initiator.ValidateOrDeferCompletions();

    ASSIGN_OR_RETURN(auto recv_completions,
                     target.PollRecvCompletions(completions_per_iteration));
    if (int remaining_ops = recv_completions - completions_per_iteration;
        remaining_ops) {
      LOG(ERROR) << "Target has remaining outstanding ops: " << remaining_ops;
      return absl::InternalError(absl::StrCat(
          "Target has remaining outstanding ops: ", remaining_ops));
    }
    ops_validated += initiator.ValidateOrDeferCompletions();
  }
  return absl::OkStatus();
}

// We test the stability by focusing on the datapath without worrying about
// resource leaks. The steps are as below:
//  1. Create QPs;
//  2. In a loop, keep issuing ops till the end of test duration;
//  3. Destroy created QPs;
TEST_F(RdmaStabilityTest, StabilityDurationTest) {
  Client initiator(/*client_id=*/0, context(), port_attr(), kClientConfig),
      target(/*client_id=*/1, context(), port_attr(), kClientConfig);
  // Test setup
  // TODO(author5) Create a test suite to replace the test flags.
  int num_qps = absl::GetFlag(FLAGS_qps);
  ASSERT_LE(num_qps, kMaxQps);
  int ops = absl::GetFlag(FLAGS_outstanding_ops);
  ASSERT_LE(ops, kMaxOutstandingOps);
  int op_size = absl::GetFlag(FLAGS_op_size);
  ASSERT_LE(op_size, kMaxOpSize);
  OpTypes op_type = absl::GetFlag(FLAGS_op_type);
  CreateSetUpRcQps(initiator, target, num_qps);

  // Run the test for a certain time. Each iteration, we post N ops, wait until
  // all N ops are completed, and then move to the next iteration.
  absl::Time end_time = absl::Now() + absl::GetFlag(FLAGS_test_duration);
  while (absl::Now() < end_time) {
    // Post operations on all clients.
    Client::OpAttributes attributes = {
        .op_type = op_type, .op_bytes = op_size, .num_ops = ops};
    for (int qp_id = 0; qp_id < num_qps; ++qp_id) {
      attributes.initiator_qp_id = qp_id;
      // For SEND/RECV, we need to post ops on both initiator and targets. Post
      // RECV on target before posting SEND on initiator.
      if (op_type == OpTypes::kRecv || op_type == OpTypes::kSend) {
        attributes.op_type = OpTypes::kRecv;
        ASSERT_OK(target.PostOps(attributes))
            << "Target fails to post RECV\n"
            << validation_->TransportSnapshot();

        attributes.op_type = OpTypes::kSend;
        ASSERT_OK(initiator.PostOps(attributes))
            << "Initiator fails to post SEND\n"
            << validation_->TransportSnapshot();
      } else {
        ASSERT_OK(initiator.PostOps(attributes))
            << "Initiator fails to post ops\n"
            << validation_->TransportSnapshot();
      }
    }
    // Check poll completions.
    switch (op_type) {
      case OpTypes::kSend:
      case OpTypes::kRecv:
        ASSERT_OK(
            PollTwoSidedOpsCompletions(initiator, target, ops * num_qps, ops))
            << "Poll completion error\n"
            << validation_->TransportSnapshot();
        break;
      case OpTypes::kRead:
      case OpTypes::kWrite:
        ASSERT_OK(PollOneSidedOpsCompletions(initiator, ops * num_qps))
            << "Poll completion error\n"
            << validation_->TransportSnapshot();
        break;
      default:
        LOG(FATAL) << "Not supported OP type.";
    }
  }

  ASSERT_OK(PollAndAckAsyncEvents()) << "Has async events\n"
                                     << validation_->TransportSnapshot();

  EXPECT_THAT(validation_->PostTestValidation(), IsOk());
  initiator.CheckAllDataLanded();
  if (op_type == OpTypes::kSend || op_type == OpTypes::kRecv) {
    target.CheckAllDataLanded();
  }
}

// We test the stability by focusing on possible resource leaks. The steps are
// as below:
// In a loop till the test timeouts:
//  1. Create QPs;
//  2. Issue ops for a while;
//  3. Destroy the created QPs;
TEST_F(RdmaStabilityTest, StabilityResourceTest) {
  Client initiator(/*client_id=*/0, context(), port_attr(), kClientConfig),
      target(/*client_id=*/1, context(), port_attr(), kClientConfig);
  // Test setup
  int num_qps = absl::GetFlag(FLAGS_qps);
  ASSERT_LE(num_qps, kMaxQps);
  int ops = absl::GetFlag(FLAGS_outstanding_ops);
  ASSERT_LE(ops, kMaxOutstandingOps);
  int op_size = absl::GetFlag(FLAGS_op_size);
  ASSERT_LE(op_size, kMaxOpSize);
  OpTypes op_type = absl::GetFlag(FLAGS_op_type);
  auto traffic_duration_per_iteration = absl::Milliseconds(10);

  // Run the test for a certain time. Each iteration, we post N ops, wait until
  // all N ops are completed, and then move to the next iteration.
  absl::Time end_time = absl::Now() + absl::GetFlag(FLAGS_test_duration);
  while (absl::Now() < end_time) {
    // Create qps. We timeout the QP creation after 30s.
    std::unique_ptr<std::future<void>> create_qps =
        std::make_unique<std::future<void>>(std::async(
            std::launch::async,
            [&]() { return CreateSetUpRcQps(initiator, target, num_qps); }));
    ASSERT_NE(create_qps->wait_for(std::chrono::seconds(30)),
              std::future_status::timeout)
        << "Fail to create and setup QPs\n"
        << validation_->TransportSnapshot();

    // Generates traffic for X seconds, then go to the next iteration.
    absl::Time traffic_generation_end =
        absl::Now() + traffic_duration_per_iteration;
    while (absl::Now() < traffic_generation_end) {
      // Post operations on all clients.
      Client::OpAttributes attributes = {
          .op_type = op_type, .op_bytes = op_size, .num_ops = ops};
      for (int qp_id = 0; qp_id < num_qps; ++qp_id) {
        attributes.initiator_qp_id = qp_id;
        // For SEND/RECV, we need to post ops on both initiator and targets.
        if (op_type == OpTypes::kRecv || op_type == OpTypes::kSend) {
          attributes.op_type = OpTypes::kRecv;
          ASSERT_OK(target.PostOps(attributes))
              << "Target fails to post RECV\n"
              << validation_->TransportSnapshot();

          attributes.op_type = OpTypes::kSend;
          ASSERT_OK(initiator.PostOps(attributes))
              << "Initiator fails to post SEND\n"
              << validation_->TransportSnapshot();
        } else {
          ASSERT_OK(initiator.PostOps(attributes))
              << "Initiator fails to post ops\n"
              << validation_->TransportSnapshot();
        }
      }
      // Check poll completions.
      switch (op_type) {
        case OpTypes::kSend:
        case OpTypes::kRecv:
          ASSERT_OK(
              PollTwoSidedOpsCompletions(initiator, target, ops * num_qps, ops))
              << "Poll completion error\n"
              << validation_->TransportSnapshot();
          break;
        case OpTypes::kRead:
        case OpTypes::kWrite:
          ASSERT_OK(PollOneSidedOpsCompletions(initiator, ops * num_qps))
              << "Poll completion error\n"
              << validation_->TransportSnapshot();
          break;
        default:
          LOG(FATAL) << "Not supported OP type.";
      }
    }
    // Checks before this iteration.
    ASSERT_OK(PollAndAckAsyncEvents()) << "Has async events\n"
                                       << validation_->TransportSnapshot();
    initiator.CheckAllDataLanded();
    if (op_type == OpTypes::kSend || op_type == OpTypes::kRecv) {
      target.CheckAllDataLanded();
    }

    // Destroy qps
    for (int qp_id = 0; qp_id < num_qps; ++qp_id) {
      ASSERT_OK(initiator.DeleteQp(qp_id))
          << "Fail to destroy initiator QP-" << qp_id << "\n"
          << validation_->TransportSnapshot();
      ASSERT_OK(target.DeleteQp(qp_id))
          << "Fail to destroy target QP-" << qp_id << "\n"
          << validation_->TransportSnapshot();
    }
  }

  EXPECT_THAT(validation_->PostTestValidation(), IsOk());
}

}  // namespace
}  // namespace rdma_unit_test
