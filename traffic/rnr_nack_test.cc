#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "internal/verbs_attribute.h"

#include "public/status_matchers.h"
#include "traffic/client.h"
#include "traffic/op_types.h"
#include "traffic/rdma_stress_fixture.h"

namespace rdma_unit_test {
namespace {

class RnrNackTest : public RdmaStressFixture {
 public:
  constexpr static int kInitiatorSendQueueSize = 32;
  constexpr static int kInitiatorRecvQueueSize = 32;
  constexpr static int kTargetSendQueueSize = 16;
  constexpr static int kTargetRecvQueueSize = 16;
  constexpr static int kNumOps = 32;
  constexpr static int kOpSize = 32;

  constexpr static Client::Config kClientConfig = {
      .max_op_size = kOpSize,
      .max_outstanding_ops_per_qp = kNumOps,
      .max_qps = 1};

 protected:

  void CreateAndConnectQp(Client &initiator, Client &target,
                          QpAttribute qp_attr) {
    CHECK_OK(initiator.CreateQp(
        /*is_rc=*/true,
        QpInitAttribute()
            .set_max_send_wr(kInitiatorSendQueueSize)
            .set_max_recv_wr(kInitiatorRecvQueueSize)));  // Crash OK
    CHECK_OK(target.CreateQp(
        /*is_rc=*/true,
        QpInitAttribute()
            .set_max_send_wr(kTargetSendQueueSize)
            .set_max_recv_wr(kTargetRecvQueueSize)));  // Crash OK
    CHECK_OK(SetUpRcClientsQPs(&initiator, /*qp_id=*/0, &target,
                               /*qp_id=*/0, qp_attr));  // Crash OK.
    CHECK_OK(SetUpRcClientsQPs(&target, /*qp_id=*/0, &initiator,
                               /*qp_id=*/0, qp_attr));  // Crash OK.
  }
};

// Test for exercising RNR NACK retry failure. It uses two clients, initiator
// and target with a single qp. The send queue size at the initiator is 32, the
// receive queue size at the target is 16. The test posts 32 send ops on the
// initiator and 16 recv ops on the target. After a sufficiently long time, only
// 16 ops will have successful completions at the initiator and target.
// Additionaly, there will be 16 error completions at the initiator.
TEST_F(RnrNackTest, RetryFailure) {
  Client initiator(/*client_id=*/0, context(), port_attr(), kClientConfig),
      target(/*client_id=*/1, context(), port_attr(), kClientConfig);

  // Set the rnr_timer to minimum and single retry, so that we timeout quickly.
  CreateAndConnectQp(
      initiator, target,
      rdma_unit_test::QpAttribute().set_min_rnr_timer(1).set_rnr_retry(1));

  Client::OpAttributes attributes{.op_type = OpTypes::kRecv,
                                  .op_bytes = kOpSize,
                                  .num_ops = 1,
                                  .initiator_qp_id = 0};
  // Post 16 recv ops at the target.
  for (int i = 0; i < kTargetRecvQueueSize; ++i) {
    ASSERT_OK(target.PostOps(attributes));
  }
  // Post 32 send ops at the initiator.
  attributes.op_type = OpTypes::kSend;
  for (int i = 0; i < kNumOps; ++i) {
    ASSERT_OK(initiator.PostOps(attributes));
  }

  // First 16 ops should return successful completion at initiator and target.
  EXPECT_THAT(initiator.PollSendCompletions(16), IsOkAndHolds(16));
  EXPECT_THAT(target.PollRecvCompletions(16), IsOkAndHolds(16));

  // Sleep for sufficiently long time to cause RNR'ed ops to retry and timeout.
  // Since rnr_timer is 1 (0.01ms) and rnr_retry is 1, we timeout very quickly.
  absl::SleepFor(absl::Seconds(10));

  EXPECT_FALSE(initiator.PollSendCompletions(16).ok());
  EXPECT_FALSE(target.PollRecvCompletions(1).ok());

  HaltExecution(initiator);
  HaltExecution(target);
  EXPECT_OK(validation_->TransportSnapshot());
}

// Test for exercising RNR NACK retry success. It uses two clients, initiator
// and target with a single qp. The send queue size at the initiator is 32, the
// receive queue size at the target is 16. The test posts 32 send ops on the
// initiator and 16 recv ops on the target. After an appropriate time larger
// than half RTT, the target posts additional 16 recv ops. It is expected that
// both initiator and target get the remaining 16 successful completions.
TEST_F(RnrNackTest, RetrySuccess) {
  Client initiator(/*client_id=*/0, context(), port_attr(), kClientConfig),
      target(/*client_id=*/1, context(), port_attr(), kClientConfig);

  // Set the rnr_timer to maximum and infinite retries, so we never timeout.
  CreateAndConnectQp(
      initiator, target,
      rdma_unit_test::QpAttribute().set_min_rnr_timer(31).set_rnr_retry(7));

  Client::OpAttributes attributes{.op_type = OpTypes::kRecv,
                                  .op_bytes = kOpSize,
                                  .num_ops = 1,
                                  .initiator_qp_id = 0};
  // Post 16 recv ops at the target.
  for (int i = 0; i < kTargetRecvQueueSize; ++i) {
    ASSERT_OK(target.PostOps(attributes));
  }
  // Post 32 send ops at the initiator.
  attributes.op_type = OpTypes::kSend;
  for (int i = 0; i < kNumOps; ++i) {
    ASSERT_OK(initiator.PostOps(attributes));
  }

  // First 16 ops should return successful completion at initiator and target.
  EXPECT_THAT(initiator.PollSendCompletions(16), IsOkAndHolds(16));
  EXPECT_THAT(target.PollRecvCompletions(16), IsOkAndHolds(16));

  // Sleep for long enough for send op to be received and cause an RNR nack.
  // Since rnr_timer is max and rnr_retry is infinite, we will never timeout.
  absl::SleepFor(absl::Seconds(1));

  // Post 16 recv ops at the target.
  attributes.op_type = OpTypes::kRecv;
  for (int i = 0; i < kTargetRecvQueueSize; ++i) {
    ASSERT_OK(target.PostOps(attributes));
  }

  // Last 16 ops should return successful completion at initiator and target.
  EXPECT_THAT(initiator.PollSendCompletions(16), IsOkAndHolds(16));
  EXPECT_THAT(target.PollRecvCompletions(16), IsOkAndHolds(16));

  HaltExecution(initiator);
  HaltExecution(target);
  EXPECT_OK(validation_->TransportSnapshot());
}

}  // namespace
}  // namespace rdma_unit_test
