#ifndef THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_INTROSPECTION_MLX5_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_INTROSPECTION_MLX5_H_

#include "absl/container/flat_hash_set.h"
#include "infiniband/verbs.h"
#include "internal/introspection_registrar.h"
#include "public/introspection.h"

namespace rdma_unit_test {

// Concrete class to override specific behaviour for Mellanox NIC.  The
// following anaomolies have been observed during unit test development
// BufferTest::*
//    allows mw to be bound to a range exceeding the associated mr
// QpTest::BasicSetup
//    appears to SIGSEGV if max_inline_data is set.
class IntrospectionMlx5 : public NicIntrospection {
 public:
  // Register MLX5 NIC with the Introspection Registrar.
  static void Register() {
    IntrospectionRegistrar::GetInstance().Register(
        "mlx5", [](const ibv_device_attr& attr) {
          return new IntrospectionMlx5(attr);
        });
  }

  bool SupportsRcSendWithInvalidate() const override { return false; }

  bool SupportsRcRemoteMwAtomic() const override { return false; }

 protected:
  const absl::flat_hash_set<DeviationEntry>& GetDeviations() const override {
    static const absl::flat_hash_set<DeviationEntry> deviations{
        // Deregistering unknown AH handles will cause client crashes.
        {"AhTest", "DeregInvalidAh", ""},
        // MW address not checked at bind.
        {"BufferMwTest", "BindExceedFront", ""},
        {"BufferMwTest", "BindExceedRear", ""},
        // No check for invalid cq.
        {"CompChannelTest", "RequestNotificationInvalidCq", ""},
        // Hardware returns true when requesting notification on a CQ without a
        // Completion Channel.
        {"CompChannelTest", "RequestNoificationOnCqWithoutCompChannel", ""},
        // Will hang.
        {"CompChannelTest", "AcknowledgeWithoutOutstanding", ""},
        // Will hang.
        {"CompChannelTest", "AcknowledgeTooMany", ""},
        // Allows invalid SGE size for atomics.
        {"LoopbackRcQpTest", "FetchAddInvalidSize", ""},
        {"LoopbackRcQpTest", "FetchAddSmallSge", ""},
        {"LoopbackRcQpTest", "FetchAddLargeSge", ""},
        {"LoopbackRcQpTest", "CompareSwapInvalidSize", ""},
        // Bad recv length larger than region does not cause a failure.
        {"LoopbackRcQpTest", "BadRecvLength", ""},
        // Supports multiple SGEs for atomics.
        {"LoopbackRcQpTest", "FetchAddSplitSgl", ""},
        // Fails to send completion when qp in error state.
        {"LoopbackRcQpTest", "ReqestOnFailedQp", ""},
        // No completions when remote in error state.
        {"LoopbackRcQpTest", "SendRemoteQpInErrorStateRecvWqeAfterTransition",
         "NoCompletion"},
        {"LoopbackRcQpTest", "SendRemoteQpInErrorStateRecvWqeBeforeTransition",
         "NoCompletion"},
        {"LoopbackRcQpTest", "SendRemoteQpInErrorStateNoRecvWqe",
         "NoCompletion"},
        {"LoopbackRcQpTest", "ReadRemoteQpInErrorState", "NoCompletion"},
        {"LoopbackRcQpTest", "WriteRemoteQpInErrorState", "NoCompletion"},
        {"LoopbackRcQpTest", "FetchAddRemoteQpInErrorState", "NoCompletion"},
        {"LoopbackRcQpTest", "CompareSwapRemoteQpInErrorState", "NoCompletion"},
        // Permissions not checked at bind.
        {"MwTest", "BindType1ReadWithNoLocalWrite", ""},
        {"MwTest", "BindType1AtomicWithNoLocalWrite", ""},
        // Deregistering a bound window reports success.
        {"MwTest", "DeregMrWhenBound", ""},
        // Allows bind to invalid qp.
        {"MwTest", "InvalidQp", ""},
        // Allows binding when MR is missing bind permissions.
        {"MwBindTest", "MissingBind", ""},
        // Allows binding when MR is missing bind permissions.
        {"MwBindTest", "NoMrBindAccess", ""},
        // Allows creation over device cap.
        {"QpTest", "ExceedsDeviceCap", ""},
        // Incorrectly report device cap on QPs..
        {"QpTest", "ExceedsMaxQp", ""}};

    return deviations;
  }

 private:
  IntrospectionMlx5() = delete;
  ~IntrospectionMlx5() = default;
  explicit IntrospectionMlx5(const ibv_device_attr& attr)
      : NicIntrospection(attr) {
    // ibv_queury_device incorrectly reports max_qp_wr as 32768.
    // Unable to create RC qp above 8192, and UD qp above 16384
    attr_.max_qp_wr = 8192;
    // ibv_query_device may report the incorrect capabilities for some cards.
    // Override result when checking for Type2 support.
    attr_.device_cap_flags &= ~IBV_DEVICE_MEM_WINDOW_TYPE_2B;
  }
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_INTROSPECTION_MLX5_H_
