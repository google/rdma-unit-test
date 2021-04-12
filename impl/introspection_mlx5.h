#ifndef THIRD_PARTY_RDMA_UNIT_TEST_IMPL_INTROSPECTION_MLX5_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_IMPL_INTROSPECTION_MLX5_H_

#include "infiniband/verbs.h"
#include "impl/introspection_registrar.h"
#include "public/introspection.h"

namespace rdma_unit_test {

// Concrete class to override specific behaviour for Mellanox NIC.  The
// following anaomolies have been observed during unit test development
// AHTest::DeregUnknownAh
//    blindly free's the ah. Resulting in an invalid free.
// AHTest::DeallocPdWithOutstandingAh
//    allows pd destruction with AH outstanding.
// BufferTest::*
//    allows mw to be bound to a range exceeding the associated mr
// QPTest::BasicSetup
//    appears to SIGSEGV if max_inline_data is set.
class IntrospectionMlx5 : public NicIntrospection {
 public:
  // Register MLX4 NIC with the Introspection Registrar.
  static void Register() {
    IntrospectionRegistrar::GetInstance().Register(
        "mlx5", [](const ibv_device_attr& attr) {
          return new IntrospectionMlx5(attr);
        });
  }

  bool SupportsRcSendWithInvalidate() const { return false; }

  bool SupportsRcRemoteMwAtomic() const { return false; }

  // CqAdvancedTest::RecvSharedCq failure with multiple outstanding recv
  // requests. Completions are returned but no data transferred which results
  // in the WaitingForChange to fail.
  // TODO(author1): determine if there is a test issue.
  bool SupportsMultipleOutstandingRecvRequests() const { return false; }

  bool CorrectlyReportsInvalidObjects() const { return false; }

  bool CorrectlyReportsCompChannelErrors() const { return false; }

  bool CorrectlyReportsMemoryWindowErrors() const { return false; }

  bool CorrectlyReportsAddressHandleErrors() const { return false; }

  bool CorrectlyReportsInvalidRemoteKeyErrors() const { return false; }

  bool CorrectlyReportsInvalidSizeErrors() const { return false; }

  bool CorrectlyReportsInvalidRecvLengthErrors() const { return false; }

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

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_IMPL_INTROSPECTION_MLX5_H_
