/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_INTROSPECTION_MLX4_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_INTROSPECTION_MLX4_H_

#include "absl/container/flat_hash_map.h"
#include "infiniband/verbs.h"
#include "internal/introspection_registrar.h"
#include "public/introspection.h"

namespace rdma_unit_test {

// Concrete class to override specific behaviour for Mellonox NIC.  The
// following anaomolies have been observed during unit test development
// QpTest::BasicSetup appears to SIGSEGV if max_inline_data is set.
class IntrospectionMlx4 : public NicIntrospection {
 public:
  // Register MLX4 NIC with the Introspection Registrar.
  static void Register() {
    IntrospectionRegistrar::GetInstance().Register(
        "mlx4", [](const ibv_device_attr& attr) {
          return new IntrospectionMlx4(attr);
        });
  }

  bool FullCqIdlesQp() const override { return true; }

  bool SupportsReRegMr() const override { return true; }

  bool SupportsTrafficClass() const override { return true; }

 protected:
  const absl::flat_hash_map<TestcaseKey, std::string>& GetDeviations()
      const override {
    static const absl::flat_hash_map<TestcaseKey, std::string> deviations{
        {{"AhTest", "DeallocPdWithOutstandingAh"},
         "Allows Deallocating PD with Ah."},
        {{"AhTest", "DeregInvalidAh"}, "Unknown AH handles crashes client."},
        {{"BufferMrTest", "ReadZeroByte"},
         "Zero byte read gives IBV_WC_LOC_QP_OP_ERR"},
        {{"BufferMrTest", "ReadZeroByteOutsideMr"},
         "Zero byte read gives IBV_WC_LOC_QP_OP_ERR"},
        {{"BufferMrTest", "ReadZeroByteFromZeroByteMr"},
         "Zero byte read gives IBV_WC_LOC_QP_OP_ERR"},
        {{"BufferMrTest", "ReadZeroByteOutsideZeroByteMr"},
         "Zero byte read gives IBV_WC_LOC_QP_OP_ERR"},
        {{"BufferMrTest", "ReadZeroByteInvalidRKey"},
         "Zero byte read gives IBV_WC_LOC_QP_OP_ERR"},
        {{"BufferMwTest", "ReadZeroByte"},
         "Zero byte read gives IBV_WC_LOC_QP_OP_ERR"},
        {{"BufferMwTest", "ReadZeroByteOutsideMw"},
         "Zero byte read gives IBV_WC_LOC_QP_OP_ERR"},
        {{"BufferMwTest", "ReadZeroByteFromZeroByteMw"},
         "Zero byte read gives IBV_WC_LOC_QP_OP_ERR"},
        {{"BufferMwTest", "ReadZeroByteOutsideZeroByteMw"},
         "Zero byte read gives IBV_WC_LOC_QP_OP_ERR"},
        // Zero byte write is successful.
        {{"BufferTest", "ZeroByteWriteInvalidRKey"}, ""},
        {{"CompChannelTest", "RequestNotificationInvalidCq"},
         "Invalid CQ handle is not checked."},
        {{"CompChannelTest", "RequestNotificationOnCqWithoutCompChannel"},
         "NIC does not return immediate error when requesting notification on "
         "CQ without Completion Channel."},
        {{"CompChannelTest", "AcknowledgeWithoutOutstanding"},
         "Ack-ing nonexistent completion will crash the client."},
        {{"DeviceLimitTest", "MaxAh"}, "Client crashes on too many AHs."},
        {{"DeviceLimitTest", "MaxMw"},
         "max_mw is not correctly reported for the device."},
        {{"DeviceLimitTest", "MaxQp"}, "Can create much more QPs than max_qp."},
        // Provider still update remote buffer when LKey is invalid.
        {{"LoopbackRcQpTest", "FetchAddInvalidLKey"}, ""},
        // Allows bind to invalid qp.
        {{"MwTest", "InvalidQp"}, ""},
    };
    return deviations;
  }

 private:
  IntrospectionMlx4() = delete;
  ~IntrospectionMlx4() = default;
  explicit IntrospectionMlx4(const ibv_device_attr& attr)
      : NicIntrospection(attr) {
    // ibv_query_device may report the incorrect capabilities for some cards.
    // Override result when checking for Type2 support.
    attr_.device_cap_flags &= ~IBV_DEVICE_MEM_WINDOW_TYPE_2B;
  }
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_INTROSPECTION_MLX4_H_
