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

#include "absl/container/flat_hash_set.h"
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

 protected:
  const absl::flat_hash_set<DeviationEntry>& GetDeviations() const override {
    static const absl::flat_hash_set<DeviationEntry> deviations{
        // Can dealloc PD with outstanding AHs.
        {"AhTest", "DeallocPdWithOutstandingAh", ""},
        // Deregistering unknown AH handles will cause client crashes.
        {"AhTest", "DeregInvalidAh", ""},
        // Zero byte read is an error.
        {"BufferMrTest", "ReadZeroByte", ""},
        {"BufferMrTest", "ReadZeroByteOutsideMr", ""},
        {"BufferMrTest", "ReadZeroByteFromZeroByteMr", ""},
        {"BufferMrTest", "ReadZeroByteOutsideZeroByteMr", ""},
        {"BufferMrTest", "ReadZeroByteInvalidRKey", ""},
        {"BufferMwTest", "ReadZeroByte", ""},
        {"BufferMwTest", "ReadZeroByteOutsideMw", ""},
        {"BufferMwTest", "ReadZeroByteFromZeroByteMw", ""},
        {"BufferMwTest", "ReadZeroByteOutsideZeroByteMw", ""},
        // Zero byte write is successful.
        {"BufferTest", "ZeroByteWriteInvalidRKey", ""},
        // No check for invalid cq.
        {"CompChannelTest", "RequestNotificationInvalidCq", ""},
        // Hardware returns true when requesting notification on a CQ without a
        // Completion Channel.
        {"CompChannelTest", "RequestNoificationOnCqWithoutCompChannel", ""},
        // Will hang.
        {"CompChannelTest", "AcknowledgeWithoutOutstanding", ""},
        // Will hang.
        {"CompChannelTest", "AcknowledgeTooMany", ""},
        // Does not fail with bad recv length.
        {"LoopbackRcQpTest", "BadRecvLength", ""},
        // Supports multiple SGEs for atomics.
        {"LoopbackRcQpTest", "FetchAddSplitSgl", ""},
        // Allows bind to invalid qp.
        {"MwTest", "InvalidQp", ""},
        // Allows creation over max qp.
        {"QpTest", "ExceedsMaxQp", ""},
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
