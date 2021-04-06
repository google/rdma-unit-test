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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_IMPL_INTROSPECTION_MLX4_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_IMPL_INTROSPECTION_MLX4_H_

#include "infiniband/verbs.h"
#include "impl/introspection_registrar.h"
#include "public/introspection.h"

namespace rdma_unit_test {

// Concrete class to override specific behaviour for Mellonox NIC.  The
// following anaomolies have been observed during unit test development
// AHTest::DeregUnknownAh
//    blindly free's the ah. Resulting in an invalid free.
// AHTest::DeallocPdWithOutstandingAh
//    allows pd destruction with AH outstanding.
// BufferTest::*
//    has various failures.
// CompChannelTest::AcknowledgeWithoutOutstanding
//    hangs when acknowledging too many.
// CompChannelTest::AcknowledgeTooMany
//    hangs when acknowledging too many.
// MRLoopbackTest::StressDereg
//    does not set opcode on failure.
// QPTest::BasicSetup
//    appears to SIGSEGV if max_inline_data is set.
class IntrospectionMlx4 : public NicIntrospection {
 public:
  // Register MLX4 NIC with the Introspection Registrar.
  static void Register() {
    IntrospectionRegistrar::GetInstance().Register(
        "mlx4", [](const ibv_device_attr& attr) {
          return new IntrospectionMlx4(attr);
        });
  }

  // Returns if the device supports target.
  bool CheckCapability(ibv_device_cap_flags target) const {
    // ibv_query_device may report the incorrect capabilities for some cards.
    // Override result when checking for Type2 support.
    if (target == IBV_DEVICE_MEM_WINDOW_TYPE_2B) return false;
    return (attr_.device_cap_flags & target) > 0;
  }

  bool FullCqIdlesQp() const { return true; }

  bool CanDestroyPdWithAhOutstanding() const { return true; }

  bool CorrectlyReportsCompChannelErrors() const { return false; }

  bool CorrectlyReportRequestionNotifications() const { return false; }

  bool CorrectlyReportsAddressHandleErrors() const { return false; }

  // This is a potential bug on CX3.
  bool CorrectlyReportsMemoryRegionErrors() const { return false; }

 private:
  IntrospectionMlx4() = delete;
  ~IntrospectionMlx4() = default;
  explicit IntrospectionMlx4(const ibv_device_attr& attr)
      : NicIntrospection(attr) {}
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_IMPL_INTROSPECTION_MLX4_H_
