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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_INTROSPECTION_RXE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_INTROSPECTION_RXE_H_

#include <string>

#include "absl/container/flat_hash_map.h"
#include "infiniband/verbs.h"
#include "internal/introspection_registrar.h"
#include "public/introspection.h"

namespace rdma_unit_test {

// Concrete class to override specific behaviour for SoftROCE RXE.
class IntrospectionRxe : public NicIntrospection {
 public:
  // Register SoftROCE NIC with the Introspection Registrar.
  static void Register() {
    IntrospectionRegistrar::GetInstance().Register(
        "rxe", [](const std::string& name, const ibv_device_attr& attr) {
          return new IntrospectionRxe(name, attr);
        });
  }

  bool SupportsIpV6() const override { return false; }

  bool SupportsRcQp() const override { return false; }

  bool IsSlowNic() const override { return true; }

 protected:
  const absl::flat_hash_map<TestcaseKey, std::string>& GetDeviations()
      const override {
    static const absl::flat_hash_map<TestcaseKey, std::string> deviations{
        // Returns success completion.
        {{"BufferTest", "ZeroByteReadInvalidRKey"}, ""},
        // Zero byte write is successful.
        {{"BufferTest", "ZeroByteWriteInvalidRKey"}, ""},
        // Hardware returns true when requesting notification on a CQ without a
        // Completion Channel.
        {{"CompChannelTest", "RequestNotificationOnCqWithoutCompChannel"}, ""},
        {{"CompChannelTest", "AcknowledgeWithoutOutstanding"},
         "Provider crashes when ack-ing without outstanding completion."},
        {{"PdRcLoopbackMrTest", "BasicReadMrOtherPdLocal"},
         "Provider does not support PD."},
        {{"PdRcLoopbackMrTest", "BasicReadMrOtherPdRemote"},
         "Provider does not support PD."},
        {{"PdRcLoopbackMrTest", "BasicWriteMrOtherPdLocal"},
         "Provider does not support PD."},
        {{"PdRcLoopbackMrTest", "BasicWriteMrOtherPdRemote"},
         "Provider does not support PD."},
        {{"PdRcLoopbackMrTest", "BasicFetchAddMrOtherPdLocal"},
         "Provider does not support PD."},
        {{"PdRcLoopbackMrTest", "BasicFetchAddMrOtherPdRemote"},
         "Provider does not support PD."},
        {{"PdRcLoopbackMrTest", "BasicCompSwapMrOtherPdLocal"},
         "Provider does not support PD."},
        {{"PdRcLoopbackMrTest", "BasicCompSwapMrOtherPdRemote"},
         "Provider does not support PD."},
        {{"PdUdLoopbackTest", "SendAhOnOtherPd"},
         "Provider does not support PD."},
        {{"SrqPdTest", "SrqRecvMrSrqMatch"}, ""},
        {{"SrqPdTest", "SrqRecvMrSrqMismatch"}, ""},
        // TODO(author2): Be more specific.
        {{"QpTest", "OverflowSendWr"}, "Does not handle overflow QP."},
        {{"QpTest", "UnknownType"}, "Can create QPs of unknown type."},
        // Does not handle overflow well.
        {{"SrqTest", "OverflowSrq"}, ""},
    };
    return deviations;
  }

 private:
  IntrospectionRxe() = delete;
  ~IntrospectionRxe() = default;
  IntrospectionRxe(const std::string& name, const ibv_device_attr& attr)
      : NicIntrospection(name, attr) {}
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_INTROSPECTION_RXE_H_
