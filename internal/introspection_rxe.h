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

#include "absl/container/flat_hash_set.h"
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
        "rxe",
        [](const ibv_device_attr& attr) { return new IntrospectionRxe(attr); });
  }

  bool SupportsIpV6() const override { return false; }

  bool SupportsRcQp() const override { return false; }

 protected:
  const absl::flat_hash_set<DeviationEntry>& GetDeviations() const override {
    static const absl::flat_hash_set<DeviationEntry> deviations{
        // Supports multiple SGEs for atomics.
        {"LoopbackRcQpTest", "FetchAddSplitSgl", ""},
        // Returns success completion.
        {"BufferTest", "ZeroByteReadInvalidRKey", "no error"},
        // Zero byte write is successful.
        {"BufferTest", "ZeroByteWriteInvalidRKey", ""},
        // Hardware returns true when requesting notification on a CQ without a
        // Completion Channel.
        {"CompChannelTest", "RequestNotificationOnCqWithoutCompChannel", ""},
        // Will hang.
        {"CompChannelTest", "AcknowledgeWithoutOutstanding", ""},
        // Will hang.
        {"CompChannelTest", "AcknowledgeTooMany", ""},
        // RXE PD support is lacking.
        {"PdRcLoopbackMrTest", "BasicReadMrOtherPdLocal", ""},
        {"PdRcLoopbackMrTest", "BasicReadMrOtherPdRemote", ""},
        {"PdRcLoopbackMrTest", "BasicWriteMrOtherPdLocal", ""},
        {"PdRcLoopbackMrTest", "BasicWriteMrOtherPdRemote", ""},
        {"PdRcLoopbackMrTest", "BasicFetchAddMrOtherPdLocal", ""},
        {"PdRcLoopbackMrTest", "BasicFetchAddMrOtherPdRemote", ""},
        {"PdRcLoopbackMrTest", "BasicCompSwapMrOtherPdLocal", ""},
        {"PdRcLoopbackMrTest", "BasicCompSwapMrOtherPdRemote", ""},
        {"PdUdLoopbackTest", "SendAhOnOtherPd", ""},
        {"SrqPdTest", "SrqRecvMrSrqMatch", ""},
        {"SrqPdTest", "SrqRecvMrSrqMismatch", ""},
        // Does not handle overflow well.
        {"QpTest", "OverflowSendWr", ""},
        // Can create QPs of unknown type.
        {"QpTest", "UnknownType", ""},
        // Does not handle overflow well.
        {"SrqTest", "OverflowSrq", ""},
    };
    return deviations;
  }

 private:
  IntrospectionRxe() = delete;
  ~IntrospectionRxe() = default;
  explicit IntrospectionRxe(const ibv_device_attr& attr)
      : NicIntrospection(attr) {}
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_INTROSPECTION_RXE_H_
