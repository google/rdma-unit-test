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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_INTROSPECTION_MLX5_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_INTROSPECTION_MLX5_H_

#include <string>

#include "absl/container/flat_hash_map.h"
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
        "mlx5", [](const std::string& name, const ibv_device_attr& attr) {
          return new IntrospectionMlx5(name, attr);
        });
  }

  bool SupportsRcSendWithInvalidate() const override { return false; }

  bool SupportsRcRemoteMwAtomic() const override { return false; }

 protected:
  const absl::flat_hash_map<TestcaseKey, std::string>& GetDeviations()
      const override {
    static const absl::flat_hash_map<TestcaseKey, std::string> deviations{
        // Deregistering unknown AH handles will cause client crashes.
        {{"AhTest", "DeregInvalidAh"}, ""},
        // MW address not checked at bind.
        {{"BufferMwTest", "BindExceedFront"}, ""},
        {{"BufferMwTest", "BindExceedRear"}, ""},
        {{"CompChannelTest", "RequestNotificationInvalidCq"},
         "Invalid CQ handle is not checked."},
        // Hardware returns true when requesting notification on a CQ without a
        // Completion Channel.
        {{"CompChannelTest", "RequestNotificationOnCqWithoutCompChannel"}, ""},
        // Will hang.
        {{"CompChannelTest", "AcknowledgeWithoutOutstanding"}, ""},
        // Will hang.
        {{"CompChannelTest", "AcknowledgeTooMany"}, ""},
        // Allows invalid SGE size for atomics.
        {{"LoopbackRcQpTest", "FetchAddInvalidSize"}, ""},
        {{"LoopbackRcQpTest", "FetchAddSmallSge"}, ""},
        {{"LoopbackRcQpTest", "FetchAddLargeSge"}, ""},
        {{"LoopbackRcQpTest", "CompareSwapInvalidSize"}, ""},
        // Fails to send completion when qp in error state.
        {{"LoopbackRcQpTest", "ReqestOnFailedQp"}, ""},
        // No completions when remote in error state.
        {{"LoopbackRcQpTest", "SendRemoteQpInErrorState"},
         "Provider does not generate local completion when remote is in error "
         "state."},
        {{"LoopbackRcQpTest", "ReadRemoteQpInErrorState"},
         "Provider does not generate local completion when remote is in error "
         "state."},
        {{"LoopbackRcQpTest", "WriteRemoteQpInErrorState"},
         "Provider does not generate local completion when remote is in error "
         "state."},
        {{"LoopbackRcQpTest", "FetchAddRemoteQpInErrorState"},
         "Provider does not generate local completion when remote is in error "
         "state."},
        {{"LoopbackRcQpTest", "CompareSwapRemoteQpInErrorState"},
         "Provider does not generate local completion when remote is in error "
         "state."},
        {{"MwTest", "InvalidQp"}, "Allows bind to invalid qp."},
        // Allows creation over device cap.
        {{"QpTest", "ExceedsDeviceCap"}, ""},
    };
    return deviations;
  }

 private:
  IntrospectionMlx5() = delete;
  ~IntrospectionMlx5() = default;
  explicit IntrospectionMlx5(const std::string& name,
                             const ibv_device_attr& attr)
      : NicIntrospection(name, attr) {
  }
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_INTROSPECTION_MLX5_H_
