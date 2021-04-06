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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_IMPL_INTROSPECTION_RXE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_IMPL_INTROSPECTION_RXE_H_

#include "infiniband/verbs.h"
#include "impl/introspection_registrar.h"
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

  bool SupportsIpV6() const { return false; }

  bool CorrectlyReportsQueuePairErrors() const { return false; }

  bool CorrectlyReportsInvalidRemoteKeyErrors() const { return false; }

 private:
  IntrospectionRxe() = delete;
  ~IntrospectionRxe() = default;
  explicit IntrospectionRxe(const ibv_device_attr& attr)
      : NicIntrospection(attr) {}
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_IMPL_INTROSPECTION_RXE_H_
