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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_IMPL_INTROSPECTION_REGISTRAR_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_IMPL_INTROSPECTION_REGISTRAR_H_

#include <functional>
#include <string_view>
#include <utility>
#include <vector>

#include "infiniband/verbs.h"

namespace rdma_unit_test {

// ProviderRegistrar allows external providers to register themselves to
// modifying standard behaviour.
class NicIntrospection;

class IntrospectionRegistrar {
 public:
  using Factory = std::function<NicIntrospection*(const ibv_device_attr& attr)>;

  static IntrospectionRegistrar& GetInstance();
  Factory GetFactory(std::string_view device);
  void Register(std::string_view device, Factory factory);

 private:
  IntrospectionRegistrar() = default;
  ~IntrospectionRegistrar() = default;
  std::vector<std::pair<std::string_view, Factory>> devices_;
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_IMPL_INTROSPECTION_REGISTRAR_H_
