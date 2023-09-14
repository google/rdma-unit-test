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

#include "internal/introspection_registrar.h"

#include <string_view>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "absl/strings/match.h"

namespace rdma_unit_test {

IntrospectionRegistrar& IntrospectionRegistrar::GetInstance() {
  static auto* kInstance = new IntrospectionRegistrar();
  return *kInstance;
}

IntrospectionRegistrar::Factory IntrospectionRegistrar::GetFactory(
    std::string_view device) {
  for (auto [name, factory] : devices_) {
    if (absl::StartsWith(device, name)) {
      return factory;
    }
  }
  return nullptr;
}

void IntrospectionRegistrar::Register(std::string_view device,
                                      Factory factory) {
  devices_.push_back({device, factory});
}

}  // namespace rdma_unit_test
