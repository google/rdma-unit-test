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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_OP_PROFILES_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_OP_PROFILES_H_

#include "absl/flags/declare.h"
#include "traffic/config.pb.h"

ABSL_DECLARE_FLAG(bool, generate_atomics);

namespace rdma_unit_test {

// Returns an OperationGenerator with UD queue pairs and a even mixture of
// operations sizes that we most commonly test.
Config::OperationProfile MixedSizeUdOpProfile();

// Returns an OperationProfile with RC queue pairs and a even mixture of all
// operation types and operation sizes that we most commonly test.
Config::OperationProfile MixedRcOpProfile();

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_OP_PROFILES_H_
