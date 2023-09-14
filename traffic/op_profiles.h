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

#include "traffic/config.pb.h"

namespace rdma_unit_test {

// Returns an OperationProfile for UD queue pairs with an even mixture of
// operations sizes that we most commonly test.
constexpr int kDefaultMtu = 4096;
Config::OperationProfile MixedSizeUdOpProfile(int mtu = kDefaultMtu);

// Returns an OperationProfile for RC queue pairs with an even mixture of all
// data operation types and operation sizes that we most commonly test.
Config::OperationProfile MixedRcOpProfile();

// Returns an OperationProfile for RC queue pairs with an even mixture of all
// data + atomic operation types and operation sizes that we most commonly test.
Config::OperationProfile MixedRcOpProfileWithAtomics();

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_TRAFFIC_OP_PROFILES_H_
