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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_UNIT_RDMA_VERBS_FIXTURE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_UNIT_RDMA_VERBS_FIXTURE_H_

#include "public/basic_fixture.h"
#include "public/verbs_helper_suite.h"

namespace rdma_unit_test {

// Test fixture for all unit test:
// 1. Provides helper functions from member ibv_ to ensure all ibverbs resource
// automatically is auto torn down at the end of each test.
// 2. Provides options to dump hardware counters at the start and end of each
// test.
class RdmaVerbsFixture : public BasicFixture {
 protected:
  void SetUp() override;
  void TearDown() override;

  VerbsHelperSuite ibv_;
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_UNIT_RDMA_VERBS_FIXTURE_H_
