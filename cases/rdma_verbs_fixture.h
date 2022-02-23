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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_CASES_RDMA_VERBS_FIXTURE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_CASES_RDMA_VERBS_FIXTURE_H_

#include "cases/basic_fixture.h"

namespace rdma_unit_test {

// Test fixture for all unit test: Provides helper functions from member ibv_
// to ensure all ibverbs resource automatically tears down at the end of each
// test.
class RdmaVerbsFixture : public BasicFixture {
 protected:
  VerbsHelperSuite ibv_;
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_CASES_RDMA_VERBS_FIXTURE_H_
