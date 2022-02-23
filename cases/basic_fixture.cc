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

#include "cases/basic_fixture.h"

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "infiniband/verbs.h"
#include "public/flags.h"
#include "public/introspection.h"
#include "public/verbs_helper_suite.h"

namespace rdma_unit_test {

BasicFixture::BasicFixture() {
}

BasicFixture::~BasicFixture() {
}

void BasicFixture::SetUp() {
  auto result = Introspection().KnownIssue();
  if (result.has_value()) {
    GTEST_SKIP() << "Skipping the test because of known issue: "
                 << result.value();
  }
}

void BasicFixture::SetUpTestSuite() {
  // Make initial call to construct the proper Introspection here so the
  // constructor is not called mid-test.
  Introspection();
}

void BasicFixture::TearDownTestSuite() {
}

}  // namespace rdma_unit_test
