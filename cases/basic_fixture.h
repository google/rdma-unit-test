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

#ifndef RDMA_UNIT_TEST_CASES_BASIC_FIXTURE_H_
#define RDMA_UNIT_TEST_CASES_BASIC_FIXTURE_H_

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "infiniband/verbs.h"
#include "public/flags.h"
#include "public/introspection.h"
#include "public/rdma_memblock.h"
#include "public/util.h"
#include "public/verbs_helper_suite.h"

namespace rdma_unit_test {

// A light-weight test fixture for RDMA unit tests. Provides default-created
// VerbsHelperSuite for smart verbs creations and for setting up QPs.
class BasicFixture : public ::testing::Test {
 public:
  BasicFixture() = default;
  ~BasicFixture() override = default;

 protected:
  static void SetUpTestSuite();
  static void TearDownTestSuite();

  VerbsHelperSuite ibv_;
};

}  // namespace rdma_unit_test

#endif  // RDMA_UNIT_TEST_CASES_BASIC_FIXTURE_H_
