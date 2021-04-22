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

#include <errno.h>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

#include "infiniband/verbs.h"
#include "cases/basic_fixture.h"
#include "cases/status_matchers.h"
#include "public/flags.h"
#include "public/verbs_helper_suite.h"

namespace rdma_unit_test {

class AhTest : public BasicFixture {
 protected:
  struct BasicSetup {
    ibv_context* context;
    ibv_pd* pd;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    auto context_or = ibv_.OpenDevice();
    if (!context_or.ok()) {
      return context_or.status();
    }
    setup.context = context_or.value();
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    return setup;
  }
};

TEST_F(AhTest, CreateAh) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_ah* ah = ibv_.CreateAh(setup.pd);
  EXPECT_NE(nullptr, ah);
}

TEST_F(AhTest, DeregUnknownAh) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP() << "transport handling of unknown AH will crash.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_ah dummy;
  dummy.context = setup.context;
  dummy.handle = -1;
  EXPECT_EQ(ENOENT, ibv_destroy_ah(&dummy));
}

TEST_F(AhTest, DeallocPdWithOutstandingAh) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_ah* ah = nullptr;
  verbs_util::AddressHandleAttributes ah_attr(
      ibv_.GetContextAddressInfo(setup.context));
  ah = ibv_.CreateAh(setup.pd);
  ASSERT_NE(nullptr, ah);
  int result = ibv_.DeallocPd(setup.pd);
  if (Introspection().ShouldDeviateForCurrentTest()) {
    EXPECT_EQ(0, result);
  } else {
    EXPECT_EQ(EBUSY, result);
  }
}

}  // namespace rdma_unit_test
