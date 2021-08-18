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
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

#include "infiniband/verbs.h"
#include "cases/basic_fixture.h"
#include "public/flags.h"
#include "public/status_matchers.h"
#include "public/util.h"
#include "public/verbs_helper_suite.h"

namespace rdma_unit_test {

using ::testing::NotNull;

class AhTest : public BasicFixture {
 protected:
  struct BasicSetup {
    ibv_context* context;
    verbs_util::PortGid port_gid;
    ibv_pd* pd;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_gid = ibv_.GetLocalPortGid(setup.context);
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    return setup;
  }
};

TEST_F(AhTest, CreateAh) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_ah* ah = ibv_.CreateAh(setup.pd, setup.port_gid.gid);
  EXPECT_THAT(ah, NotNull());
}

TEST_F(AhTest, DeregUnknownAh) {
  if (Introspection().ShouldDeviateForCurrentTest()) {
    GTEST_SKIP() << "transport handling of unknown AH will crash.";
  }
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_ah dummy;
  dummy.context = setup.context;
  dummy.handle = -1;
  EXPECT_EQ(ibv_destroy_ah(&dummy), ENOENT);
}

TEST_F(AhTest, DeallocPdWithOutstandingAh) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_ah* ah = ibv_.CreateAh(setup.pd, setup.port_gid.gid);
  ASSERT_THAT(ah, NotNull());
  int result = ibv_.DeallocPd(setup.pd);
  if (Introspection().ShouldDeviateForCurrentTest()) {
    EXPECT_EQ(result, 0);
  } else {
    EXPECT_EQ(result, EBUSY);
  }
}

}  // namespace rdma_unit_test
