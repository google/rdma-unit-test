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
#include "internal/verbs_attribute.h"
#include "public/status_matchers.h"
#include "public/verbs_helper_suite.h"
#include "unit/rdma_verbs_fixture.h"

namespace rdma_unit_test {

using ::testing::NotNull;

class AhTest : public RdmaVerbsFixture {
 protected:
  struct BasicSetup {
    ibv_context* context;
    PortAttribute port_attr;
    // A simple AH attribute that points (loopback) from the port to the port
    // that port_attr defines.
    ibv_ah_attr simple_ah_attr;
    ibv_pd* pd;
  };

  absl::StatusOr<BasicSetup> CreateBasicSetup() {
    BasicSetup setup;
    ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
    setup.port_attr = ibv_.GetPortAttribute(setup.context);
    setup.simple_ah_attr = AhAttribute().GetAttribute(
        setup.port_attr.port, setup.port_attr.gid_index, setup.port_attr.gid);
    setup.pd = ibv_.AllocPd(setup.context);
    if (!setup.pd) {
      return absl::InternalError("Failed to allocate pd.");
    }
    return setup;
  }
};

TEST_F(AhTest, CreateAndDestroy) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_ah* ah = ibv_.extension().CreateAh(setup.pd, setup.simple_ah_attr);
  ASSERT_THAT(ah, NotNull());
  EXPECT_EQ(ah->context, setup.context);
  EXPECT_EQ(ah->pd, setup.pd);
  EXPECT_EQ(ibv_destroy_ah(ah), 0);
}

TEST_F(AhTest, DestroyWithInvalidHandle) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_ah* ah = ibv_.extension().CreateAh(setup.pd, setup.simple_ah_attr);
  uint32_t handle = ah->handle;
  ah->handle = 0xDEADBEEF;
  int result = ibv_destroy_ah(ah);
  EXPECT_NE(result, 0);
  if (result != 0) {
    ah->handle = handle;
    ibv_destroy_ah(ah);
  }
}

TEST_F(AhTest, DeallocPdWithOutstandingAh) {
  ASSERT_OK_AND_ASSIGN(BasicSetup setup, CreateBasicSetup());
  ibv_ah* ah = ibv_.CreateAh(setup.pd, setup.simple_ah_attr);
  ASSERT_THAT(ah, NotNull());
  EXPECT_EQ(ibv_.DeallocPd(setup.pd), EBUSY);
}

}  // namespace rdma_unit_test
