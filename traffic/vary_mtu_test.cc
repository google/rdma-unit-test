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

#include <algorithm>
#include <memory>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "infiniband/verbs.h"
#include "internal/verbs_attribute.h"
#include "public/status_matchers.h"
#include "public/verbs_util.h"
#include "traffic/client.h"
#include "traffic/op_types.h"
#include "traffic/operation_generator.h"
#include "traffic/qp_state.h"
#include "traffic/rdma_stress_fixture.h"

namespace rdma_unit_test {
namespace {

class VaryMtuTest : public RdmaStressFixture,
                    public testing::WithParamInterface</*mtu*/ ibv_mtu> {};

// For each MTU, create a single QP with a single large op. Check that both that
// the op completes successfully, and that the number of transactions is
// consistent with the MTU size.
TEST_P(VaryMtuTest, VaryMtuTest) {
  constexpr int kOpBytes = 16384;
  ConstantRcOperationGenerator op_generator(OpTypes::kWrite, kOpBytes);
  constexpr int kNumQps = 1;
  constexpr int kOpsPerQp = 1;
  constexpr int kMaxInflightOps = 1;
  constexpr Client::Config kClientConfig = {
      .max_op_size = kOpBytes,
      .max_outstanding_ops_per_qp = kMaxInflightOps,
      .max_qps = kNumQps};

  ibv_mtu mtu = GetParam();
  LOG(INFO) << "Running VaryMtuTest with MTU = " << mtu;

  // Create two clients for the test.
  Client initiator(/*client_id=*/0, context(), port_attr(), kClientConfig),
      target(/*client_id=*/1, context(), port_attr(), kClientConfig);
  LOG(INFO) << "initiator id: " << initiator.client_id()
            << ", target id: " << target.client_id();

  CreateSetUpRcQps(initiator, target, kNumQps, QpAttribute().set_path_mtu(mtu));
  for (uint32_t qp_id = 0; qp_id < kNumQps; ++qp_id) {
    initiator.qp_state(qp_id)->set_op_generator(&op_generator);
  }

  initiator.ExecuteOps(target, kNumQps, kOpsPerQp, /*batch_per_qp=*/1,
                       kMaxInflightOps, kMaxInflightOps);

  HaltExecution(initiator);
  HaltExecution(target);
  EXPECT_OK(validation_->PostTestValidation());
}

INSTANTIATE_TEST_SUITE_P(
    VaryMtuTest, VaryMtuTest,
    /*mtu=*/
    testing::Values(IBV_MTU_256, IBV_MTU_512, IBV_MTU_1024, IBV_MTU_2048,
                    IBV_MTU_4096),
    [](const testing::TestParamInfo<VaryMtuTest::ParamType>& info) {
      return absl::StrFormat("%dmtu", verbs_util::VerbsMtuToInt(info.param));
    });

}  // namespace
}  // namespace rdma_unit_test
