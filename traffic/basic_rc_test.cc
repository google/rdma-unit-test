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
#include <array>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "public/status_matchers.h"
#include "traffic/op_profiles.h"
#include "traffic/op_types.h"
#include "traffic/rdma_stress_fixture.h"

ABSL_FLAG(int, total_ops, 1000000,
          "The number of total ops to issue for each iteration of RunTest. The "
          "ops will be divided evenly amongst the qps.");

namespace rdma_unit_test {
namespace {

struct BasicRcTestParameter {
  std::string test_name;
  int num_qps;
  int op_size;
  int total_ops;
};

class BasicRcTest : public RdmaStressFixture,
                    public testing::WithParamInterface<BasicRcTestParameter> {};

TEST_P(BasicRcTest, BasicRcTests) {
  BasicRcTestParameter parameters = GetParam();
  int num_qps = parameters.num_qps;
  int op_size = parameters.op_size;
  int total_ops = parameters.total_ops;
  // Make sure that max_inflight_ops is large enough for the maximum number of
  // qps used for this test.
  constexpr int kMaxInflightOpsPerQp = 16;
  constexpr int kBatchPerQp = 8;
  int max_inflight_ops = kMaxInflightOpsPerQp * num_qps;

  // Create two clients for the test.
  const Client::Config client_config = {
      .max_op_size = op_size,
      .max_outstanding_ops_per_qp = max_inflight_ops,
      .max_qps = num_qps};
  Client initiator(/*client_id=*/0, context(), NewPd(), port_gid(),
                   client_config),
      target(/*client_id=*/1, context(), NewPd(), port_gid(), client_config);
  LOG(INFO) << "initiator id: " << initiator.client_id()
            << ", target id: " << target.client_id();
  CreateSetUpRcQps(initiator, target, num_qps);
  int ops_per_qp = total_ops / num_qps;

  // Test each op type independently.
  for (OpTypes op_type : {OpTypes::kRead, OpTypes::kWrite, OpTypes::kSend}) {
    ConfigureLatencyMeasurements(op_type);
    LOG(INFO) << "Executing " << op_size << "B " << TestOp::ToString(op_type)
              << " on " << num_qps << " qps.";

    // Test this combination of number of qps, op size, and
    // op type
    auto op_generator = ConstantRcOperationGenerator(op_type, op_size);
    for (int qp_id = 0; qp_id < num_qps; ++qp_id) {
      initiator.GetQpState(qp_id)->set_op_generator(&op_generator);
    }
    initiator.ExecuteOps(target, num_qps, ops_per_qp, kBatchPerQp,
                         kMaxInflightOpsPerQp, max_inflight_ops);

    HaltExecution(initiator);
    CollectClientLatencyStats(initiator);
  }

  DumpState(initiator);
  EXPECT_THAT(validation_->PostTestValidation(), IsOk());
  CheckLatencies();
}

std::vector<BasicRcTestParameter> GenerateVaryQpOpSizePameters(
    std::vector<int> qps_options, std::vector<int> op_size_options,
    std::vector<int> total_ops_options) {
  std::vector<BasicRcTestParameter> params;
  for (int qps : qps_options) {
    for (int op_size : op_size_options) {
      for (int total_ops : total_ops_options) {
        BasicRcTestParameter param{
            .test_name = absl::StrFormat("%dQp%dBytes%dOpsTest", qps, op_size,
                                         total_ops),
            .num_qps = qps,
            .op_size = op_size,
            .total_ops = total_ops,
        };
        params.push_back(param);
      }
    }
  }
  return params;
}

INSTANTIATE_TEST_SUITE_P(
    VaryQpOpSizeTests, BasicRcTest,
    testing::ValuesIn(GenerateVaryQpOpSizePameters(
        /*qps_options=*/{1, 10},
        /*op_size_options=*/
        {64, 512, 1024, 2 * 1024, 4 * 1024, 8 * 1024, 16 * 1024},
        /*total_ops_options=*/{absl::GetFlag(FLAGS_total_ops)})),
    [](const testing::TestParamInfo<BasicRcTest::ParamType>& info) {
      return info.param.test_name;
    });

// This test encompasses http://b/176186131
TEST_F(BasicRcTest, MixedOpTypeSizeTest) {
  RandomizedOperationGenerator rc_op_generator(MixedRcOpProfile());
  const int max_op_bytes = rc_op_generator.MaxOpSize();
  constexpr int kNumQps = 100;
  constexpr int kMaxInflightOps = 32;

  // Create two clients for the test.
  const Client::Config kClientConfig = {
      .max_op_size = max_op_bytes,
      .max_outstanding_ops_per_qp = kMaxInflightOps,
      .max_qps = kNumQps};
  Client initiator(/*client_id=*/0, context(), NewPd(), port_gid(),
                   kClientConfig),
      target(/*client_id=*/1, context(), NewPd(), port_gid(), kClientConfig);
  LOG(INFO) << "initiator id: " << initiator.client_id()
            << ", target id: " << target.client_id();

  CreateSetUpRcQps(initiator, target, kNumQps);
  for (int qp_id = 0; qp_id < kNumQps; ++qp_id) {
    initiator.GetQpState(qp_id)->set_op_generator(&rc_op_generator);
  }

  const int kTotalOps = absl::GetFlag(FLAGS_total_ops);
  const int kOpsPerQp = kTotalOps / kNumQps;
  initiator.ExecuteOps(target, kNumQps, kOpsPerQp, /*batch_per_qp=*/1,
                       kMaxInflightOps, kMaxInflightOps);

  HaltExecution(initiator);
  CollectClientLatencyStats(initiator);
  DumpState(initiator);
  EXPECT_THAT(validation_->PostTestValidation(), IsOk());
  CheckLatencies();
}

}  // namespace
}  // namespace rdma_unit_test
