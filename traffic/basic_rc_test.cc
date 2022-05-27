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
#include <tuple>
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
#include "traffic/operation_generator.h"
#include "traffic/rdma_stress_fixture.h"

ABSL_FLAG(int, total_ops, 1000000,
          "The number of total ops to issue for each iteration of RunTest. The "
          "ops will be divided evenly amongst the qps.");

namespace rdma_unit_test {
namespace {

class BasicRcTest : public RdmaStressFixture {
 protected:
  struct Config {
    int num_qps = 1;
    int op_size = 64;
    int num_ops = 1;
    OperationGenerator* op_generator;
  };

  // Perform RC communication between two Clients:
  // `num_qps`: number of QPs on each client.
  // `op_size`: the size of each operation.
  // `num_ops`: The total number of operations performed, evenly distributed
  // across each client. `op_generator`: See OperationGenerator, used to
  // generate operation parameters.
  void ExecuteRcTest(const Config& config) {
    constexpr int kMaxInflightOpsPerQp = 32;
    constexpr int kBatchPerQp = 8;

    // Create two clients for the test.
    const Client::Config client_config = {
        .max_op_size =
            std::max(config.op_size, config.op_generator->MaxOpSize()),
        .max_outstanding_ops_per_qp = kMaxInflightOpsPerQp,
        .max_qps = config.num_qps};
    Client initiator(/*client_id=*/0, context(), port_attr(), client_config),
        target(/*client_id=*/1, context(), port_attr(), client_config);
    LOG(INFO) << "initiator id: " << initiator.client_id()
              << ", target id: " << target.client_id();
    CreateSetUpRcQps(initiator, target, config.num_qps);

    LOG(INFO) << "Executing " << config.num_ops << " " << config.op_size
              << "B ops on " << config.num_ops << " qps.";

    for (int qp_id = 0; qp_id < config.num_qps; ++qp_id) {
      initiator.qp_state(qp_id)->set_op_generator(config.op_generator);
    }

    initiator.ExecuteOps(
        target, config.num_qps, config.num_ops / config.num_qps, kBatchPerQp,
        kMaxInflightOpsPerQp, kMaxInflightOpsPerQp * config.num_qps);

    HaltExecution(initiator);
    CollectClientLatencyStats(initiator);
    DumpState(initiator);
    EXPECT_THAT(validation_->PostTestValidation(), IsOk());
    CheckLatencies();
  }
};

// This test encompasses http://b/176186131
TEST_F(BasicRcTest, MixedRcOps) {
  RandomizedOperationGenerator op_generator(MixedRcOpProfile());
  const Config kConfig{
      .num_qps = 100,
      .op_size = op_generator.MaxOpSize(),
      .num_ops = absl::GetFlag(FLAGS_total_ops),
      .op_generator = &op_generator,
  };

  ExecuteRcTest(kConfig);
}

class RcConstantOpTest : public BasicRcTest,
                         public testing::WithParamInterface<std::tuple<
                             /*num_qps*/ int, /*op_size*/ int, OpTypes>> {};

TEST_P(RcConstantOpTest, Basic) {
  const int kNumQps = std::get<0>(GetParam());
  const int kOpSize = std::get<1>(GetParam());
  const int kNumOps = absl::GetFlag(FLAGS_total_ops);
  const OpTypes kOpType = std::get<2>(GetParam());
  ConstantRcOperationGenerator op_generator(kOpType, kOpSize);
  const Config kConfig{.num_qps = kNumQps,
                       .op_size = kOpSize,
                       .num_ops = kNumOps,
                       .op_generator = &op_generator};

  ConfigureLatencyMeasurements(kOpType);
  ExecuteRcTest(kConfig);
}

INSTANTIATE_TEST_SUITE_P(
    Basic, RcConstantOpTest,
    testing::Combine(
        /*num_qps=*/testing::Values(1, 10),
        /*op_size=*/
        testing::Values(64, 512, 1024, 2 * 1024, 4 * 1024, 8 * 1024, 16 * 1024),
        testing::Values(OpTypes::kRead, OpTypes::kWrite, OpTypes::kSend)),
    [](const testing::TestParamInfo<RcConstantOpTest::ParamType>& info) {
      const int num_qps = std::get<0>(info.param);
      const int op_size = std::get<1>(info.param);
      const int num_ops = absl::GetFlag(FLAGS_total_ops);
      const OpTypes op_type = std::get<2>(info.param);
      return absl::StrFormat("%dQps%dByteOp%dOps%s", num_qps, op_size, num_ops,
                             TestOp::ToString(op_type));
    });

}  // namespace
}  // namespace rdma_unit_test
