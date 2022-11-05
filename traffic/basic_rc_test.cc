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
#include <tuple>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/str_format.h"
#include "public/status_matchers.h"
#include "traffic/op_profiles.h"
#include "traffic/op_types.h"
#include "traffic/operation_generator.h"
#include "traffic/rdma_stress_fixture.h"

namespace rdma_unit_test {
namespace {

class BasicRcTest : public RdmaStressFixture {
 protected:
  struct Config {
    int num_qps;
    int op_size;
    int num_ops;
    int ops_in_flight;
    OperationGenerator* op_generator;
  };

  // Perform RC communication between two Clients:
  // `num_qps`: number of QPs on each client.
  // `op_size`: the size of each operation.
  // `num_ops`: The total number of operations performed, evenly distributed
  // across each client. `op_generator`: See OperationGenerator, used to
  // generate operation parameters.
  void ExecuteRcTest(const Config& config) {
    // Create two clients for the test.
    const Client::Config client_config = {
        .max_op_size =
            std::max(config.op_size, config.op_generator->MaxOpSize()),
        .max_outstanding_ops_per_qp = config.ops_in_flight,
        .max_qps = config.num_qps};
    Client initiator(/*client_id=*/0, context(), port_attr(), client_config),
        target(/*client_id=*/1, context(), port_attr(), client_config);
    LOG(INFO) << "initiator id: " << initiator.client_id()
              << ", target id: " << target.client_id();
    CreateSetUpRcQps(initiator, target, config.num_qps);

    LOG(INFO) << "Executing " << config.num_ops << " " << config.op_size
              << "B ops on " << config.num_qps << " qps.";

    for (int qp_id = 0; qp_id < config.num_qps; ++qp_id) {
      initiator.qp_state(qp_id)->set_op_generator(config.op_generator);
    }

    // Batch size must be smaller than the number of ops inflight.
    int batch_per_qp = std::max(1, config.ops_in_flight / 4);
    initiator.ExecuteOps(
        target, config.num_qps, config.num_ops / config.num_qps, batch_per_qp,
        config.ops_in_flight, config.ops_in_flight * config.num_qps);

    HaltExecution(initiator);
    HaltExecution(target);
    CollectClientLatencyStats(initiator);
    CheckLatencies();
    EXPECT_THAT(validation_->PostTestValidation(), IsOk());
  }
};

class ConstantOpTest : public BasicRcTest,
                       public testing::WithParamInterface<std::tuple<
                           /*num_qps*/ int, /*op_size*/ int, /*num_ops*/ int,
                           /*ops_in_flight*/ int, OpTypes>> {};

// This test verifies the correctness of each op-type one at a time while
// increasing the number of qps, op size and ops inflight.
TEST_P(ConstantOpTest, Basic) {
  const int kNumQps = std::get<0>(GetParam());
  const int kOpSize = std::get<1>(GetParam());
  const int kNumOps = std::get<2>(GetParam());
  const int kOpsInFlight = std::get<3>(GetParam());
  const OpTypes kOpType = std::get<4>(GetParam());
  ConstantRcOperationGenerator op_generator(kOpType, kOpSize);
  const Config kConfig{.num_qps = kNumQps,
                       .op_size = kOpSize,
                       .num_ops = kNumOps,
                       .ops_in_flight = kOpsInFlight,
                       .op_generator = &op_generator};

  ConfigureLatencyMeasurements(kOpType);
  ExecuteRcTest(kConfig);
}

INSTANTIATE_TEST_SUITE_P(
    BasicRcData, ConstantOpTest,
    testing::Combine(
        /*num_qps=*/testing::Values(1, 100, 1000),
        /*op_size=*/testing::Values(32, 256, 2048, 16 * 1024),
        /*num_ops=*/testing::Values(100, 1000000),
        /*ops_in_flight=*/testing::Values(32),
        testing::Values(OpTypes::kRead, OpTypes::kWrite, OpTypes::kSend)),
    [](const testing::TestParamInfo<ConstantOpTest::ParamType>& info) {
      const int num_qps = std::get<0>(info.param);
      const int op_size = std::get<1>(info.param);
      const int num_ops = std::get<2>(info.param);
      const int ops_in_flight = std::get<3>(info.param);
      const OpTypes op_type = std::get<4>(info.param);
      return absl::StrFormat("%dQps%dByteOp%dOps%dInflight%s", num_qps, op_size,
                             num_ops, ops_in_flight, TestOp::ToString(op_type));
    });

INSTANTIATE_TEST_SUITE_P(
    BasicRcAtomic, ConstantOpTest,
    testing::Combine(
        /*num_qps=*/testing::Values(1, 100, 1000),
        /*op_size=*/testing::Values(TestOp::kAtomicWordSize),
        /*num_ops=*/testing::Values(100, 1000000),
        /*ops_in_flight=*/testing::Values(32),
        testing::Values(OpTypes::kFetchAdd, OpTypes::kCompSwap)),
    [](const testing::TestParamInfo<ConstantOpTest::ParamType>& info) {
      const int num_qps = std::get<0>(info.param);
      const int num_ops = std::get<2>(info.param);
      const int ops_in_flight = std::get<3>(info.param);
      const OpTypes op_type = std::get<4>(info.param);
      return absl::StrFormat("%dQps%dOps%dInflight%s", num_qps, num_ops,
                             ops_in_flight, TestOp::ToString(op_type));
    });

class MixedOpsTest : public BasicRcTest,
                     public testing::WithParamInterface<std::tuple<
                         /*num_qps*/ int, /*num_ops*/ int,
                         /*ops_in_flight*/ int, /*enable_atomics*/ bool>> {};

// This test verifies the correctness of a mixture of op-types and op sizes
// posted with increasing number of qps and ops inflight.
TEST_P(MixedOpsTest, Basic) {
  const int kNumQps = std::get<0>(GetParam());
  const int kNumOps = std::get<1>(GetParam());
  const int kOpsInFlight = std::get<2>(GetParam());
  std::unique_ptr<RandomizedOperationGenerator> op_generator;
  if (std::get<3>(GetParam())) {  // atomics enabled.
    op_generator = std::make_unique<RandomizedOperationGenerator>(
        MixedRcOpProfileWithAtomics());
  } else {
    op_generator =
        std::make_unique<RandomizedOperationGenerator>(MixedRcOpProfile());
  }
  const Config kConfig{
      .num_qps = kNumQps,
      .op_size = op_generator->MaxOpSize(),
      .num_ops = kNumOps,
      .ops_in_flight = kOpsInFlight,
      .op_generator = op_generator.get(),
  };

  ExecuteRcTest(kConfig);
}

INSTANTIATE_TEST_SUITE_P(
    BasicRc, MixedOpsTest,
    testing::Combine(/*num_qps=*/testing::Values(1, 100, 1000),
                     /*num_ops=*/testing::Values(100, 1000000),
                     /*ops_in_flight=*/testing::Values(1, 32),
                     /*enable_atomics=*/testing::Bool()),
    [](const testing::TestParamInfo<MixedOpsTest::ParamType>& info) {
      const int num_qps = std::get<0>(info.param);
      const int num_ops = std::get<1>(info.param);
      const int ops_in_flight = std::get<2>(info.param);
      const bool enable_atomics = std::get<3>(info.param);
      return absl::StrFormat("%dQps%dOps%dInflight%sAtomics", num_qps, num_ops,
                             ops_in_flight,
                             (enable_atomics ? "With" : "Without"));
    });

}  // namespace
}  // namespace rdma_unit_test
