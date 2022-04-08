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

#include <string>

#include "glog/logging.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "absl/time/time.h"
#include "public/introspection.h"
#include "random_walk/internal/multi_node_orchestrator.h"
#include "random_walk/internal/random_walk_config.pb.h"
#include "random_walk/internal/single_node_orchestrator.h"

namespace rdma_unit_test {
namespace random_walk {

class RandomWalkTest : public testing::Test {
 public:
  const std::string kDefaultWeight = R"pb(create_cq: 1
                                          destroy_cq: 1
                                          allocate_pd: 2
                                          deallocate_pd: 1
                                          register_mr: 2
                                          deregister_mr: 2
                                          allocate_type_1_mw: 5
                                          allocate_type_2_mw: 5
                                          deallocate_type_1_mw: 5
                                          deallocate_type_2_mw: 5
                                          bind_type_1_mw: 5
                                          bind_type_2_mw: 5
                                          create_rc_qp_pair: 2
                                          create_ud_qp: 1
                                          modify_qp_error: 0
                                          destroy_qp: 2
                                          create_ah: 5
                                          destroy_ah: 5
                                          send: 5
                                          send_with_inv: 0
                                          recv: 5
                                          read: 10
                                          write: 10
                                          fetch_add: 10
                                          comp_swap: 10
  )pb";

  RandomWalkTest() {
  }

  ~RandomWalkTest() {
  }

 protected:
  static void SetUpTestSuite() {
    Introspection();
  }

  static void TearDownTestSuite() {
  }
};

TEST_F(RandomWalkTest, SingleNodeTwoClientRandomWalk20SDefaultWeight) {
  ActionWeights weights;
  google::protobuf::TextFormat::ParseFromString(kDefaultWeight, &weights);
  LOG(INFO) << "Debug string: " << weights.DebugString();
  if (!Introspection().SupportsType2()) {
    weights.set_allocate_type_2_mw(0);
    weights.set_bind_type_2_mw(0);
    weights.set_deallocate_type_2_mw(0);
  }
  SingleNodeOrchestrator orchestrator(2, weights);
  orchestrator.RunClients(absl::Seconds(20));
}

TEST_F(RandomWalkTest, MultiNodeTwoClientRandomWalk20SDefaultWeight) {
  ActionWeights weights;
  google::protobuf::TextFormat::ParseFromString(kDefaultWeight, &weights);
  if (!Introspection().SupportsType2()) {
    weights.set_allocate_type_2_mw(0);
    weights.set_bind_type_2_mw(0);
    weights.set_deallocate_type_2_mw(0);
  }
  MultiNodeOrchestrator orchestrator(2, weights);
  orchestrator.RunClients(absl::Seconds(20));
}

}  // namespace random_walk
}  // namespace rdma_unit_test
