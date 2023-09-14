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
#include <tuple>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "absl/strings/str_format.h"
#include "absl/time/time.h"
#include "google/protobuf/text_format.h"
#include "public/basic_fixture.h"
#include "public/introspection.h"
#include "random_walk/action_weights.h"
#include "random_walk/internal/multi_node_orchestrator.h"
#include "random_walk/internal/random_walk_config.pb.h"
#include "random_walk/internal/single_node_orchestrator.h"

namespace rdma_unit_test {
namespace random_walk {

class RandomWalkTest : public BasicFixture {
 protected:
  // All random walk runs for 20 seconds. If we want to stress, use
  // --test_per_run flag.
  absl::Duration kRandomWalkDuration = absl::Seconds(20);
};

TEST_F(RandomWalkTest, SingleNodeSingleClientControlPath) {
  ActionWeights weights = UniformControlPathActions();
  SingleNodeOrchestrator orchestrator(1, weights);
  orchestrator.RunClients(kRandomWalkDuration);
}

TEST_F(RandomWalkTest, SingleNodeSingleClientRdma) {
  ActionWeights weights = SimpleRdmaActions();
  SingleNodeOrchestrator orchestrator(1, weights);
  orchestrator.RunClients(kRandomWalkDuration);
}

TEST_F(RandomWalkTest, SingleNodeTwoClientsRdma) {
  ActionWeights weights = SimpleRdmaActions();
  SingleNodeOrchestrator orchestrator(2, weights);
  orchestrator.RunClients(kRandomWalkDuration);
}

TEST_F(RandomWalkTest, MultiNodeTwoClientsRdma) {
  ActionWeights weights = SimpleRdmaActions();
  MultiNodeOrchestrator orchestrator(2, weights);
  orchestrator.RunClients(kRandomWalkDuration);
}

}  // namespace random_walk
}  // namespace rdma_unit_test
