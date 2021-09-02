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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_MULTI_NODE_ORCHESTRATOR_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_MULTI_NODE_ORCHESTRATOR_H_

#include <stddef.h>

#include <memory>
#include <vector>

#include "absl/time/time.h"
#include "infiniband/verbs.h"
#include "random_walk/internal/grpc_update_dispatcher.h"
#include "random_walk/internal/grpc_update_handler.h"
#include "random_walk/internal/random_walk_client.h"
#include "random_walk/internal/random_walk_config.pb.h"
#include "random_walk/internal/rpc_server.h"

namespace rdma_unit_test {
namespace random_walk {

// The class creates and coordinates multiple RandomWalkClients on multiple
// nodes, one per client, to perform RDMA random walk. This process entails.
//   1. Create the TestOrchestrator object.
//   2. Call RunClients.

class MultiNodeOrchestrator {
 public:
  MultiNodeOrchestrator(size_t num_clients, const ActionWeights& weights);
  // Movable but not copyable.
  MultiNodeOrchestrator(MultiNodeOrchestrator&& orch) = default;
  MultiNodeOrchestrator& operator=(MultiNodeOrchestrator&& orch) = default;
  MultiNodeOrchestrator(const MultiNodeOrchestrator& orch) = delete;
  MultiNodeOrchestrator& operator=(const MultiNodeOrchestrator& orch) = delete;
  ~MultiNodeOrchestrator() = default;

  // Creates and Runs a Network of RandomWalkClients for a fixed duration.
  void RunClients(absl::Duration duration);
  // Creates and Runs a Network of RandomWalkClients for a fixed number of
  // steps.
  void RunClients(size_t num_steps);

 private:
  std::vector<std::shared_ptr<RandomWalkClient>> clients_;
  std::vector<std::unique_ptr<GrpcUpdateHandler>> handlers_;
};

}  // namespace random_walk
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_MULTI_NODE_ORCHESTRATOR_H_
