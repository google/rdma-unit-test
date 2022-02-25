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

#include "random_walk/internal/single_node_orchestrator.h"

#include <cstddef>
#include <memory>
#include <thread>  // NOLINT
#include <vector>

#include "absl/time/time.h"
#include "infiniband/verbs.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"
#include "random_walk/internal/loopback_update_dispatcher.h"
#include "random_walk/internal/random_walk_client.h"
#include "random_walk/internal/random_walk_config.pb.h"
#include "random_walk/internal/types.h"

namespace rdma_unit_test {
namespace random_walk {

SingleNodeOrchestrator::SingleNodeOrchestrator(size_t num_clients,
                                               const ActionWeights& weights) {
  clients_.resize(num_clients);
  std::vector<std::shared_ptr<LoopbackUpdateDispatcher>> dispatchers(
      num_clients, nullptr);
  for (ClientId id = 0; id < num_clients; ++id) {
    clients_[id] = std::make_shared<RandomWalkClient>(id, weights);
    dispatchers[id] = std::make_shared<LoopbackUpdateDispatcher>();
    clients_[id]->RegisterUpdateDispatcher(dispatchers[id]);
  }

  for (ClientId local_id = 0; local_id < num_clients; ++local_id) {
    for (ClientId remote_id = 0; remote_id < num_clients; ++remote_id) {
      clients_[local_id]->AddRemoteClient(remote_id,
                                          clients_[remote_id]->GetGid());
      dispatchers[local_id]->RegisterRemote(remote_id, clients_[remote_id]);
    }
  }
}

void SingleNodeOrchestrator::RunClients(absl::Duration duration) {
  std::vector<std::thread> client_threads;
  for (const auto& client : clients_) {
    client_threads.emplace_back([&]() { return client->Run(duration); });
  }
  for (auto& client : client_threads) {
    client.join();
  }
  for (const auto& client : clients_) {
    client->PrintStats();
  }
}

void SingleNodeOrchestrator::RunClients(size_t steps) {
  std::vector<std::thread> client_threads;
  for (const auto& client : clients_) {
    client_threads.emplace_back([&]() { return client->Run(steps); });
  }
  for (auto& client : client_threads) {
    client.join();
  }
  for (const auto& client : clients_) {
    client->PrintStats();
  }
}

}  // namespace random_walk
}  // namespace rdma_unit_test
