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

#include "random_walk/internal/multi_node_orchestrator.h"

#include <cstddef>
#include <memory>
#include <thread>  // NOLINT
#include <vector>

#include "absl/time/time.h"
#include "grpcpp/grpcpp.h"
#include "infiniband/verbs.h"
#include "public/verbs_helper_suite.h"
#include "public/verbs_util.h"
#include "random_walk/internal/grpc_update_dispatcher.h"
#include "random_walk/internal/grpc_update_handler.h"
#include "random_walk/internal/random_walk_client.h"
#include "random_walk/internal/random_walk_config.pb.h"
#include "random_walk/internal/rpc_server.h"
#include "random_walk/internal/types.h"

namespace rdma_unit_test {
namespace random_walk {

MultiNodeOrchestrator::MultiNodeOrchestrator(size_t num_clients,
                                             const ActionWeights& weights) {
  clients_.resize(num_clients);
  handlers_.resize(num_clients);
  std::vector<std::shared_ptr<GrpcUpdateDispatcher>> dispatchers(num_clients,
                                                                 nullptr);

  for (ClientId id = 0; id < num_clients; ++id) {
    clients_[id] = std::make_shared<RandomWalkClient>(id, weights);
    dispatchers[id] = std::make_shared<GrpcUpdateDispatcher>(id);
    clients_[id]->RegisterUpdateDispatcher(dispatchers[id]);
    handlers_[id] = std::make_unique<GrpcUpdateHandler>(clients_[id]);
  }

  for (ClientId local_id = 0; local_id < num_clients; ++local_id) {
    for (ClientId remote_id = 0; remote_id < num_clients; ++remote_id) {
      clients_[local_id]->AddRemoteClient(remote_id,
                                          clients_[remote_id]->GetGid());
      dispatchers[local_id]->RegisterRemoteUpdateHandler(
          remote_id, handlers_[remote_id]->GetServerAddress());
    }
  }
}

void MultiNodeOrchestrator::RunClients(absl::Duration duration) {
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

void MultiNodeOrchestrator::RunClients(size_t num_steps) {
  std::vector<std::thread> client_threads;
  for (const auto& client : clients_) {
    client_threads.emplace_back([&]() { return client->Run(num_steps); });
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
