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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_GRPC_UPDATE_DISPATCHER_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_GRPC_UPDATE_DISPATCHER_H_

#include <cstdint>
#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "grpcpp/channel.h"
#include "random_walk/internal/client_update_service.grpc.pb.h"
#include "random_walk/internal/client_update_service.pb.h"
#include "random_walk/internal/types.h"
#include "random_walk/internal/update_dispatcher_interface.h"

namespace rdma_unit_test {
namespace random_walk {

// The class responsible for dispatching ClientUpdates from a RandomWalkClient
// to other RandomWalkClients via gRPC.
class GrpcUpdateDispatcher : public UpdateDispatcherInterface {
 public:
  GrpcUpdateDispatcher() = delete;
  GrpcUpdateDispatcher(ClientId owner_id);
  // Movable but not copable.
  GrpcUpdateDispatcher(GrpcUpdateDispatcher&& handler) = default;
  GrpcUpdateDispatcher& operator=(GrpcUpdateDispatcher&& handler) = default;
  GrpcUpdateDispatcher(const GrpcUpdateDispatcher& handler) = delete;
  GrpcUpdateDispatcher& operator=(const GrpcUpdateDispatcher& handler) = delete;
  ~GrpcUpdateDispatcher() = default;

  // Registers the gRPC server address for a remote UpdateHandler. All
  // ClientUpdate targetting this specific [client_id] will be dispatched to
  // [grpc_server_addr].
  void RegisterRemoteUpdateHandler(ClientId client_id,
                                   const std::string& grpc_server_addr);

  // Implements UpdateDispatcherInterface.
  void DispatchUpdate(const ClientUpdate& update) override;

 private:
  struct RemoteHandler {
    ClientId client_id;
    std::string server_addr;
    std::shared_ptr<::grpc::Channel> channel;
    std::shared_ptr<ClientUpdateService::Stub> stub;
    uint32_t next_sequence_number = 0;
  };

  // Sends an ClientUpdate to the RandomWalkClient specified by [client_id].
  void SendUpdate(ClientId client_id, const ClientUpdate& update);

  // Maps remote RandomWalkClient's Id to the RemoteHandler struct.
  absl::flat_hash_map<uint32_t, RemoteHandler> rpc_infos_;
  // The Id of the RandomWalkClient owning the dispatcher.
  const ClientId owner_id_;
};

}  // namespace random_walk
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_GRPC_UPDATE_DISPATCHER_H_
