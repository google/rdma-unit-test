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

#include "random_walk/internal/grpc_update_dispatcher.h"

#include <cstdint>
#include <memory>
#include <string>

#include "glog/logging.h"
#include "absl/container/flat_hash_map.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "grpcpp/client_context.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/grpcpp.h"
#include "grpcpp/security/credentials.h"
#include "grpcpp/support/status.h"
#include "public/map_util.h"
#include "public/status_matchers.h"
#include "random_walk/internal/client_update_service.grpc.pb.h"
#include "random_walk/internal/client_update_service.pb.h"
#include "random_walk/internal/rpc_server.h"
#include "random_walk/internal/types.h"

namespace rdma_unit_test {
namespace random_walk {

GrpcUpdateDispatcher::GrpcUpdateDispatcher(ClientId owner_id)
    : owner_id_(owner_id) {}

void GrpcUpdateDispatcher::RegisterRemoteUpdateHandler(
    ClientId client_id, const std::string& grpc_server_addr) {
  RemoteHandler handler{.client_id = client_id,
                        .server_addr = grpc_server_addr};
  std::shared_ptr<::grpc::ChannelCredentials> creds =
  ::grpc::InsecureChannelCredentials();
  handler.channel = grpc::CreateChannel(grpc_server_addr, creds);
  handler.stub = ClientUpdateService::NewStub(handler.channel);
  map_util::InsertOrDie(rpc_infos_, client_id, handler);
}

void GrpcUpdateDispatcher::DispatchUpdate(const ClientUpdate& update) {
  if (update.has_destination_id()) {
    SendUpdate(update.destination_id(), update);
  } else {
    for (const auto& [client_id, remote_handler] : rpc_infos_) {
      SendUpdate(client_id, update);
    }
  }
}

void GrpcUpdateDispatcher::SendUpdate(ClientId client_id,
                                      const ClientUpdate& update) {
  struct RpcArgs {
    ::grpc::ClientContext context;
    OrderedUpdateRequest request;
    UpdateResponse response;
  };
  DCHECK(rpc_infos_.find(client_id) != rpc_infos_.end());
  RemoteHandler& info = rpc_infos_.at(client_id);
  RpcArgs* args = new RpcArgs;
  args->context.set_deadline(
      absl::ToChronoTime(absl::Now() + absl::Seconds(20)));
  *args->request.mutable_update() = update;
  args->request.set_source_id(owner_id_);
  args->request.set_sequence_number(++info.next_sequence_number);

  info.stub->async()->Update(&args->context, &args->request, &args->response,
                             [args](::grpc::Status s) {
                               if (!s.ok()) {
                                 LOG(FATAL) << s.error_message();  // Crash ok
                               }
                               delete args;
                             });
}

}  // namespace random_walk
}  // namespace rdma_unit_test
