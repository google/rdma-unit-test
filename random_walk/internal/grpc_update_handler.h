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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_GRPC_UPDATE_HANDLER_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_GRPC_UPDATE_HANDLER_H_

#include <memory>
#include <string>

#include "grpcpp/server.h"
#include "random_walk/internal/inbound_update_interface.h"
#include "random_walk/internal/rpc_server.h"

namespace rdma_unit_test {
namespace random_walk {

// This is a RAII class that maintains a gRPC server to receive
// ClientUpdate.
class GrpcUpdateHandler {
 public:
  GrpcUpdateHandler() = delete;
  GrpcUpdateHandler(std::shared_ptr<InboundUpdateInterface> client);
  // Movable but not copyable.
  GrpcUpdateHandler(GrpcUpdateHandler&& handler) = default;
  GrpcUpdateHandler& operator=(GrpcUpdateHandler&& handler) = default;
  GrpcUpdateHandler(const GrpcUpdateHandler& handler) = delete;
  GrpcUpdateHandler& operator=(const GrpcUpdateHandler& handler) = delete;
  ~GrpcUpdateHandler();

  // Returns the address of the gRPC server.
  std::string GetServerAddress() const;

 private:
  std::string server_address_;
  std::unique_ptr<grpc::Server> rpc_server_ = nullptr;
  std::shared_ptr<RpcServer> backend_ = nullptr;
};

}  // namespace random_walk
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_GRPC_UPDATE_HANDLER_H_
