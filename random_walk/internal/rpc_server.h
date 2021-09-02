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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_RPC_SERVER_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_RPC_SERVER_H_

#include <memory>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "grpcpp/impl/codegen/server_context.h"
#include "random_walk/internal/client_update_service.grpc.pb.h"
#include "random_walk/internal/client_update_service.pb.h"
#include "random_walk/internal/inbound_update_interface.h"
#include "random_walk/internal/types.h"
#include "random_walk/internal/update_reorder_queue.h"

namespace rdma_unit_test {
namespace random_walk {

// The gRPC server class for receiving ClientUpdate.
class RpcServer final : public ClientUpdateService::Service {
 public:
  RpcServer() = delete;
  explicit RpcServer(std::shared_ptr<InboundUpdateInterface> client);
  ~RpcServer() override = default;

  //////
  // RPC handling functions.
  //////
  ::grpc::Status Update(::grpc::ServerContext* context,
                        const OrderedUpdateRequest* request,
                        UpdateResponse* response) override;

 private:
  absl::Mutex mutex_;
  const std::shared_ptr<InboundUpdateInterface> client_;
  absl::flat_hash_map<ClientId, UpdateReorderQueue> reorder_queues_
      ABSL_GUARDED_BY(mutex_);
};

}  // namespace random_walk
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_RPC_SERVER_H_
