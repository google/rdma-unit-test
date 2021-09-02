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

#include "random_walk/internal/rpc_server.h"

#include <cstring>
#include <memory>

#include "glog/logging.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "random_walk/internal/client_update_service.grpc.pb.h"
#include "random_walk/internal/client_update_service.pb.h"
#include "random_walk/internal/inbound_update_interface.h"
#include "random_walk/internal/types.h"
#include "random_walk/internal/update_reorder_queue.h"

namespace rdma_unit_test {
namespace random_walk {

RpcServer::RpcServer(std::shared_ptr<InboundUpdateInterface> client)
    : client_(client) {}

grpc::Status RpcServer::Update(grpc::ServerContext* context,
                               const OrderedUpdateRequest* request,
                               UpdateResponse* response) {
  absl::MutexLock guard(&mutex_);
  DCHECK(client_);
  UpdateReorderQueue& reorder_queue = reorder_queues_[request->source_id()];
  reorder_queue.Push(request->sequence_number(), request->update());
  for (auto maybe_update = reorder_queue.Pull(); maybe_update.has_value();
       maybe_update = reorder_queue.Pull()) {
    client_->PushInboundUpdate(maybe_update.value());
  }
  return ::grpc::Status::OK;
}

}  // namespace random_walk
}  // namespace rdma_unit_test
