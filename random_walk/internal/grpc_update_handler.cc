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

#include "random_walk/internal/grpc_update_handler.h"

#include <memory>
#include <string>

#include "absl/log/check.h"
#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "grpcpp/security/credentials.h"
#include "grpcpp/security/server_credentials.h"
#include "grpcpp/server.h"
#include "grpcpp/server_builder.h"
#include "random_walk/internal/inbound_update_interface.h"
#include "random_walk/internal/rpc_server.h"

namespace rdma_unit_test {
namespace random_walk {

GrpcUpdateHandler::GrpcUpdateHandler(
    std::shared_ptr<InboundUpdateInterface> client) {
  backend_ = std::make_shared<RpcServer>(client);
  ::grpc::ServerBuilder builder;
  int selected_port;
  std::shared_ptr<::grpc::ServerCredentials> creds =
     ::grpc::InsecureServerCredentials();
  builder.AddListeningPort("dns:///localhost:0", creds, &selected_port);
  builder.RegisterService(backend_.get());
  rpc_server_ = builder.BuildAndStart();
  CHECK_GT(selected_port, 0);  // Crash ok
  server_address_ = absl::StrCat("dns:///localhost:", selected_port);
}

GrpcUpdateHandler::~GrpcUpdateHandler() {
  rpc_server_->Shutdown(absl::ToChronoTime(absl::Now() + absl::Seconds(1)));
  rpc_server_->Wait();
}

std::string GrpcUpdateHandler::GetServerAddress() const {
  return server_address_;
}

}  // namespace random_walk
}  // namespace rdma_unit_test
