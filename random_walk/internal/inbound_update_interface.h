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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_INBOUND_UPDATE_INTERFACE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_INBOUND_UPDATE_INTERFACE_H_

#include "random_walk/internal/client_update_service.pb.h"

namespace rdma_unit_test {
namespace random_walk {

// The interface an UpdateHandler use to interfact with RandomWalkClient.
// Provides methods for pushing a remote ClientUpdate to the client.
class InboundUpdateInterface {
 public:
  virtual ~InboundUpdateInterface() = default;

  // Pushes a remote ClientUpdate to a RandomWalkClient.
  virtual void PushInboundUpdate(const ClientUpdate& update) = 0;
};

}  // namespace random_walk
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_INBOUND_UPDATE_INTERFACE_H_
