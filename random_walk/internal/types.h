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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_TYPES_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_TYPES_H_

#include <array>
#include <cstdint>
#include <string>

#include "infiniband/verbs.h"

namespace rdma_unit_test {
namespace random_walk {

typedef uint32_t ClientId;

// The set of available actions (verbs) for random walks.
// To add a new action:
//    1. Add action here to enum class Action.
//    2. Add action to the const std::array kActions in RandomWalkClient.
//    3. Update random_walk_config.proto.
//    4. Add Try[Action name]() to RandomWalkClient implementing the action.
//    5. Modify TryDoRandomAction() and DoAction() in RandomWalkClient
//    accordingly.
enum class Action {
  CREATE_CQ,
  DESTROY_CQ,
  ALLOC_PD,
  DEALLOC_PD,
  REG_MR,
  DEREG_MR,
  ALLOC_TYPE_1_MW,
  ALLOC_TYPE_2_MW,
  BIND_TYPE_1_MW,
  BIND_TYPE_2_MW,
  DEALLOC_TYPE_1_MW,
  DEALLOC_TYPE_2_MW,
  CREATE_RC_QP_PAIR,
  CREATE_UD_QP,
  MODIFY_QP_ERROR,
  DESTROY_QP,
  CREATE_AH,
  DESTROY_AH,
  SEND,
  SEND_WITH_INV,
  RECV,
  READ,
  WRITE,
  FETCH_ADD,
  COMP_SWAP,
};

// The static list of all Actions.
constexpr std::array<Action, 25> kActions = {Action::CREATE_CQ,
                                             Action::DESTROY_CQ,
                                             Action::ALLOC_PD,
                                             Action::DEALLOC_PD,
                                             Action::REG_MR,
                                             Action::DEREG_MR,
                                             Action::ALLOC_TYPE_1_MW,
                                             Action::ALLOC_TYPE_2_MW,
                                             Action::DEALLOC_TYPE_1_MW,
                                             Action::DEALLOC_TYPE_2_MW,
                                             Action::BIND_TYPE_1_MW,
                                             Action::BIND_TYPE_2_MW,
                                             Action::CREATE_RC_QP_PAIR,
                                             Action::CREATE_UD_QP,
                                             Action::MODIFY_QP_ERROR,
                                             Action::DESTROY_QP,
                                             Action::CREATE_AH,
                                             Action::DESTROY_AH,
                                             Action::SEND,
                                             Action::SEND_WITH_INV,
                                             Action::RECV,
                                             Action::READ,
                                             Action::WRITE,
                                             Action::FETCH_ADD,
                                             Action::COMP_SWAP};

// Returns a std::string which is the name of the Action.
std::string ActionToString(const Action& action);

}  // namespace random_walk
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_TYPES_H_
