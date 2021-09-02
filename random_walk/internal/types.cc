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

#include "random_walk/internal/types.h"

#include <string>

namespace rdma_unit_test {
namespace random_walk {

std::string ActionToString(const Action& action) {
  switch (action) {
    case Action::CREATE_CQ: {
      return "Create CQ";
    }
    case Action::DESTROY_CQ: {
      return "Destroy CQ";
    }
    case Action::ALLOC_PD: {
      return "Allocate PD";
    }
    case Action::DEALLOC_PD: {
      return "Deallocate PD";
    }
    case Action::REG_MR: {
      return "Register MR";
    }
    case Action::DEREG_MR: {
      return "Deregister MR";
    }
    case Action::ALLOC_TYPE_1_MW: {
      return "Allocate Type 1 MW";
    }
    case Action::ALLOC_TYPE_2_MW: {
      return "Allocate Type 2 MW";
    }
    case Action::BIND_TYPE_1_MW: {
      return "Bind Type 1 MW";
    }
    case Action::BIND_TYPE_2_MW: {
      return "Bind Type 2 MW";
    }
    case Action::DEALLOC_TYPE_1_MW: {
      return "Deallocate Type 1 MW";
    }
    case Action::DEALLOC_TYPE_2_MW: {
      return "Deallocate Type 2 MW";
    }
    case Action::CREATE_RC_QP_PAIR: {
      return "Create RC QP pair";
    }
    case Action::CREATE_UD_QP: {
      return "Create UD QP";
    }
    case Action::MODIFY_QP_ERROR: {
      return "Modify QP error";
    }
    case Action::DESTROY_QP: {
      return "Destroy QP";
    }
    case Action::CREATE_AH: {
      return "Create AH";
    }
    case Action::DESTROY_AH: {
      return "Create AH";
    }
    case Action::SEND: {
      return "Send";
    }
    case Action::SEND_WITH_INV: {
      return "Send with invalidate";
    }
    case Action::RECV: {
      return "Receive";
    }
    case Action::READ: {
      return "Read";
    }
    case Action::WRITE: {
      return "Write";
    }
    case Action::FETCH_ADD: {
      return "Fetch Add";
    }
    case Action::COMP_SWAP: {
      return "Compare Swap";
    }
  }
}

}  // namespace random_walk
}  // namespace rdma_unit_test
