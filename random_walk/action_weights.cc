// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "random_walk/action_weights.h"

#include <string>

#include "absl/log/check.h"
#include "google/protobuf/text_format.h"
#include "public/status_matchers.h"
#include "random_walk/internal/random_walk_config.pb.h"

namespace rdma_unit_test {
namespace random_walk {

ActionWeights UniformControlPathActions() {
  const std::string kWeights = R"pb(create_cq: 1
                                    destroy_cq: 1
                                    allocate_pd: 2
                                    deallocate_pd: 1
                                    register_mr: 1
                                    deregister_mr: 1
                                    allocate_type_1_mw: 1
                                    allocate_type_2_mw: 1
                                    deallocate_type_1_mw: 1
                                    deallocate_type_2_mw: 1
                                    bind_type_1_mw: 0
                                    bind_type_2_mw: 0
                                    create_rc_qp_pair: 1
                                    create_ud_qp: 1
                                    modify_qp_error: 0
                                    destroy_qp: 0
                                    create_ah: 1
                                    destroy_ah: 1
                                    send: 0
                                    send_with_inv: 0
                                    recv: 0
                                    read: 0
                                    write: 0
                                    fetch_add: 0
                                    comp_swap: 0
  )pb";

  ActionWeights weights;
  google::protobuf::TextFormat::ParseFromString(kWeights, &weights);
  return weights;
}

ActionWeights SimpleRdmaActions() {
  const std::string kWeights = R"pb(create_cq: 1
                                    destroy_cq: 1
                                    allocate_pd: 2
                                    deallocate_pd: 1
                                    register_mr: 2
                                    deregister_mr: 2
                                    allocate_type_1_mw: 2
                                    allocate_type_2_mw: 2
                                    deallocate_type_1_mw: 2
                                    deallocate_type_2_mw: 2
                                    bind_type_1_mw: 2
                                    bind_type_2_mw: 2
                                    create_rc_qp_pair: 2
                                    create_ud_qp: 1
                                    modify_qp_error: 0
                                    destroy_qp: 3
                                    create_ah: 2
                                    destroy_ah: 2
                                    send: 0
                                    send_with_inv: 0
                                    recv: 0
                                    read: 20
                                    write: 20
                                    fetch_add: 0
                                    comp_swap: 0
  )pb";

  ActionWeights weights;
  bool result = google::protobuf::TextFormat::ParseFromString(kWeights, &weights);
  CHECK_EQ(result, true) << "Failed to parse proto from string.";  // Crash ok
  return weights;
}

}  // namespace random_walk
}  // namespace rdma_unit_test
