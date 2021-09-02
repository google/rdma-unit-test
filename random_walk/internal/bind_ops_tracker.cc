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

#include "random_walk/internal/bind_ops_tracker.h"

#include <cstdint>

#include "infiniband/verbs.h"
#include "public/map_util.h"

namespace rdma_unit_test {
namespace random_walk {

void BindOpsTracker::PushType1MwBind(ibv_mw* mw, ibv_mw_bind bind_wr) {
  BindWr wr_copy{.mw = mw, .bind_info = bind_wr.bind_info};
  map_util::InsertOrDie(bind_wrs_, bind_wr.wr_id, wr_copy);
}

void BindOpsTracker::PushType2MwBind(ibv_send_wr bind_wr) {
  BindWr wr_copy{.mw = bind_wr.bind_mw.mw,
                 .bind_info = bind_wr.bind_mw.bind_info,
                 .rkey = bind_wr.bind_mw.rkey};
  map_util::InsertOrDie(bind_wrs_, bind_wr.wr_id, wr_copy);
}

BindOpsTracker::BindWr BindOpsTracker::ExtractBindWr(uint64_t wr_id) {
  return map_util::ExtractOrDie(bind_wrs_, wr_id);
}

}  // namespace random_walk
}  // namespace rdma_unit_test
