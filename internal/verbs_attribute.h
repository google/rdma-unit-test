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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_VERBS_ATTRIBUTE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_VERBS_ATTRIBUTE_H_

// The following classes provides convenient and readable API for customizing
// various attributes used by ibverbs interface. In most of the tests, these
// attributes (such as ibv_qp_attr::timeout) are not elevant and takes on a
// default value while in a small portion of the test, these values needs to
// be specifically set, often via a multi-layer API and thus hard to passed
// down using a single parameter. These classes makes it:
// 1. Easy to create default attributes.
// 2. Easy to modify default attributes.
// 3. Easy to pass attributes around test helper functions.

#include <cstdint>
#include <string>

#include "absl/flags/declare.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "infiniband/verbs.h"

ABSL_DECLARE_FLAG(ibv_mtu, verbs_mtu);

bool AbslParseFlag(absl::string_view text, ibv_mtu* out, std::string* error);

std::string AbslUnparseFlag(ibv_mtu mtu);

namespace rdma_unit_test {

// Helper class for creating ibv_ah_attr used by ibv_create_ah.
class AhAttribute {
 public:
  AhAttribute() = default;
  ~AhAttribute() = default;

  AhAttribute& set_flow_label(uint32_t flow_label);
  AhAttribute& set_hop_limit(uint32_t hop_limit);
  AhAttribute& set_traffic_class(uint32_t traffic_class);
  AhAttribute& set_sl(uint32_t sl);

  ibv_ah_attr GetAttribute(uint8_t port, uint8_t sgid_index,
                           ibv_gid dgid) const;

 private:
  uint32_t flow_label_ = 0;  // 0 to tell route to ignore this field.
  uint8_t hop_limit_ = 127;
  uint8_t traffic_class_ = 0;  // Used by router for delivery priority.
  uint8_t sl_ = 5;             // any nonzero value for service level.
};

// Helper class for creating ibv_qp_attr used by ibv_modified_qp.
class QpAttribute {
 public:
  QpAttribute();
  ~QpAttribute() = default;

  QpAttribute& set_qp_access_flags(unsigned int qp_access_flags);
  QpAttribute& set_pkey_index(uint16_t pkey_index);
  QpAttribute& set_flow_label(uint32_t flow_label);
  QpAttribute& set_hop_limit(uint32_t hop_limit);
  QpAttribute& set_traffic_class(uint32_t traffic_class);
  QpAttribute& set_sl(uint32_t sl);
  QpAttribute& set_path_mtu(ibv_mtu path_mtu);
  QpAttribute& set_rq_psn(uint32_t rq_psn);
  QpAttribute& set_max_dest_rd_atomic(uint8_t max_dest_rd_atomic);
  QpAttribute& set_min_rnr_timer(uint8_t min_rnr_timer);
  QpAttribute& set_sq_psn(uint32_t sq_psn);
  QpAttribute& set_timeout(uint8_t timeout);
  QpAttribute& set_timeout(absl::Duration timeout);
  QpAttribute& set_retry_cnt(uint8_t retry_cnt);
  QpAttribute& set_rnr_retry(uint8_t rnr_retry);
  QpAttribute& set_max_rd_atomic(uint8_t max_rd_atomic);

  // Return ibv_qp_attr and corresponding mask for modifying RC QP from RESET
  // state to INIT state.
  // Note: `port` is the local physical port associated with this QP.
  ibv_qp_attr GetRcResetToInitAttr(uint8_t port) const;
  int GetRcResetToInitMask() const;

  // Return ibv_qp_attr and corresponding mask for modifying RC QP from INIT
  // state to RTR state.
  // Note: `port` is the local physical port that the packet will be sent from.
  ibv_qp_attr GetRcInitToRtrAttr(uint8_t port, uint8_t sgid_index, ibv_gid dgid,
                                 uint32_t dest_qp_num) const;
  int GetRcInitToRtrMask() const;

  // Return ibv_qp_attr and corresponding mask for modifying RC QP from RTR
  // state to RTS state.
  ibv_qp_attr GetRcRtrToRtsAttr() const;
  int GetRcRtrToRtsMask() const;

  // Return ibv_qp_attr and corresponding mask for modifying Ud QP from RESET
  // state to INIT state.
  // Note: `port` is the local physical port associated with this QP.
  ibv_qp_attr GetUdResetToInitAttr(uint8_t port, uint32_t qkey) const;
  int GetUdResetToInitMask() const;

  // Return ibv_qp_attr and corresponding mask for modifying RC QP from INIT
  // state to RTR state.
  // Note: `port` is the local physical port that the packet will be sent from.
  ibv_qp_attr GetUdInitToRtrAttr() const;
  int GetUdInitToRtrMask() const;

  // Return ibv_qp_attr and corresponding mask for modifying RC QP from RTR
  // state to RTS state.
  ibv_qp_attr GetUdRtrToRtsAttr() const;
  int GetUdRtrToRtsMask() const;

 private:
  // Required and optional ambient attributes for bringing QP from RESET to
  // INIT to RTR and to RTS.
  // Note:
  // 1. Does not include ibv_qp_attr::qp_state becauseit is by definition not
  // customiable.
  // 2. Does not include attributes like dest_qp_num because it is not ambient
  // attribute, i.e. it needs to be set on a per case basis.

  // Attributes for bringing QP from RESET to INIT state.
  unsigned int qp_access_flags_ = IBV_ACCESS_REMOTE_WRITE |
                                  IBV_ACCESS_REMOTE_READ |
                                  IBV_ACCESS_REMOTE_ATOMIC;
  uint16_t pkey_index_ = 0;

  // Attributes for bringing QP from INIT to RTR state.
  AhAttribute ah_attr_;
  ibv_mtu path_mtu_ = IBV_MTU_4096;
  uint32_t rq_psn_ = 1024;  // must match sq_psn in RcRtsRequiredAttr.
  uint8_t max_dest_rd_atomic_ = 10;
  uint8_t min_rnr_timer_ = 26;  // 81.92 milliseconds

  // Attributes for bringing QP from RTR to RTS state.
  uint32_t sq_psn_ = 1024;  // must match rq_psn in RcRtrRequiredAttr.
  uint8_t timeout_ = 0;     // infinite timeout
  uint8_t retry_cnt_ = 5;
  uint8_t rnr_retry_ = 5;
  uint8_t max_rd_atomic_ = 10;
};

// Helper class for creating ibv_qp_init_attr used by ibv_create_qp.
class QpInitAttribute {
 public:
  QpInitAttribute() = default;
  ~QpInitAttribute() = default;

  QpInitAttribute& set_max_send_wr(uint32_t max_send_wr);
  QpInitAttribute& set_max_recv_wr(uint32_t max_recv_wr);
  QpInitAttribute& set_max_send_sge(uint32_t max_send_sge);
  QpInitAttribute& set_max_recv_sge(uint32_t max_recv_sge);
  QpInitAttribute& set_max_inline_data(uint32_t max_inline_data);
  QpInitAttribute& set_sq_sig_all(int sq_sig_all);

  // Returns a default ibv_qp_init_attr;
  ibv_qp_init_attr GetAttribute(ibv_cq* send_cq, ibv_cq* recv_cq,
                                ibv_qp_type qp_type,
                                ibv_srq* srq = nullptr) const;

 private:
  uint32_t max_send_wr_ = 200;
  uint32_t max_recv_wr_ = 200;
  uint32_t max_send_sge_ = 1;
  uint32_t max_recv_sge_ = 1;
  // TODO(author2): ibv_device_attr does not have a field
  // for device limit on max_inline_data. Hence we use a conservative
  // value that works on all devices. Move this value into NicIntrospection.
  uint32_t max_inline_data_ = 36;
  int sq_sig_all_ = 0;
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_INTERNAL_VERBS_ATTRIBUTE_H_
