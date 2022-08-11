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

#include "internal/verbs_attribute.h"

#include <cstdint>

#include "absl/flags/flag.h"
#include "absl/strings/str_cat.h"
#include "infiniband/verbs.h"

ABSL_FLAG(ibv_mtu, verbs_mtu, IBV_MTU_4096,
          "The MTU value used in modify_qp. Valid values: 256, 512, 1024, "
          "2048, 4096[default]");

bool AbslParseFlag(absl::string_view text, ibv_mtu* out, std::string* error) {
  if (text == "256") {
    *out = IBV_MTU_256;
  } else if (text == "512") {
    *out = IBV_MTU_512;
  } else if (text == "1024") {
    *out = IBV_MTU_1024;
  } else if (text == "2048") {
    *out = IBV_MTU_2048;
  } else if (text == "4096") {
    *out = IBV_MTU_4096;
  } else {
    *error = absl::StrCat("Unknown MTU value : ", text);
    return false;
  }
  return true;
}

std::string AbslUnparseFlag(ibv_mtu mtu) {
  // The enum ibv_mtu use value 1 to 5 for IBV_MTU_256 to IBV_MTU_4096.
  return std::to_string(128 << mtu);
}

namespace rdma_unit_test {

AhAttribute& AhAttribute::set_flow_label(uint32_t flow_label) {
  flow_label_ = flow_label;
  return *this;
}

AhAttribute& AhAttribute::set_hop_limit(uint32_t hop_limit) {
  hop_limit_ = hop_limit;
  return *this;
}

AhAttribute& AhAttribute::set_traffic_class(uint32_t traffic_class) {
  traffic_class_ = traffic_class;
  return *this;
}

AhAttribute& AhAttribute::set_sl(uint32_t sl) {
  sl_ = sl;
  return *this;
}

ibv_ah_attr AhAttribute::GetAttribute(uint8_t port, uint8_t sgid_index,
                                      ibv_gid dgid) const {
  return ibv_ah_attr{
      .grh{.dgid = dgid,
           .flow_label = flow_label_,
           .sgid_index = sgid_index,
           .hop_limit = hop_limit_,
           .traffic_class = traffic_class_},
      // TODO(author2): Maybe look at dlid.
      .sl = sl_,
      .is_global = 1,
      .port_num = port,
  };
}

QpAttribute::QpAttribute() : path_mtu_(absl::GetFlag(FLAGS_verbs_mtu)) {}

QpAttribute& QpAttribute::set_qp_access_flags(unsigned int qp_access_flags) {
  qp_access_flags_ = qp_access_flags;
  return *this;
}

QpAttribute& QpAttribute::set_pkey_index(uint16_t pkey_index) {
  pkey_index_ = pkey_index;
  return *this;
}

QpAttribute& QpAttribute::set_flow_label(uint32_t flow_label) {
  ah_attr_.set_flow_label(flow_label);
  return *this;
}

QpAttribute& QpAttribute::set_hop_limit(uint32_t hop_limit) {
  ah_attr_.set_hop_limit(hop_limit);
  return *this;
}

QpAttribute& QpAttribute::set_traffic_class(uint32_t traffic_class) {
  ah_attr_.set_traffic_class(traffic_class);
  return *this;
}

QpAttribute& QpAttribute::set_sl(uint32_t sl) {
  ah_attr_.set_sl(sl);
  return *this;
}

QpAttribute& QpAttribute::set_path_mtu(ibv_mtu path_mtu) {
  path_mtu_ = path_mtu;
  return *this;
}

QpAttribute& QpAttribute::set_rq_psn(uint32_t rq_psn) {
  rq_psn_ = rq_psn;
  return *this;
}

QpAttribute& QpAttribute::set_max_dest_rd_atomic(uint8_t max_dest_rd_atomic) {
  max_dest_rd_atomic_ = max_dest_rd_atomic;
  return *this;
}

QpAttribute& QpAttribute::set_min_rnr_timer(uint8_t min_rnr_timer) {
  min_rnr_timer_ = min_rnr_timer;
  return *this;
}
QpAttribute& QpAttribute::set_sq_psn(uint32_t sq_psn) {
  sq_psn_ = sq_psn;
  return *this;
}

QpAttribute& QpAttribute::set_timeout(uint8_t timeout) {
  timeout_ = timeout;
  return *this;
}

QpAttribute& QpAttribute::set_timeout(absl::Duration timeout) {
  // ibv_qp_attr::timeout uses value 0 for infinite timeout. For other values,
  // the time calculation is 4.096 * 2^{timeout} usecs.
  uint8_t ibv_timeout = 0;
  for (uint8_t i = 1; i <= 31; ++i) {
    if (timeout < absl::Nanoseconds(4096ull << i)) {
      ibv_timeout = i;
      break;
    }
  }
  return set_timeout(ibv_timeout);
}

QpAttribute& QpAttribute::set_retry_cnt(uint8_t retry_cnt) {
  retry_cnt_ = retry_cnt;
  return *this;
}

QpAttribute& QpAttribute::set_rnr_retry(uint8_t rnr_retry) {
  rnr_retry_ = rnr_retry;
  return *this;
}

QpAttribute& QpAttribute::set_max_rd_atomic(uint8_t max_rd_atomic) {
  max_rd_atomic_ = max_rd_atomic;
  return *this;
}

ibv_qp_attr QpAttribute::GetRcResetToInitAttr(uint8_t port) const {
  return ibv_qp_attr{
      .qp_state = IBV_QPS_INIT,
      .qp_access_flags = qp_access_flags_,
      .pkey_index = pkey_index_,
      .port_num = port,
  };
}

int QpAttribute::GetRcResetToInitMask() const {
  return IBV_QP_STATE | IBV_QP_ACCESS_FLAGS | IBV_QP_PKEY_INDEX | IBV_QP_PORT;
}

ibv_qp_attr QpAttribute::GetRcInitToRtrAttr(uint8_t port, uint8_t sgid_index,
                                            ibv_gid dgid,
                                            uint32_t dest_qp_num) const {
  return ibv_qp_attr{.qp_state = IBV_QPS_RTR,
                     .path_mtu = path_mtu_,
                     .rq_psn = rq_psn_,
                     .dest_qp_num = dest_qp_num,
                     .ah_attr = ah_attr_.GetAttribute(port, sgid_index, dgid),
                     .max_dest_rd_atomic = max_dest_rd_atomic_,
                     .min_rnr_timer = min_rnr_timer_};
}

int QpAttribute::GetRcInitToRtrMask() const {
  return IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_RQ_PSN | IBV_QP_DEST_QPN |
         IBV_QP_AV | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
}

ibv_qp_attr QpAttribute::GetRcRtrToRtsAttr() const {
  return ibv_qp_attr{.qp_state = IBV_QPS_RTS,
                     .sq_psn = sq_psn_,
                     .max_rd_atomic = max_rd_atomic_,
                     .timeout = timeout_,
                     .retry_cnt = retry_cnt_,
                     .rnr_retry = rnr_retry_};
}

int QpAttribute::GetRcRtrToRtsMask() const {
  return IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC |
         IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY;
}

ibv_qp_attr QpAttribute::GetUdResetToInitAttr(uint8_t port,
                                              uint32_t qkey) const {
  return ibv_qp_attr{
      .qp_state = IBV_QPS_INIT,
      .qkey = qkey,
      .pkey_index = pkey_index_,
      .port_num = port,
  };
}

int QpAttribute::GetUdResetToInitMask() const {
  return IBV_QP_STATE | IBV_QP_QKEY | IBV_QP_PKEY_INDEX | IBV_QP_PORT;
}

ibv_qp_attr QpAttribute::GetUdInitToRtrAttr() const {
  return ibv_qp_attr{.qp_state = IBV_QPS_RTR};
}

int QpAttribute::GetUdInitToRtrMask() const { return IBV_QP_STATE; }

ibv_qp_attr QpAttribute::GetUdRtrToRtsAttr() const {
  return ibv_qp_attr{
      .qp_state = IBV_QPS_RTS,
      .sq_psn = sq_psn_,
  };
}

int QpAttribute::GetUdRtrToRtsMask() const {
  return IBV_QP_STATE | IBV_QP_SQ_PSN;
}

QpInitAttribute& QpInitAttribute::set_max_send_wr(uint32_t max_send_wr) {
  max_send_wr_ = max_send_wr;
  return *this;
}

QpInitAttribute& QpInitAttribute::set_max_recv_wr(uint32_t max_recv_wr) {
  max_recv_wr_ = max_recv_wr;
  return *this;
}

QpInitAttribute& QpInitAttribute::set_max_send_sge(uint32_t max_send_sge) {
  max_send_sge_ = max_send_sge;
  return *this;
}

QpInitAttribute& QpInitAttribute::set_max_recv_sge(uint32_t max_recv_sge) {
  max_recv_sge_ = max_recv_sge;
  return *this;
}

QpInitAttribute& QpInitAttribute::set_max_inline_data(
    uint32_t max_inline_data) {
  max_inline_data_ = max_inline_data;
  return *this;
}

QpInitAttribute& QpInitAttribute::set_sq_sig_all(int sq_sig_all) {
  sq_sig_all_ = sq_sig_all;
  return *this;
}

// Returns a default ibv_qp_init_attr;
ibv_qp_init_attr QpInitAttribute::GetAttribute(ibv_cq* send_cq, ibv_cq* recv_cq,
                                               ibv_qp_type qp_type,
                                               ibv_srq* srq) const {
  return ibv_qp_init_attr{
      .send_cq = send_cq,
      .recv_cq = recv_cq,
      .srq = srq,
      .cap{.max_send_wr = max_send_wr_,
           .max_recv_wr = max_recv_wr_,
           .max_send_sge = max_send_sge_,
           .max_recv_sge = max_recv_sge_,
           .max_inline_data = max_inline_data_},
      .qp_type = qp_type,
      .sq_sig_all = sq_sig_all_,
  };
}

}  // namespace rdma_unit_test
