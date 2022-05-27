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

#include "unit/batch_op_fixture.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <thread>  // NOLINT

#include "glog/logging.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/barrier.h"
#include "infiniband/verbs.h"
#include "internal/verbs_attribute.h"
#include "public/status_matchers.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

absl::StatusOr<BatchOpFixture::BasicSetup> BatchOpFixture::CreateBasicSetup() {
  BasicSetup setup;
  setup.src_memblock = ibv_.AllocAlignedBufferByBytes(kBufferSize);
  std::fill_n(setup.src_memblock.data(), setup.src_memblock.size(),
              kSrcContent);
  setup.dst_memblock = ibv_.AllocAlignedBufferByBytes(kBufferSize);
  std::fill_n(setup.dst_memblock.data(), setup.dst_memblock.size(),
              kDstContent);
  ASSIGN_OR_RETURN(setup.context, ibv_.OpenDevice());
  setup.port_attr = ibv_.GetPortAttribute(setup.context);
  setup.pd = ibv_.AllocPd(setup.context);
  if (!setup.pd) {
    return absl::InternalError("Failed to allocate pd.");
  }

  setup.src_mr = ibv_.RegMr(setup.pd, setup.src_memblock);
  if (!setup.src_mr) {
    return absl::InternalError("Failed to register source mr.");
  }
  memset(setup.dst_memblock.data(), 0, setup.dst_memblock.size());
  setup.dst_mr = ibv_.RegMr(setup.pd, setup.dst_memblock);
  if (!setup.dst_mr) {
    return absl::InternalError("Failed to register destination mr.");
  }
  return setup;
}

int BatchOpFixture::QueueSend(BasicSetup& setup, QpPair& qp) {
  return QueueWork(setup, qp, WorkType::kSend);
}

int BatchOpFixture::QueueWrite(BasicSetup& setup, QpPair& qp) {
  return QueueWork(setup, qp, WorkType::kWrite);
}

int BatchOpFixture::QueueRecv(BasicSetup& setup, QpPair& qp) {
  uint32_t wr_id = qp.next_recv_wr_id++;
  DCHECK_LT(wr_id, setup.dst_memblock.size());
  auto dst_buffer =
      qp.dst_buffer.subspan(wr_id, 1);  // use wr_id as index to the buffer.
  ibv_sge sge = verbs_util::CreateSge(dst_buffer, setup.dst_mr);
  ibv_recv_wr wqe = verbs_util::CreateRecvWr(wr_id, &sge, /*num_sge=*/1);
  ibv_recv_wr* bad_wr;
  return ibv_post_recv(qp.recv_qp, &wqe, &bad_wr);
}

int BatchOpFixture::QueueWork(BasicSetup& setup, QpPair& qp,
                              WorkType work_type) {
  uint64_t wr_id = qp.next_send_wr_id++;
  DCHECK_LT(wr_id, setup.src_memblock.size());
  auto src_buffer = setup.src_memblock.subspan(
      wr_id, 1);  // use wr_id as index to the buffer.
  ibv_sge sge = verbs_util::CreateSge(src_buffer, setup.src_mr);
  ibv_send_wr wqe;
  switch (work_type) {
    case WorkType::kSend: {
      wqe = verbs_util::CreateSendWr(wr_id, &sge, /*num_sge=*/1);
      break;
    }
    case WorkType::kWrite: {
      DCHECK_LT(wr_id, setup.dst_memblock.size());
      auto dst_buffer =
          qp.dst_buffer.subspan(wr_id, 1);  // use wr_id as index to the buffer.
      wqe = verbs_util::CreateWriteWr(wr_id, &sge, /*num_sge=*/1,
                                      dst_buffer.data(), setup.dst_mr->rkey);
      break;
    }
  }
  ibv_send_wr* bad_wr;
  return ibv_post_send(qp.send_qp, &wqe, &bad_wr);
}

absl::StatusOr<std::vector<BatchOpFixture::QpPair>>
BatchOpFixture::CreateTestQpPairs(BasicSetup& setup, ibv_cq* send_cq,
                                  ibv_cq* recv_cq, size_t max_qp_wr,
                                  int count) {
  if (max_qp_wr * count > setup.dst_memblock.size()) {
    return absl::InternalError(
        "Not enough space on destination buffer for all QPs.");
  }
  std::vector<QpPair> qp_pairs;
  for (int i = 0; i < count; ++i) {
    QpPair qp_pair;
    qp_pair.send_qp = ibv_.CreateQp(
        setup.pd, send_cq, recv_cq, IBV_QPT_RC,
        QpInitAttribute().set_max_send_wr(max_qp_wr).set_max_recv_wr(
            max_qp_wr));
    if (!qp_pair.send_qp) {
      return absl::InternalError("Failed to create send qp.");
    }
    qp_pair.recv_qp = ibv_.CreateQp(
        setup.pd, send_cq, recv_cq, IBV_QPT_RC,
        QpInitAttribute().set_max_send_wr(max_qp_wr).set_max_recv_wr(
            max_qp_wr));
    if (!qp_pair.recv_qp) {
      return absl::InternalError("Failed to create recv qp.");
    }
    RETURN_IF_ERROR(ibv_.SetUpLoopbackRcQps(qp_pair.send_qp, qp_pair.recv_qp,
                                            setup.port_attr));
    qp_pair.dst_buffer = setup.dst_memblock.subspan(i * max_qp_wr, max_qp_wr);
    qp_pairs.push_back(qp_pair);
  }
  return qp_pairs;
}

void BatchOpFixture::ThreadedSubmission(std::vector<QpPair> qp_pairs,
                                        int times_per_pair,
                                        std::function<void(QpPair&)> work) {
  std::vector<std::thread> threads;
  absl::Barrier wait_barrier(qp_pairs.size());
  threads.reserve(qp_pairs.size());
  for (auto& qp_pair : qp_pairs) {
    threads.push_back(
        std::thread([&qp_pair, times_per_pair, &wait_barrier, work]() {
          wait_barrier.Block();
          for (int i = 0; i < times_per_pair; ++i) {
            work(qp_pair);
          }
        }));
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

}  // namespace rdma_unit_test
