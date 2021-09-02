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

#include "random_walk/internal/sampling.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "absl/random/discrete_distribution.h"
#include "absl/random/distributions.h"
#include "absl/types/span.h"
#include "public/rdma_memblock.h"
#include "public/verbs_util.h"
#include "random_walk/internal/random_walk_config.pb.h"
#include "random_walk/internal/types.h"

namespace rdma_unit_test {
namespace random_walk {

RdmaMemBlock RandomWalkSampler::RandomMrRdmaMemblock(
    const RdmaMemBlock& memblock) const {
  DCHECK_LE(kMinMrSize, memblock.size());
  uint64_t size =
      absl::Uniform(bitgen_, kMinMrSize, std::min(kMaxMrSize, memblock.size()));
  uint64_t offset = absl::Uniform(bitgen_, 0ul, memblock.size() - size);
  return memblock.subblock(offset, size);
}

absl::Span<uint8_t> RandomWalkSampler::RandomMwSpan(ibv_mr* mr) const {
  DCHECK_GE(mr->length, kMinMwSize);
  uint64_t size = absl::Uniform(bitgen_, kMinMwSize, mr->length);
  uint64_t offset = absl::Uniform(bitgen_, 0ul, mr->length - size);
  uint8_t* addr = reinterpret_cast<uint8_t*>(mr->addr) + offset;
  return absl::MakeSpan(addr, size);
}

std::pair<std::vector<absl::Span<uint8_t>>, uint8_t*>
RandomWalkSampler::RandomRdmaBuffersPair(ibv_mr* local_mr,
                                         uint64_t remote_memory_addr,
                                         uint64_t remote_memory_length,
                                         size_t max_buffers) const {
  uint64_t length = kOpSizes[absl::Uniform(bitgen_, 0ul, kOpSizes.size())];
  std::vector<absl::Span<uint8_t>> buffers =
      CreateLocalBuffers(local_mr, length, max_buffers);
  uint64_t remote_offset =
      absl::Uniform(bitgen_, 0ul, remote_memory_length - length);
  uint8_t* remote_addr =
      reinterpret_cast<uint8_t*>(remote_memory_addr) + remote_offset;
  return std::make_pair(buffers, remote_addr);
}

uint8_t* RandomWalkSampler::RandomAtomicAddr(void* addr,
                                             uint64_t length) const {
  uint64_t start_addr = reinterpret_cast<uint64_t>(addr);
  uint64_t valid_length = length;
  if (start_addr % 8) {
    start_addr += 8 - start_addr % 8;
    valid_length -= 8 - start_addr % 8;
  }
  DCHECK_GE(valid_length, 8ul);
  valid_length -= valid_length % 8;
  uint64_t num_blocks = valid_length / 8;
  uint64_t random_block = absl::Uniform(bitgen_, 0ul, num_blocks);
  return reinterpret_cast<uint8_t*>(start_addr + random_block * 8);
}

std::vector<absl::Span<uint8_t>> RandomWalkSampler::RandomRcSendRecvSpans(
    ibv_mr* mr, size_t max_buffers) {
  DCHECK_GE(mr->length, kRcSendRecvBufferSize);
  return CreateLocalBuffers(mr, kRcSendRecvBufferSize, max_buffers);
}

std::vector<absl::Span<uint8_t>> RandomWalkSampler::RandomUdSendSpans(
    ibv_mr* mr, size_t max_buffers) {
  DCHECK_GE(mr->length, kUdSendRecvPayloadSize);
  return CreateLocalBuffers(mr, kUdSendRecvPayloadSize, max_buffers);
}

std::vector<absl::Span<uint8_t>> RandomWalkSampler::RandomUdRecvSpans(
    ibv_mr* mr, size_t max_buffers) {
  DCHECK_GE(mr->length, kUdSendRecvPayloadSize + sizeof(ibv_grh));
  return CreateLocalBuffers(mr, kUdSendRecvPayloadSize + sizeof(ibv_grh),
                            max_buffers);
}

std::vector<absl::Span<uint8_t>> RandomWalkSampler::CreateLocalBuffers(
    ibv_mr* mr, size_t total_length, size_t max_buffers) const {
  DCHECK_LE(total_length, mr->length);
  size_t num_buffers = 1;
  if (max_buffers > 1 && absl::Bernoulli(bitgen_, 0.5)) {
    num_buffers = absl::Uniform<size_t>(bitgen_, 2ul, max_buffers + 1);
  }
  std::vector<size_t> buffer_boundaries;
  buffer_boundaries.reserve(num_buffers + 1);
  buffer_boundaries.push_back(0);
  buffer_boundaries.push_back(total_length);
  for (size_t i = 0; i < num_buffers - 1; ++i) {
    buffer_boundaries.push_back(absl::Uniform(bitgen_, 0ul, total_length));
  }
  std::sort(buffer_boundaries.begin(), buffer_boundaries.end());
  std::vector<absl::Span<uint8_t>> buffers;
  buffers.reserve(num_buffers);
  for (size_t i = 0; i < buffer_boundaries.size() - 1; ++i) {
    size_t buffer_length = buffer_boundaries[i + 1] - buffer_boundaries[i];
    size_t buffer_offset =
        absl::Uniform(bitgen_, 0ul, mr->length - buffer_length);
    absl::Span<uint8_t> new_buffer = absl::MakeSpan(
        static_cast<uint8_t*>(mr->addr) + buffer_offset, buffer_length);
    buffers.push_back(new_buffer);
  }
  return buffers;
}

ActionSampler::ActionSampler(const ActionWeights& weights)
    : action_weights_(weights) {}

Action ActionSampler::RandomAction() const {
  std::vector<double> weight_vector;
  std::vector<Action> label_vector;

  for (auto action : kActions) {
    weight_vector.push_back(GetActionWeight(action));
    label_vector.push_back(action);
  }

  absl::discrete_distribution<int> dist(weight_vector.begin(),
                                        weight_vector.end());
  int result = dist(bitgen_);

  return label_vector[result];
}

double ActionSampler::GetActionWeight(Action action) const {
  switch (action) {
    case Action::CREATE_CQ: {
      return action_weights_.create_cq();
    }
    case Action::DESTROY_CQ: {
      return action_weights_.destroy_cq();
    }
    case Action::ALLOC_PD: {
      return action_weights_.allocate_pd();
    }
    case Action::DEALLOC_PD: {
      return action_weights_.deallocate_pd();
    }
    case Action::REG_MR: {
      return action_weights_.register_mr();
    }
    case Action::DEREG_MR: {
      return action_weights_.deregister_mr();
    }
    case Action::ALLOC_TYPE_1_MW: {
      return action_weights_.allocate_type_1_mw();
    }
    case Action::ALLOC_TYPE_2_MW: {
      return action_weights_.allocate_type_2_mw();
    }
    case Action::DEALLOC_TYPE_1_MW: {
      return action_weights_.deallocate_type_1_mw();
    }
    case Action::DEALLOC_TYPE_2_MW: {
      return action_weights_.deallocate_type_2_mw();
    }
    case Action::BIND_TYPE_1_MW: {
      return action_weights_.bind_type_1_mw();
    }
    case Action::BIND_TYPE_2_MW: {
      return action_weights_.bind_type_2_mw();
    }
    case Action::CREATE_RC_QP_PAIR: {
      return action_weights_.create_rc_qp_pair();
    }
    case Action::CREATE_UD_QP: {
      return action_weights_.create_ud_qp();
    }
    case Action::MODIFY_QP_ERROR: {
      return action_weights_.modify_qp_error();
    }
    case Action::DESTROY_QP: {
      return action_weights_.destroy_qp();
    }
    case Action::CREATE_AH: {
      return action_weights_.create_ah();
    }
    case Action::DESTROY_AH: {
      return action_weights_.destroy_ah();
    }
    case Action::SEND: {
      return action_weights_.send();
    }
    case Action::SEND_WITH_INV: {
      return action_weights_.send_with_inv();
    }
    case Action::RECV: {
      return action_weights_.recv();
    }
    case Action::READ: {
      return action_weights_.read();
    }
    case Action::WRITE: {
      return action_weights_.write();
    }
    case Action::FETCH_ADD: {
      return action_weights_.fetch_add();
    }
    case Action::COMP_SWAP: {
      return action_weights_.comp_swap();
    }
    default: {
      LOG(FATAL) << "Unknown action";  // Crash ok
    }
  }
}

}  // namespace random_walk
}  // namespace rdma_unit_test
