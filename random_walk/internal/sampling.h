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

#ifndef THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_SAMPLING_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_SAMPLING_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <list>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/internal/hash_function_defaults.h"
#include "absl/random/random.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "infiniband/verbs.h"
#include "public/page_size.h"
#include "public/rdma_memblock.h"
#include "public/verbs_util.h"
#include "random_walk/internal/random_walk_config.pb.h"
#include "random_walk/internal/types.h"

namespace rdma_unit_test {
namespace random_walk {

// This file contains a set of sampler classes that help a RandomWalkClient
// to generate random commands.

// The class provides helper functions for sampling random command parameters.
class RandomWalkSampler {
 public:
  // The size of the ground memory, i.e. the memory from which MR are allcoated
  // and register, in pages and bytes.
  static constexpr uint64_t kGroundMemoryPages = 4 * 1024;
  static constexpr uint64_t kGroundMemorySize =
      kGroundMemoryPages * kPageSize;  // 16MB
  static constexpr uint64_t kRcSendRecvBufferSize = 16 * 1024;
  static constexpr uint64_t kUdSendRecvPayloadSize = 1000;  // sub-MTU for UD.
  // Mr sizes is uniformly random in [kMinMrSize, kMaxMrSize].
  static constexpr uint64_t kMinMrSize = 1024 * 1024;
  static constexpr uint64_t kMaxMrSize = kGroundMemorySize;
  // Mw sizes is uniformly random in [kMinMwSize, Size of MR it is in].
  static constexpr uint64_t kMinMwSize = 1024 * 1024;
  // RDMA op size is chosen randomly from the elements in the list kOpSizes.
  static constexpr std::array<uint64_t, 4> kOpSizes = {32 * 1024, 64 * 1024,
                                                       128 * 1024, 1024 * 1024};

  RandomWalkSampler() = default;
  // Copyable.
  RandomWalkSampler(const RandomWalkSampler& sampler) = default;
  RandomWalkSampler& operator=(const RandomWalkSampler& sampler) = default;
  ~RandomWalkSampler() = default;

  // Returns a uniformly random element from an absl::flat_hash_set.
  template <class Type,
            class Hash = absl::container_internal::hash_default_hash<Type>,
            class Eq = absl::container_internal::hash_default_eq<Type>>
  absl::optional<Type> GetRandomSetElement(
      const absl::flat_hash_set<Type, Hash, Eq>& set) const {
    if (set.empty()) {
      return absl::nullopt;
    }
    size_t rand_idx = absl::Uniform(bitgen_, 0u, set.size());
    auto iter = set.cbegin();
    std::advance(iter, rand_idx);
    return *iter;
  }

  // Returns a uniformly random key from an absl::flat_hash_map.
  template <typename Key, typename Value>
  absl::optional<Key> GetRandomMapKey(
      const absl::flat_hash_map<Key, Value>& map) const {
    if (map.empty()) {
      return absl::nullopt;
    }
    size_t rand_idx = absl::Uniform(bitgen_, 0u, map.size());
    auto iter = map.cbegin();
    std::advance(iter, rand_idx);
    return iter->first;
  }

  // Returns a uniformly random value from an absl::flat_hash_map.
  template <typename Key, typename Value>
  absl::optional<Value> GetRandomMapValue(
      const absl::flat_hash_map<Key, Value>& map) const {
    if (map.empty()) {
      return absl::nullopt;
    }
    size_t rand_idx = absl::Uniform(bitgen_, 0u, map.size());
    auto iter = map.cbegin();
    std::advance(iter, rand_idx);
    return iter->second;
  }

  // Returns a uniformly random key value pair from an absl::flat_hash_map.
  template <typename Key, typename Value>
  absl::optional<std::pair<Key, Value>> GetRandomMapKeyValuePair(
      const absl::flat_hash_map<Key, Value>& map) const {
    if (map.empty()) {
      return absl::nullopt;
    }
    size_t rand_idx = absl::Uniform(bitgen_, 0u, map.size());
    auto iter = map.cbegin();
    std::advance(iter, rand_idx);
    return *iter;
  }

  // Returns a uniformly random element from a list.
  template <typename Type>
  absl::optional<Type> GetRandomListElement(const std::list<Type>& lst) const {
    if (lst.empty()) {
      return absl::nullopt;
    }
    size_t rand_idx = absl::Uniform(bitgen_, 0u, lst.size());
    auto iter = lst.begin();
    std::advance(iter, rand_idx);
    return *iter;
  }

  // Returns a random subblock of a RdmaMemBlock.
  RdmaMemBlock RandomMrRdmaMemblock(const RdmaMemBlock& memblock) const;

  // Returns a randomly generated absl::Span inside the range of a Mr.
  absl::Span<uint8_t> RandomMwSpan(ibv_mr* mr) const;

  // Returns a pair consisting of a vector of local buffers and a remote pointer
  // for RDMA.
  std::pair<std::vector<absl::Span<uint8_t>>, uint8_t*> RandomRdmaBuffersPair(
      ibv_mr* local_mr, uint64_t remote_memory_addr,
      uint64_t remote_memory_length, size_t max_buffers) const;

  // Returns a random 8-byte aligned address in the range [addr, addr + 8).
  uint8_t* RandomAtomicAddr(void* addr, uint64_t length) const;

  // Returns a vector of absl::Span's generated randomly in an Mr for RC
  // send/recv.
  std::vector<absl::Span<uint8_t>> RandomRcSendRecvSpans(ibv_mr* mr,
                                                         size_t max_buffers);

  // Returns a vector of absl::Span's generated randomly in an Mr for UD send.
  std::vector<absl::Span<uint8_t>> RandomUdSendSpans(ibv_mr* mr,
                                                     size_t max_buffers);

  // Returns a vector of absl::Span's generated randomly in an Mr for UD recv.
  // Recv buffer is sizeof(ibv_grh) bytes bigger than the send buffer.
  std::vector<absl::Span<uint8_t>> RandomUdRecvSpans(ibv_mr* mr,
                                                     size_t max_buffers);

 private:
  // Create a random number of local buffers who are:
  // 1. inside the [mr] provided.
  // 2. whose lengths sum up to [total_length]
  // 3. The total number of buffers is 1 with probability 0.5, and with
  //    probability 0.5, the number is uniform in [2, max_sge_of_qp]
  std::vector<absl::Span<uint8_t>> CreateLocalBuffers(ibv_mr* mr,
                                                      size_t total_length,
                                                      size_t max_buffers) const;

  mutable absl::BitGen bitgen_;
};

// ActionSampler provides functions that samples a random action, i.e. command
// types (see RandomWalkClient::DoRandomAction) according to some distribution.
// The distribution of random actions can be initialized in the constructor.
class ActionSampler {
 public:
  explicit ActionSampler(const ActionWeights& action_weights = {});
  // Copyable.
  ActionSampler(const ActionSampler& sampler) = default;
  ActionSampler& operator=(const ActionSampler& sampler) = default;
  ~ActionSampler() = default;

  // Returns a random action.
  Action RandomAction() const;

 private:
  double GetActionWeight(Action action) const;

  mutable absl::BitGen bitgen_;
  const ActionWeights action_weights_;
};

// StreamSampler performs Reservoir Sampling in a stream of objects to produce
// uniformly random sample in the stream. Its usage entails:
// (1) Create a StreamSampler object.
// (2) As a new element from the stream comes by, call
//     UpdateSample(new element).
// (3) Call ExtractSample to retrieve a uniformly random objects seen so far.
// Note: Reservoir Sampling is a sampling algorithm that samples from a stream
// of objects with constant memory. It keeps a sample, initially empty. As the
// i-th (starting with 1) stream element comes by, the new element replaces the
// current sample with probability 1/i. The algorithm returns the sample when
// the stream ends. A simple calculation can show that if the stream size is n,
// the i-th element is sample with probability
// (1/i) * (1 - 1/(i+1)) * ... (1 - 1/n) = 1/n.
template <typename ElemT>
class StreamSampler {
 public:
  StreamSampler() = default;
  // Copyable.
  StreamSampler(const StreamSampler& sampler) = default;
  StreamSampler& operator=(const StreamSampler& sampler) = default;
  ~StreamSampler() = default;

  // Updates the final sample with a new stream element probabilistically.
  // More specifically, as in Reservoir Sampling, it will replace the current
  // sample with the new element with probability 1/[elements seen so far].
  void UpdateSample(const ElemT& element) {
    ++element_seen;
    if (absl::Uniform(bitgen_, 0ul, element_seen) == 0) {
      sample = element;
    }
  }

  // Extracts the sample. The sample is guaranteed to be uniformly random
  // among the elements seen so far.
  absl::optional<ElemT> ExtractSample() const { return sample; }

 private:
  size_t element_seen = 0;
  absl::optional<ElemT> sample = absl::nullopt;
  mutable absl::BitGen bitgen_;
};

}  // namespace random_walk
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_SAMPLING_H_
