#ifndef THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_COMPLETION_PROFILE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_COMPLETION_PROFILE_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "random_walk/internal/types.h"

namespace rdma_unit_test {
namespace random_walk {

// A class for profiling RDMA op completions. It offers methods for storing
// and summarizing RDMA ops and its completion status.
class CompletionProfile {
 public:
  CompletionProfile() = default;
  CompletionProfile(const CompletionProfile& other) = delete;
  CompletionProfile& operator=(const CompletionProfile& other) = delete;
  CompletionProfile(CompletionProfile&& other) = default;
  CompletionProfile& operator=(CompletionProfile&& other) = default;
  ~CompletionProfile() = default;

  // Registers a wr_id and its corresponding [Action]. This step is required
  // because for ibverbs completions (ibv_wc), its opcode is not guaranteed
  // to be valid unless the completion status is IBV_WC_SUCCESS.
  void RegisterAction(uint64_t wr_id, Action action);

  // Registers a completion entry.
  void RegisterCompletion(const ibv_wc& completion);

  // Return a string representing the completion profile of all completions
  // registered so far.
  std::string DumpStats() const;

 private:
  // An action profile represents the count of each completion status.
  using ActionProfile = absl::flat_hash_map<ibv_wc_status, size_t>;

  // Stores the action profile (see type ActionProfile) for each action.
  absl::flat_hash_map<Action, ActionProfile> profiles_;

  // Stores the corresponding action of each wr_id.
  absl::flat_hash_map<uint64_t, Action> actions_;
};

}  // namespace random_walk
}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_RANDOM_WALK_INTERNAL_COMPLETION_PROFILE_H_
