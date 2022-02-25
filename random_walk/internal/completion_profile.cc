#include "random_walk/internal/completion_profile.h"

#include <cstdint>
#include <ostream>
#include <sstream>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "infiniband/verbs.h"
#include "public/map_util.h"
#include "random_walk/internal/types.h"

namespace rdma_unit_test {
namespace random_walk {

void CompletionProfile::RegisterAction(uint64_t wr_id, Action action) {
  actions_[wr_id] = action;
}

void CompletionProfile::RegisterCompletion(const ibv_wc& completion) {
  auto iter = actions_.find(completion.wr_id);
  DCHECK(iter != actions_.end());
  profiles_[iter->second][completion.status]++;
  actions_.erase(iter);
}

std::string CompletionProfile::DumpStats() const {
  std::stringstream sstream;
  sstream << "Dumping completion profile:" << std::endl;
  for (const auto& [action, profile] : profiles_) {
    sstream << "Action : " << ActionToString(action) << std::endl;
    for (const auto& [status, count] : profile) {
      if (count > 0) {
        sstream << "  " << ibv_wc_status_str(status) << " : " << count
                << std::endl;
      }
    }
  }

  return sstream.str();
}

}  // namespace random_walk
}  // namespace rdma_unit_test
