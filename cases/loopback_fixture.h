#ifndef THIRD_PARTY_RDMA_UNIT_TEST_CASES_LOOPBACK_FIXTURE_H_
#define THIRD_PARTY_RDMA_UNIT_TEST_CASES_LOOPBACK_FIXTURE_H_

#include <cstdint>
#include <utility>

#include "absl/status/statusor.h"
#include "infiniband/verbs.h"
#include "cases/basic_fixture.h"

namespace rdma_unit_test {

class LoopbackFixture : public BasicFixture {
 protected:
  struct Client {
    ibv_context* context = nullptr;
    verbs_util::PortGid port_gid;
    ibv_pd* pd = nullptr;
    ibv_cq* cq = nullptr;
    ibv_qp* qp = nullptr;
    RdmaMemBlock buffer;
    ibv_mr* mr = nullptr;
  };

  // Create a client given a specific QP type, memory buffer size and content.
  absl::StatusOr<Client> CreateClient(ibv_qp_type qp_type = IBV_QPT_RC,
                                      int pages = 1);
};

}  // namespace rdma_unit_test

#endif  // THIRD_PARTY_RDMA_UNIT_TEST_CASES_LOOPBACK_FIXTURE_H_
