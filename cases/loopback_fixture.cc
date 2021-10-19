#include "cases/loopback_fixture.h"

#include <algorithm>
#include <cstdint>
#include <utility>

#include "absl/status/statusor.h"
#include "infiniband/verbs.h"
#include "public/status_matchers.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

absl::StatusOr<LoopbackFixture::Client> LoopbackFixture::CreateClient(
    ibv_qp_type qp_type, int pages) {
  Client client;
  client.buffer = ibv_.AllocBuffer(pages);
  std::fill_n(client.buffer.data(), client.buffer.size(), '-');
  ASSIGN_OR_RETURN(client.context, ibv_.OpenDevice());
  client.port_gid = ibv_.GetLocalPortGid(client.context);
  client.pd = ibv_.AllocPd(client.context);
  if (!client.pd) {
    return absl::InternalError("Failed to allocate pd.");
  }
  client.cq = ibv_.CreateCq(client.context);
  if (!client.cq) {
    return absl::InternalError("Failed to create cq.");
  }
  ibv_qp_init_attr attr = {.send_cq = client.cq,
                           .recv_cq = client.cq,
                           .srq = nullptr,
                           .cap = verbs_util::DefaultQpCap(),
                           .qp_type = qp_type,
                           .sq_sig_all = 0};
  client.qp = ibv_.CreateQp(client.pd, attr);
  if (!client.qp) {
    return absl::InternalError("Failed to create qp.");
  }
  // memory setup.
  client.mr = ibv_.RegMr(client.pd, client.buffer);
  if (!client.mr) {
    return absl::InternalError("Failed to register mr.");
  }
  return client;
}

}  // namespace rdma_unit_test
