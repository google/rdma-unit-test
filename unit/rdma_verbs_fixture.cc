#include "unit/rdma_verbs_fixture.h"

#include "absl/log/log.h"
#include "public/basic_fixture.h"
#include "public/introspection.h"

namespace rdma_unit_test {

void RdmaVerbsFixture::SetUp() {
  BasicFixture::SetUp();

  LOG(INFO) << "Pre test stats dump.";
  LOG(INFO) << Introspection().DumpHardwareCounters();
}

void RdmaVerbsFixture::TearDown() {
  LOG(INFO) << "Post test stats dump.";
  LOG(INFO) << Introspection().DumpHardwareCounters();

  BasicFixture::TearDown();
}

}  // namespace rdma_unit_test
