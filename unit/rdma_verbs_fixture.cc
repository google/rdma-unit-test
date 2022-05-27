#include "unit/rdma_verbs_fixture.h"

#include "glog/logging.h"
#include "public/basic_fixture.h"
#include "public/introspection.h"

namespace rdma_unit_test {

void RdmaVerbsFixture::SetUp() {
  BasicFixture::SetUp();

  VLOG(1) << "Pre test stats dump.";
  VLOG(1) << Introspection().DumpHardwareCounters();
}

void RdmaVerbsFixture::TearDown() {
  VLOG(1) << "Post test stats dump.";
  VLOG(1) << Introspection().DumpHardwareCounters();

  BasicFixture::TearDown();
}

}  // namespace rdma_unit_test
