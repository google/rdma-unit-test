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

#include "public/introspection.h"

#include <cstdint>
#include <fstream>
#include <functional>
#include <optional>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include <magic_enum.hpp>
#include "infiniband/verbs.h"
#include "internal/introspection_registrar.h"
#include "public/flags.h"
#include "public/status_matchers.h"
#include "public/verbs_util.h"

namespace rdma_unit_test {

NicIntrospection::CounterSnapshot NicIntrospection::GetCounterSnapshot() const {
  CounterSnapshot snapshot;
  for (const auto& [type, name] : GetHardwareCounters()) {
    snapshot[type] = GetCounterValue(type).value();
  }
  return snapshot;
}

std::optional<std::string> NicIntrospection::KnownIssue() const {
  const testing::TestInfo* const test_info =
      testing::UnitTest::GetInstance()->current_test_info();
  // These two modifications strip off extra bits added for parameterized tests.
  std::string suite = std::vector<std::string>(
                          absl::StrSplit(test_info->test_suite_name(), '/'))
                          .back();
  std::string name =
      std::vector<std::string>(absl::StrSplit(test_info->name(), '/')).front();
  auto iter = GetDeviations().find(std::make_tuple(suite, name));
  if (iter == GetDeviations().end()) {
    return std::nullopt;
  }
  return iter->second;
}

bool NicIntrospection::HasCounter(HardwareCounter counter) const {
  return GetHardwareCounters().contains(counter);
}

absl::StatusOr<uint64_t> NicIntrospection::GetCounterValue(
    HardwareCounter counter) const {
  auto& counters = GetHardwareCounters();
  auto iter = counters.find(counter);
  if (iter == counters.end()) {
    return absl::NotFoundError(
        absl::StrCat("Cannot found counter ", magic_enum::enum_name(counter)));
  }
  std::string path =
      absl::StrCat("/sys/class/infiniband/", Introspection().device_name(),
                   "/hw_counters/", iter->second);
  std::ifstream file(path);
  if (!file.is_open()) {
    return absl::InternalError(absl::StrCat("Cannot open file ", path));
  }
  std::string line;
  if (!std::getline(file, line)) {
    return absl::InternalError(absl::StrCat("Cannot get line ", line));
  }
  uint64_t value;
  if (absl::SimpleAtoi(line, &value)) {
    return value;
  }
  return absl::InternalError(
      absl::StrCat("Cannot extract integer from line: ", line));
}

std::string NicIntrospection::DumpHardwareCounters() const {
  std::stringstream ss;
  for (const auto& [type, name] : GetHardwareCounters()) {
    ss << absl::StrCat(magic_enum::enum_name(type), "(", name,
                       "): ", GetCounterValue(type).value())
       << std::endl;
  }
  return ss.str();
}

const NicIntrospection& Introspection() {
  static NicIntrospection* introspection = []() {
    // Introspection happens before the test happens and is used to examine
    // whether the NIC type is supported/registered.
    // In our current use case, we only test one NIC type at one time, so only
    // the first device name is needed here.
    std::string device_names = absl::GetFlag(FLAGS_device_name);
    std::vector<std::string_view> devices = absl::StrSplit(device_names, ',');
    std::string dev_name = std::string(devices.at(0));
    absl::StatusOr<ibv_context*> context_or =
        rdma_unit_test::verbs_util::OpenUntrackedDevice(dev_name);
    CHECK_OK(context_or.status());  // Crash ok
    ibv_context* context = context_or.value();
    ibv_device_attr attr;
    int query_result = ibv_query_device(context, &attr);
    CHECK_EQ(0, query_result);  // Crash ok
    std::string device_name = context->device->name;
    // roce device name is overridden as: roce[<vendor_id>:<vendor_part_id>]
    // according to
    // https://github.com/linux-rdma/rdma-core/blob/master/kernel-boot/rdma-persistent-naming.rules
    if (absl::StartsWith(device_name, "roce")) {
      device_name =
          absl::StrFormat("roce[%x:%x]", attr.vendor_id, attr.vendor_part_id);
    }
    CHECK_EQ(0, ibv_close_device(context));  // Crash ok

    IntrospectionRegistrar::Factory factory =
        IntrospectionRegistrar::GetInstance().GetFactory(device_name);
    if (!factory) {
      LOG(FATAL) << "Unknown NIC type:" << device_name;  // Crash ok
    }
    NicIntrospection* device_info = factory(device_name, attr);

    // Verify that the no ipv6 flag matches the device's capabilities
    if (!device_info->SupportsIpV6() && !absl::GetFlag(FLAGS_ipv4_only)) {
      LOG(FATAL) << device_name  // Crash ok
                 << " does not support ipv6.  Use --ipv4_only";
    }
    return device_info;
  }();
  return *introspection;
}

}  // namespace rdma_unit_test
