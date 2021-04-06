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

#include <functional>
#include <string>

#include "glog/logging.h"
#include "absl/cleanup/cleanup.h"
#include "absl/flags/flag.h"
#include "absl/status/statusor.h"
#include "infiniband/verbs.h"
#include "impl/introspection_registrar.h"
#include "public/flags.h"

namespace rdma_unit_test {

const NicIntrospection& Introspection() {
  static NicIntrospection* introspection = []() {
    int num_devices = 0;
    ibv_device** devices = ibv_get_device_list(&num_devices);
    auto free_list = absl::MakeCleanup([devices]() {
      if (devices) {
        ibv_free_device_list(devices);
      }
    });
    CHECK(devices);            // Crash ok
    CHECK_LT(0, num_devices);  // Crash ok
    // Use the first device.
    ibv_device* device = devices[0];
    CHECK(device);  // Crash ok
    ibv_context* context = ibv_open_device(device);
    CHECK(context);  // Crash ok
    ibv_device_attr attr;
    int query_result = ibv_query_device(context, &attr);
    CHECK_EQ(0, query_result);  // Crash ok
    std::string device_name = context->device->name;
    CHECK_EQ(0, ibv_close_device(context));  // Crash ok

    LOG(INFO) << "Device name = " << device_name;
    IntrospectionRegistrar::Factory factory =
        IntrospectionRegistrar::GetInstance().GetFactory(device_name);
    if (!factory) {
      LOG(FATAL) << "Unknown NIC type:" << device_name;  // Crash ok
    }
    NicIntrospection* device_info = factory(attr);

    // Verify that the no ipv6 flag matches the device's capabilities
    if (!device_info->SupportsIpV6() && !absl::GetFlag(FLAGS_no_ipv6_for_gid)) {
      LOG(FATAL) << device_name  // Crash ok
                 << " does not support ipv6.  Use --no_ipv6_for_gid";
    }
    return device_info;
  }();
  return *introspection;
}

}  // namespace rdma_unit_test
