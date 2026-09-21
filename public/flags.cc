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

#include "public/flags.h"

#include <cstdint>
#include <string>

#include "absl/flags/flag.h"
#include "absl/strings/string_view.h"

ABSL_FLAG(bool, ipv4_only, false,
          "Force use of IPv4. IPv6 GIDs will be ignored.");
ABSL_FLAG(std::string, device_name, "",
          "One or more RDMA device names as returned by ibv_devices(), "
          "split by comma. If --device_name is empty, chooses the device at "
          "index zero as returned by ibv_get_device_list().");
ABSL_FLAG(uint64_t, completion_wait_multiplier, 1,
          "The multiplier applies to completion timeouts. Setting this flag "
          "higher than 1 will increase all timeout thresholds proportionally. "
          "Increase if tests are timing out due to slow RDMA devices.");
ABSL_FLAG(uint64_t, other_wait_multiplier, 1,
          "The multiplier applies to other timeouts (waits for polling and "
          "asynchoronous events). Setting this flag higher than 1 will "
          "increase all timeout thresholds proportionally. Increase if tests "
          "are timing out due to slow RDMA devices.  Useful in emulation.");
ABSL_FLAG(uint32_t, port_num, 0,
          "The port number used for connection establishment. Default: 0 "
          "(first available port)");
ABSL_FLAG(int, gid_index, -1,
          "The GID index used for connection establishment. Ignore ipv4_only "
          "when assigned. Default: -1 (use the first available IPv6 GID, if "
          "none available, use the first available IPv4 GID)");
ABSL_FLAG(bool, skip_default_gid, false,
          "If true, skip the Default GIDs (link-local IPv6 in RoCEv2).");
