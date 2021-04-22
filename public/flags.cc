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

#include <stdint.h>

#include <string>

#include "absl/flags/flag.h"
#include "absl/strings/string_view.h"


ABSL_FLAG(uint64_t, verbs_mtu, 4096,
          "The MTU value used in modify_qp. Valid values: 256, 512, 1024, "
          "2048, 4096[default]");
ABSL_FLAG(bool, no_ipv6_for_gid, false,
          "Force use of IPv4. IPv6 ports will be ignored.");
ABSL_FLAG(std::string, device_name, "",
          "RDMA device name as returned by ibv_devices(). If --device_name is "
          "empty, chooses the device at index zero as returned by "
          "ibv_get_device_list().");
