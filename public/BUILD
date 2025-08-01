# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Public Interface for rdma unit tests:
# Note: These are the target directly referenced by test fixtures and test cases.

package(
    default_applicable_licenses = ["//:package_license"],
    default_testonly = True,
    default_visibility = ["//visibility:public"],
    licenses = ["notice"],
)

cc_library(
    name = "flags",
    srcs = ["flags.cc"],
    hdrs = ["flags.h"],
    deps = [
        "@abseil-cpp//absl/flags:flag",
        "@abseil-cpp//absl/strings",
    ],
)

cc_library(
    name = "introspection",
    srcs = ["introspection.cc"],
    hdrs = [
        "introspection.h",
    ],
    deps = [
        ":flags",
        ":status_matchers",
        ":system_libibverbs",
        ":verbs_util",
        "//internal:introspection_registrar",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/flags:flag",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/log:check",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/strings:str_format",
        "@googletest//:gtest",
        "@magic_enum",
    ],
)

cc_library(
    name = "rdma_memblock",
    srcs = ["rdma_memblock.cc"],
    hdrs = ["rdma_memblock.h"],
    deps = [
        ":page_size",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/log:check",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/strings:str_format",
        "@abseil-cpp//absl/types:span",
    ],
)

cc_library(
    name = "map_util",
    hdrs = ["map_util.h"],
    deps = [
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@abseil-cpp//absl/log:check",
    ],
)

cc_library(
    name = "verbs_util",
    srcs = ["verbs_util.cc"],
    hdrs = ["verbs_util.h"],
    deps = [
        ":flags",
        ":status_matchers",
        ":system_libibverbs",
        "@abseil-cpp//absl/cleanup",
        "@abseil-cpp//absl/flags:flag",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/log:check",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/strings:str_format",
        "@abseil-cpp//absl/time",
        "@abseil-cpp//absl/types:optional",
        "@abseil-cpp//absl/types:span",
        "@googletest//:gtest",
        "@magic_enum",
    ],
)

cc_library(
    name = "basic_fixture",
    srcs = ["basic_fixture.cc"],
    hdrs = ["basic_fixture.h"],
    deps = [
        ":introspection",
        "@abseil-cpp//absl/log:check",
        "@googletest//:gtest",
    ],
)

cc_library(
    name = "verbs_helper_suite",
    srcs = ["verbs_helper_suite.cc"],
    hdrs = ["verbs_helper_suite.h"],
    deps = [
        ":flags",
        ":page_size",
        ":rdma_memblock",
        ":status_matchers",
        ":system_libibverbs",
        ":verbs_util",
        "//internal:verbs_attribute",
        "//internal:verbs_cleanup",
        "//internal:verbs_extension",
        "@abseil-cpp//absl/base:core_headers",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/flags:flag",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/log:check",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/strings:str_format",
        "@abseil-cpp//absl/synchronization",
        "@magic_enum",
    ],
)

cc_library(
    name = "status_matchers",
    testonly = 1,
    srcs = ["status_matchers.cc"],
    hdrs = ["status_matchers.h"],
    deps = [
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/strings",
        "@googletest//:gtest",
    ],
)

cc_library(
    name = "page_size",
    hdrs = ["page_size.h"],
)

cc_library(
    name = "system_libibverbs",
    linkopts = ["-libverbs"],
)
