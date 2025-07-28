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

# Implementation of RDMA unit test framework.
# Targets in this packages is designed to be direct dependencies of targets in ../public/ but not ../cases/.

load("//third_party/bazel_rules/rules_cc/cc:cc_library.bzl", "cc_library")

package(
    default_applicable_licenses = ["//:package_license"],
    default_testonly = True,
    default_visibility = ["//visibility:public"],
    licenses = ["notice"],
)

cc_library(
    name = "introspection_irdma",
    hdrs = ["introspection_irdma.h"],
    deps = [
        ":introspection_registrar",
        ":system_libibverbs",
        "//public:introspection",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/strings",
    ],
    alwayslink = 1,
)

cc_library(
    name = "introspection_mlx4",
    hdrs = ["introspection_mlx4.h"],
    deps = [
        ":introspection_registrar",
        ":system_libibverbs",
        "//public:introspection",
        "@abseil-cpp//absl/container:flat_hash_map",
    ],
    alwayslink = 1,
)

cc_library(
    name = "introspection_mlx5",
    hdrs = ["introspection_mlx5.h"],
    deps = [
        ":introspection_registrar",
        ":system_libibverbs",
        "//public:introspection",
        "@abseil-cpp//absl/container:flat_hash_map",
    ],
    alwayslink = 1,
)

cc_library(
    name = "introspection_registrar",
    srcs = ["introspection_registrar.cc"],
    hdrs = ["introspection_registrar.h"],
    deps = [
        ":system_libibverbs",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/strings",
    ],
)

cc_library(
    name = "introspection_rxe",
    hdrs = ["introspection_rxe.h"],
    deps = [
        ":introspection_registrar",
        ":system_libibverbs",
        "//public:introspection",
        "@abseil-cpp//absl/container:flat_hash_map",
    ],
    alwayslink = 1,
)

cc_library(
    name = "verbs_extension",
    srcs = ["verbs_extension.cc"],
    hdrs = ["verbs_extension.h"],
    deps = [
        ":system_libibverbs",
        "//public:rdma_memblock",
        "//public:verbs_util",
        "@abseil-cpp//absl/status",
    ],
)

cc_library(
    name = "verbs_cleanup",
    srcs = ["verbs_cleanup.cc"],
    hdrs = ["verbs_cleanup.h"],
    deps = [
        ":system_libibverbs",
        "//public:rdma_memblock",
        "@abseil-cpp//absl/base:core_headers",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@abseil-cpp//absl/synchronization",
        "@googletest//:gtest",
    ],
)

cc_library(
    name = "handle_garble",
    srcs = ["handle_garble.cc"],
    hdrs = ["handle_garble.h"],
)

cc_library(
    name = "verbs_attribute",
    srcs = ["verbs_attribute.cc"],
    hdrs = ["verbs_attribute.h"],
    deps = [
        ":system_libibverbs",
        "@abseil-cpp//absl/flags:flag",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/time",
    ],
)

cc_library(
    name = "system_libibverbs",
    linkopts = ["-libverbs"],
)
