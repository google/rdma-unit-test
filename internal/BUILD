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

package(
    default_testonly = True,
    default_visibility = ["//visibility:public"],
    licenses = ["notice"],
)

cc_library(
    name = "introspection_mlx4",
    hdrs = ["introspection_mlx4.h"],
    deps = [
        ":introspection_registrar",
        "//public:introspection",
        "@com_google_absl//absl/container:flat_hash_map",
        "@libibverbs",
    ],
    alwayslink = 1,
)

cc_library(
    name = "introspection_mlx5",
    hdrs = ["introspection_mlx5.h"],
    deps = [
        ":introspection_registrar",
        "//public:introspection",
        "@com_google_absl//absl/container:flat_hash_map",
        "@libibverbs",
    ],
    alwayslink = 1,
)

cc_library(
    name = "introspection_registrar",
    srcs = ["introspection_registrar.cc"],
    hdrs = ["introspection_registrar.h"],
    deps = [
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/strings",
        "@libibverbs",
    ],
)

cc_library(
    name = "introspection_rxe",
    hdrs = ["introspection_rxe.h"],
    deps = [
        ":introspection_registrar",
        "//public:introspection",
        "@com_google_absl//absl/container:flat_hash_map",
        "@libibverbs",
    ],
    alwayslink = 1,
)

cc_library(
    name = "verbs_extension",
    srcs = ["verbs_extension.cc"],
    hdrs = ["verbs_extension.h"],
    deps = [
        "//public:rdma_memblock",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status",
        "@libibverbs",
    ],
)

cc_library(
    name = "verbs_cleanup",
    srcs = ["verbs_cleanup.cc"],
    hdrs = ["verbs_cleanup.h"],
    deps = [
        "//public:rdma_memblock",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/synchronization",
        "@com_google_googletest//:gtest",
        "@libibverbs",
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
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@libibverbs",
    ],
)
