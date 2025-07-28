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

load("//third_party/bazel_rules/rules_cc/cc:cc_binary.bzl", "cc_binary")
load("//third_party/bazel_rules/rules_cc/cc:cc_library.bzl", "cc_library")
load("//third_party/bazel_rules/rules_cc/cc:cc_test.bzl", "cc_test")

# Description:
#   Collection of typed test cases.
#
# linkstatic = 1 is to facilitate running on real hardware.
package(
    default_applicable_licenses = ["//:package_license"],
    default_testonly = True,
    default_visibility = ["//visibility:public"],
    licenses = ["notice"],
)

cc_library(
    name = "access_test_cc",
    srcs = ["access_test.cc"],
    deps = [
        ":rdma_verbs_fixture",
        ":system_libibverbs",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/strings",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

cc_library(
    name = "ah_test_cc",
    srcs = ["ah_test.cc"],
    deps = [
        ":rdma_verbs_fixture",
        ":system_libibverbs",
        "//internal:verbs_attribute",
        "//internal:verbs_extension",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

cc_library(
    name = "buffer_test_cc",
    srcs = ["buffer_test.cc"],
    deps = [
        ":rdma_verbs_fixture",
        ":system_libibverbs",
        "//public:basic_fixture",
        "//public:introspection",
        "//public:page_size",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/log:check",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/types:span",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

cc_library(
    name = "comp_channel_test_cc",
    srcs = ["comp_channel_test.cc"],
    deps = [
        ":rdma_verbs_fixture",
        ":system_libibverbs",
        "//internal:handle_garble",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/time",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

cc_library(
    name = "cq_test_cc",
    srcs = ["cq_test.cc"],
    deps = [
        ":batch_op_fixture",
        ":rdma_verbs_fixture",
        ":system_libibverbs",
        "//internal:handle_garble",
        "//public:flags",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/flags:flag",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/meta:type_traits",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/synchronization",
        "@abseil-cpp//absl/time",
        "@abseil-cpp//absl/types:span",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

cc_library(
    name = "cq_ex_test_cc",
    srcs = ["cq_ex_test.cc"],
    deps = [
        ":batch_op_fixture",
        ":rdma_verbs_fixture",
        ":system_libibverbs",
        "//internal:handle_garble",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/meta:type_traits",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/time",
        "@abseil-cpp//absl/types:span",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

cc_library(
    name = "device_test_cc",
    srcs = ["device_test.cc"],
    deps = [
        ":rdma_verbs_fixture",
        ":system_libibverbs",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/status:statusor",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

cc_library(
    name = "hugepage_test_cc",
    srcs = ["hugepage_test.cc"],
    deps = [
        ":loopback_fixture",
        ":system_libibverbs",
        "//public:introspection",
        "//public:page_size",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/log:check",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/strings",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

cc_library(
    name = "rc_test_cc",
    srcs = ["rc_test.cc"],
    deps = [
        ":loopback_fixture",
        ":system_libibverbs",
        "//internal:verbs_attribute",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/strings:str_format",
        "@abseil-cpp//absl/time",
        "@abseil-cpp//absl/types:span",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

cc_library(
    name = "mr_test_cc",
    srcs = ["mr_test.cc"],
    deps = [
        ":loopback_fixture",
        ":rdma_verbs_fixture",
        ":system_libibverbs",
        "//internal:handle_garble",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/synchronization",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

cc_library(
    name = "mw_test_cc",
    srcs = ["mw_test.cc"],
    deps = [
        ":loopback_fixture",
        ":system_libibverbs",
        "//internal:handle_garble",
        "//internal:verbs_attribute",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/cleanup",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/synchronization",
        "@abseil-cpp//absl/time",
        "@abseil-cpp//absl/types:optional",
        "@abseil-cpp//absl/types:span",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

cc_library(
    name = "pd_test_cc",
    srcs = ["pd_test.cc"],
    deps = [
        ":loopback_fixture",
        ":rdma_verbs_fixture",
        ":system_libibverbs",
        "//internal:handle_garble",
        "//internal:verbs_attribute",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/strings",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

cc_library(
    name = "qp_test_cc",
    srcs = ["qp_test.cc"],
    deps = [
        ":loopback_fixture",
        ":rdma_verbs_fixture",
        ":system_libibverbs",
        "//internal:handle_garble",
        "//internal:verbs_attribute",
        "//internal:verbs_extension",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/base:core_headers",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/synchronization",
        "@abseil-cpp//absl/time",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

cc_library(
    name = "srq_test_cc",
    srcs = ["srq_test.cc"],
    deps = [
        ":rdma_verbs_fixture",
        ":system_libibverbs",
        "//internal:handle_garble",
        "//internal:verbs_attribute",
        "//public:introspection",
        "//public:page_size",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/synchronization",
        "@abseil-cpp//absl/time",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

cc_library(
    name = "threaded_test_cc",
    srcs = ["threaded_test.cc"],
    deps = [
        ":rdma_verbs_fixture",
        ":system_libibverbs",
        "//internal:verbs_attribute",
        "//internal:verbs_extension",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/synchronization",
        "@abseil-cpp//absl/time",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

cc_library(
    name = "ud_test_cc",
    srcs = ["ud_test.cc"],
    deps = [
        ":loopback_fixture",
        ":rdma_verbs_fixture",
        ":system_libibverbs",
        "//internal:handle_garble",
        "//internal:verbs_attribute",
        "//public:introspection",
        "//public:page_size",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/strings:str_format",
        "@abseil-cpp//absl/synchronization",
        "@abseil-cpp//absl/time",
        "@abseil-cpp//absl/types:span",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

cc_library(
    name = "stress_test_cc",
    srcs = ["stress_test.cc"],
    deps = [
        ":rdma_verbs_fixture",
        ":system_libibverbs",
        "//internal:verbs_attribute",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/log:check",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/time",
        "@abseil-cpp//absl/types:span",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

cc_library(
    name = "async_event_test_cc",
    srcs = ["async_event_test.cc"],
    deps = [
        ":loopback_fixture",
        ":system_libibverbs",
        "//internal:verbs_attribute",
        "//public:introspection",
        "//public:status_matchers",
        "//public:verbs_util",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/time",
        "@googletest//:gtest",
    ],
    alwayslink = 1,
)

# The gunit_main is linked into the opensource version of the tests below.  rdma_unit_test depends
# on the following libraries: glog/gflag/gtest
# The standard opensource gunit_main does not initialize gflags or glog.  This target contains a
# 'main' that initializes all the dependencies before calling RUN_ALL_TESTS.
cc_library(
    name = "gunit_main",
    testonly = 1,
    srcs = ["gunit_main.cc"],
    deps = [
        "//internal:introspection_irdma",
        "//internal:introspection_mlx4",
        "//internal:introspection_mlx5",
        "//internal:introspection_rxe",
        "@abseil-cpp//absl/debugging:failure_signal_handler",
        "@abseil-cpp//absl/flags:parse",
        "@abseil-cpp//absl/log:initialize",
        "@googletest//:gtest",
    ],
)

cc_binary(
    name = "run_unit_test",
    srcs = [],
    # Required to build statically, see b/380811817 comment #23.
    features = ["no_default_linker_script"],
    deps = [
        ":access_test_cc",
        ":ah_test_cc",
        ":buffer_test_cc",
        ":comp_channel_test_cc",
        ":cq_ex_test_cc",
        ":cq_test_cc",
        ":device_test_cc",
        ":gunit_main",
        ":hugepage_test_cc",
        ":mr_test_cc",
        ":mw_test_cc",
        ":pd_test_cc",
        ":qp_test_cc",
        ":rc_test_cc",
        ":srq_test_cc",
        ":stress_test_cc",
        ":threaded_test_cc",
        ":ud_test_cc",
    ],
)

cc_binary(
    name = "run_unit_static_link_test",
    srcs = [],
    # Required to build statically, see b/380811817 comment #23.
    features = ["fully_static_link"],
    deps = [
        ":access_test_cc",
        ":ah_test_cc",
        ":buffer_test_cc",
        ":comp_channel_test_cc",
        ":cq_ex_test_cc",
        ":cq_test_cc",
        ":device_test_cc",
        ":gunit_main",
        ":hugepage_test_cc",
        ":mr_test_cc",
        ":mw_test_cc",
        ":pd_test_cc",
        ":qp_test_cc",
        ":rc_test_cc",
        ":srq_test_cc",
        ":stress_test_cc",
        ":threaded_test_cc",
        ":ud_test_cc",
        "//third_party/grte:static_libs",
    ],
)

cc_library(
    name = "rdma_verbs_fixture",
    srcs = ["rdma_verbs_fixture.cc"],
    hdrs = ["rdma_verbs_fixture.h"],
    deps = [
        "//public:basic_fixture",
        "//public:introspection",
        "//public:verbs_helper_suite",
        "@abseil-cpp//absl/log",
    ],
)

cc_library(
    name = "loopback_fixture",
    srcs = ["loopback_fixture.cc"],
    hdrs = ["loopback_fixture.h"],
    deps = [
        ":rdma_verbs_fixture",
        ":system_libibverbs",
        "//internal:verbs_attribute",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/log:check",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/strings",
    ],
)

cc_library(
    name = "batch_op_fixture",
    srcs = ["batch_op_fixture.cc"],
    hdrs = ["batch_op_fixture.h"],
    deps = [
        ":rdma_verbs_fixture",
        ":system_libibverbs",
        "//internal:verbs_attribute",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/log:check",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/synchronization",
        "@abseil-cpp//absl/types:span",
    ],
)

cc_library(
    name = "system_libibverbs",
    linkopts = ["-libverbs"],
)

cc_test(
    name = "access_test",
    srcs = [],
    linkstatic = 1,
    deps = [
        ":access_test_cc",
        ":gunit_main",
        ":system_libibverbs",
    ],
)

cc_test(
    name = "ah_test",
    srcs = [],
    linkstatic = 1,
    deps = [
        ":ah_test_cc",
        ":gunit_main",
        ":system_libibverbs",
    ],
)

cc_test(
    name = "buffer_test",
    srcs = [],
    linkstatic = 1,
    deps = [
        ":buffer_test_cc",
        ":gunit_main",
        ":system_libibverbs",
    ],
)

cc_test(
    name = "comp_channel_test",
    srcs = [],
    linkstatic = 1,
    deps = [
        ":comp_channel_test_cc",
        ":gunit_main",
        ":system_libibverbs",
    ],
)

cc_test(
    name = "cq_test",
    srcs = [],
    linkstatic = 1,
    deps = [
        ":cq_test_cc",
        ":gunit_main",
        ":system_libibverbs",
    ],
)

cc_test(
    name = "cq_ex_test",
    srcs = [],
    linkstatic = 1,
    deps = [
        ":cq_ex_test_cc",
        ":gunit_main",
        ":system_libibverbs",
    ],
)

cc_test(
    name = "device_test",
    srcs = [],
    linkstatic = 1,
    deps = [
        ":device_test_cc",
        ":gunit_main",
        ":system_libibverbs",
    ],
)

cc_test(
    name = "hugepage_test",
    srcs = [],
    linkstatic = 1,
    deps = [
        ":gunit_main",
        ":hugepage_test_cc",
        ":system_libibverbs",
    ],
)

cc_test(
    name = "rc_test",
    srcs = [],
    linkstatic = 1,
    shard_count = 2,
    deps = [
        ":gunit_main",
        ":rc_test_cc",
        ":system_libibverbs",
    ],
)

cc_test(
    name = "mr_test",
    srcs = [],
    linkstatic = 1,
    deps = [
        ":gunit_main",
        ":mr_test_cc",
        ":system_libibverbs",
    ],
)

cc_test(
    name = "mw_test",
    srcs = [],
    linkstatic = 1,
    deps = [
        ":gunit_main",
        ":mw_test_cc",
        ":system_libibverbs",
    ],
)

cc_test(
    name = "pd_test",
    srcs = [],
    linkstatic = 1,
    deps = [
        ":gunit_main",
        ":pd_test_cc",
        ":system_libibverbs",
    ],
)

cc_test(
    name = "qp_test",
    srcs = [],
    linkstatic = 1,
    deps = [
        ":gunit_main",
        ":qp_test_cc",
        ":system_libibverbs",
    ],
)

cc_test(
    name = "srq_test",
    srcs = [],
    linkstatic = 1,
    deps = [
        ":gunit_main",
        ":srq_test_cc",
        ":system_libibverbs",
    ],
)

cc_test(
    name = "threaded_test",
    srcs = [],
    linkstatic = 1,
    shard_count = 3,
    deps = [
        ":gunit_main",
        ":system_libibverbs",
        ":threaded_test_cc",
    ],
)

cc_test(
    name = "ud_test",
    srcs = [],
    linkstatic = 1,
    deps = [
        ":gunit_main",
        ":system_libibverbs",
        ":ud_test_cc",
    ],
)

cc_test(
    name = "stress_test",
    srcs = [],
    linkstatic = 1,
    deps = [
        ":gunit_main",
        ":stress_test_cc",
        ":system_libibverbs",
    ],
)

cc_test(
    name = "async_event_test",
    srcs = [],
    linkstatic = 1,
    deps = [
        ":async_event_test_cc",
        ":gunit_main",
        ":system_libibverbs",
    ],
)
