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

# Test infrastructure and collections to stress RDMA datapath.

package(
    default_testonly = True,
    default_visibility = ["//visibility:public"],
    licenses = ["notice"],
)

proto_library(
    name = "config_proto",
    srcs = ["config.proto"],
)

cc_proto_library(
    name = "config_cc_proto",
    deps = [":config_proto"],
)

cc_library(
    name = "test_op",
    testonly = 1,
    srcs = ["test_op.cc"],
    hdrs = ["test_op.h"],
    deps = [
        ":op_types",
        ":qp_op_interface",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@libibverbs",
    ],
)

cc_library(
    name = "op_types",
    srcs = ["op_types.cc"],
    hdrs = ["op_types.h"],
    deps = ["@com_google_absl//absl/strings"],
)

cc_library(
    name = "qp_op_interface",
    hdrs = ["qp_op_interface.h"],
    deps = [
        ":op_types",
        "@com_google_absl//absl/status:statusor",
        "@libibverbs",
    ],
)

cc_library(
    name = "operation_generator",
    srcs = ["operation_generator.cc"],
    hdrs = ["operation_generator.h"],
    deps = [
        ":config_cc_proto",
        ":op_types",
        "//public:status_matchers",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf_lite",
    ],
)

cc_library(
    name = "qp_state",
    testonly = 1,
    srcs = ["qp_state.cc"],
    hdrs = ["qp_state.h"],
    deps = [
        ":op_types",
        ":operation_generator",
        ":qp_op_interface",
        ":test_op",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest",
        "@libibverbs",
    ],
)

cc_library(
    name = "client",
    srcs = ["client.cc"],
    hdrs = ["client.h"],
    deps = [
        ":op_types",
        ":operation_generator",
        ":qp_op_interface",
        ":qp_state",
        "//internal:verbs_attribute",
        "//public:page_size",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest",
        "@libibverbs",
    ],
)

cc_library(
    name = "transport_validation",
    testonly = 1,
    hdrs = ["transport_validation.h"],
    deps = ["@com_google_absl//absl/status"],
)

cc_library(
    name = "latency_measurement",
    hdrs = ["latency_measurement.h"],
    deps = [
        ":client",
        ":op_types",
    ],
)

cc_library(
    name = "op_profiles",
    srcs = ["op_profiles.cc"],
    hdrs = ["op_profiles.h"],
    deps = [":config_cc_proto"],
)

cc_library(
    name = "rdma_stress_fixture",
    testonly = 1,
    srcs = ["rdma_stress_fixture.cc"],
    hdrs = ["rdma_stress_fixture.h"],
    deps = [
        ":client",
        ":latency_measurement",
        ":op_types",
        ":qp_state",
        ":transport_validation",
        "//internal:verbs_attribute",
        "//public:basic_fixture",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@libibverbs",
    ],
)

cc_test(
    name = "simple_rdma_test",
    srcs = ["simple_rdma_test.cc"],
    linkstatic = 1,
    shard_count = 2,
    deps = [
        ":config_cc_proto",
        ":rdma_stress_fixture",
        "//public:status_matchers",
        "//unit:gunit_main",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status:statusor",
        "@libibverbs",
    ],
)

cc_test(
    name = "rdma_stability_test",
    srcs = ["rdma_stability_test.cc"],
    linkstatic = 1,
    shard_count = 2,
    deps = [
        ":config_cc_proto",
        ":op_types",
        ":rdma_stress_fixture",
        "//public:status_matchers",
        "//unit:gunit_main",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@libibverbs",
    ],
)

cc_test(
    name = "basic_rc_test",
    timeout = "long",
    srcs = ["basic_rc_test.cc"],
    linkstatic = 1,
    deps = [
        ":op_profiles",
        ":op_types",
        ":operation_generator",
        ":rdma_stress_fixture",
        "//public:status_matchers",
        "//unit:gunit_main",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/strings:str_format",
        "@libibverbs",
    ],
)

cc_test(
    name = "basic_ud_test",
    timeout = "long",
    srcs = ["basic_ud_test.cc"],
    linkstatic = 1,
    deps = [
        ":op_profiles",
        ":op_types",
        ":rdma_stress_fixture",
        "//public:status_matchers",
        "//public:verbs_util",
        "//unit:gunit_main",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:str_format",
        "@libibverbs",
    ],
)

cc_test(
    name = "cq_overrun_test",
    srcs = ["cq_overrun_test.cc"],
    linkstatic = 1,
    shard_count = 2,
    deps = [
        ":op_types",
        ":rdma_stress_fixture",
        "//public:status_matchers",
        "//unit:gunit_main",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@libibverbs",
    ],
)

cc_test(
    name = "mixed_qp_mode_test",
    timeout = "long",
    srcs = ["mixed_qp_mode_test.cc"],
    linkstatic = 1,
    deps = [
        ":client",
        ":op_profiles",
        ":op_types",
        ":operation_generator",
        ":rdma_stress_fixture",
        "//public:status_matchers",
        "//unit:gunit_main",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/strings:str_format",
        "@libibverbs",
    ],
)

cc_test(
    name = "qp_oversubscription_test",
    timeout = "long",
    srcs = ["qp_oversubscription_test.cc"],
    linkstatic = 1,
    deps = [
        ":client",
        ":op_types",
        ":operation_generator",
        ":rdma_stress_fixture",
        ":test_op",
        "//public:status_matchers",
        "//unit:gunit_main",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_googletest//:gtest",
        "@libibverbs",
    ],
)

cc_test(
    name = "ud_burst_test",
    srcs = ["ud_burst_test.cc"],
    linkstatic = 1,
    deps = [
        ":client",
        ":op_types",
        ":qp_state",
        ":rdma_stress_fixture",
        "//public:status_matchers",
        "//unit:gunit_main",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest",
        "@libibverbs",
    ],
)

cc_test(
    name = "vary_mtu_test",
    timeout = "long",
    srcs = ["vary_mtu_test.cc"],
    linkstatic = 1,
    deps = [
        ":client",
        ":op_types",
        ":operation_generator",
        ":qp_state",
        ":rdma_stress_fixture",
        "//internal:verbs_attribute",
        "//public:status_matchers",
        "//public:verbs_util",
        "//unit:gunit_main",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@libibverbs",
    ],
)

cc_test(
    name = "event_driven_completion_test",
    timeout = "long",
    srcs = ["event_driven_completion_test.cc"],
    linkstatic = 1,
    deps = [
        ":client",
        ":op_types",
        ":operation_generator",
        ":qp_state",
        ":rdma_stress_fixture",
        "//public:status_matchers",
        "//unit:gunit_main",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/time",
        "@libibverbs",
    ],
)

cc_test(
    name = "rnr_nack_test",
    srcs = ["rnr_nack_test.cc"],
    linkstatic = 1,
    deps = [
        ":client",
        ":op_types",
        ":rdma_stress_fixture",
        "//internal:verbs_attribute",
        "//public:status_matchers",
        "//unit:gunit_main",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/time",
        "@libibverbs",
    ],
)

cc_library(
    name = "test_collection",
    testonly = 1,
    srcs = [
        "basic_rc_test.cc",
        "basic_ud_test.cc",
        "cq_overrun_test.cc",
        "event_driven_completion_test.cc",
        "mixed_qp_mode_test.cc",
        "qp_oversubscription_test.cc",
        "rdma_stability_test.cc",
        "rnr_nack_test.cc",
        "simple_rdma_test.cc",
        "ud_burst_test.cc",
        "vary_mtu_test.cc",
    ],
    deps = [
        ":client",
        ":config_cc_proto",
        ":op_profiles",
        ":op_types",
        ":operation_generator",
        ":qp_state",
        ":rdma_stress_fixture",
        ":test_op",
        "//internal:verbs_attribute",
        "//public:status_matchers",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest",
        "@libibverbs",
    ],
    alwayslink = 1,
)
