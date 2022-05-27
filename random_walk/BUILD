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

# Random Walk Test for RDMA.

package(
    default_testonly = True,
    default_visibility = ["//visibility:public"],
    licenses = ["notice"],
)

cc_test(
    name = "random_walk_test",
    srcs = ["random_walk_test.cc"],
    linkstatic = 1,
    shard_count = 4,
    deps = [
        ":action_weights",
        "//public:basic_fixture",
        "//public:introspection",
        "//random_walk/internal:multi_node_orchestrator",
        "//random_walk/internal:random_walk_config_cc_proto",
        "//random_walk/internal:single_node_orchestrator",
        "//unit:gunit_main",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:protobuf",
        "@libibverbs",
    ],
)

# random_walk_test_library is a library to support reuse of random walk test.
cc_library(
    name = "random_walk_test_library",
    testonly = 1,
    srcs = ["random_walk_test.cc"],
    deps = [
        ":action_weights",
        "//public:basic_fixture",
        "//public:introspection",
        "//random_walk/internal:multi_node_orchestrator",
        "//random_walk/internal:random_walk_config_cc_proto",
        "//random_walk/internal:single_node_orchestrator",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest",
        "@com_google_protobuf//:protobuf",
    ],
    alwayslink = 1,
)

cc_binary(
    name = "run_random_walk",
    srcs = ["random_walk_main.cc"],
    deps = [
        ":flags",
        "//internal:introspection_mlx4",
        "//internal:introspection_mlx5",
        "//internal:introspection_rxe",
        "//public:introspection",
        "//random_walk/internal:multi_node_orchestrator",
        "//random_walk/internal:random_walk_config_cc_proto",
        "//random_walk/internal:single_node_orchestrator",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/debugging:failure_signal_handler",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "flags",
    srcs = ["flags.cc"],
    hdrs = ["flags.h"],
    deps = ["@com_google_absl//absl/flags:flag"],
)

cc_library(
    name = "action_weights",
    srcs = ["action_weights.cc"],
    hdrs = ["action_weights.h"],
    deps = [
        "//public:status_matchers",
        "//random_walk/internal:random_walk_config_cc_proto",
        "@com_google_protobuf//:protobuf",
    ],
)
