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

load("@grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")

package(
    default_applicable_licenses = ["//:package_license"],
    default_testonly = True,
    default_visibility = ["//visibility:public"],
    licenses = ["notice"],
)

cc_library(
    name = "random_walk_client",
    srcs = ["random_walk_client.cc"],
    hdrs = ["random_walk_client.h"],
    deps = [
        ":bind_ops_tracker",
        ":client_update_service_cc_proto",
        ":completion_profile",
        ":ibv_resource_manager",
        ":inbound_update_interface",
        ":invalidate_ops_tracker",
        ":logging",
        ":random_walk_config_cc_proto",
        ":sampling",
        ":system_libibverbs",
        ":types",
        ":update_dispatcher_interface",
        "//internal:verbs_attribute",
        "//public:introspection",
        "//public:map_util",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/base:core_headers",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@abseil-cpp//absl/flags:flag",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/log:check",
        "@abseil-cpp//absl/random",
        "@abseil-cpp//absl/random:distributions",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/synchronization",
        "@abseil-cpp//absl/time",
        "@abseil-cpp//absl/types:optional",
        "@abseil-cpp//absl/types:span",
        "@magic_enum",
    ],
)

cc_library(
    name = "ibv_resource_manager",
    srcs = ["ibv_resource_manager.cc"],
    hdrs = ["ibv_resource_manager.h"],
    deps = [
        ":sampling",
        ":system_libibverbs",
        ":types",
        "//public:map_util",
        "//public:verbs_util",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@abseil-cpp//absl/flags:flag",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/log:check",
        "@abseil-cpp//absl/types:optional",
    ],
)

cc_library(
    name = "rpc_server",
    srcs = ["rpc_server.cc"],
    hdrs = ["rpc_server.h"],
    deps = [
        ":client_update_service_cc_proto",
        ":client_update_service_grpc_proto",
        ":inbound_update_interface",
        ":types",
        ":update_reorder_queue",
        "@abseil-cpp//absl/base:core_headers",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/log:check",
        "@abseil-cpp//absl/synchronization",
        "@grpc//:grpc++",
    ],
)

cc_library(
    name = "single_node_orchestrator",
    srcs = ["single_node_orchestrator.cc"],
    hdrs = ["single_node_orchestrator.h"],
    deps = [
        ":loopback_update_dispatcher",
        ":random_walk_client",
        ":random_walk_config_cc_proto",
        ":system_libibverbs",
        ":types",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/time",
    ],
)

cc_library(
    name = "multi_node_orchestrator",
    srcs = ["multi_node_orchestrator.cc"],
    hdrs = ["multi_node_orchestrator.h"],
    deps = [
        ":grpc_update_dispatcher",
        ":grpc_update_handler",
        ":random_walk_client",
        ":random_walk_config_cc_proto",
        ":rpc_server",
        ":system_libibverbs",
        ":types",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@abseil-cpp//absl/time",
        "@grpc//:grpc++",
    ],
)

cc_library(
    name = "sampling",
    srcs = ["sampling.cc"],
    hdrs = ["sampling.h"],
    deps = [
        ":random_walk_config_cc_proto",
        ":system_libibverbs",
        ":types",
        "//public:page_size",
        "//public:rdma_memblock",
        "//public:verbs_util",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/log:check",
        "@abseil-cpp//absl/random",
        "@abseil-cpp//absl/random:distributions",
        "@abseil-cpp//absl/types:optional",
        "@abseil-cpp//absl/types:span",
    ],
)

cc_library(
    name = "types",
    srcs = ["types.cc"],
    hdrs = ["types.h"],
    deps = ["@magic_enum"],
)

cc_library(
    name = "bind_ops_tracker",
    srcs = ["bind_ops_tracker.cc"],
    hdrs = ["bind_ops_tracker.h"],
    deps = [
        ":system_libibverbs",
        "//public:map_util",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/types:optional",
    ],
)

cc_library(
    name = "invalidate_ops_tracker",
    srcs = ["invalidate_ops_tracker.cc"],
    hdrs = ["invalidate_ops_tracker.h"],
    deps = [
        ":types",
        "//public:map_util",
        "@abseil-cpp//absl/container:flat_hash_map",
    ],
)

cc_library(
    name = "grpc_update_dispatcher",
    srcs = ["grpc_update_dispatcher.cc"],
    hdrs = ["grpc_update_dispatcher.h"],
    deps = [
        ":client_update_service_cc_proto",
        ":client_update_service_grpc_proto",
        ":types",
        ":update_dispatcher_interface",
        "//public:map_util",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/log:check",
        "@abseil-cpp//absl/time",
        "@grpc//:grpc++",
    ],
)

cc_library(
    name = "grpc_update_handler",
    srcs = ["grpc_update_handler.cc"],
    hdrs = ["grpc_update_handler.h"],
    deps = [
        ":inbound_update_interface",
        ":rpc_server",
        "@abseil-cpp//absl/log:check",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/time",
        "@grpc//:grpc++",
    ],
)

cc_library(
    name = "loopback_update_dispatcher",
    srcs = ["loopback_update_dispatcher.cc"],
    hdrs = ["loopback_update_dispatcher.h"],
    deps = [
        ":client_update_service_cc_proto",
        ":inbound_update_interface",
        ":update_dispatcher_interface",
        "//public:map_util",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/log:check",
    ],
)

cc_library(
    name = "logging",
    srcs = ["logging.cc"],
    hdrs = ["logging.h"],
    deps = [
        ":system_libibverbs",
        ":types",
        "//public:rdma_memblock",
        "@abseil-cpp//absl/log",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/time",
    ],
)

cc_library(
    name = "inbound_update_interface",
    hdrs = ["inbound_update_interface.h"],
    deps = [":client_update_service_cc_proto"],
)

cc_library(
    name = "update_dispatcher_interface",
    hdrs = ["update_dispatcher_interface.h"],
    deps = [":client_update_service_cc_proto"],
)

cc_library(
    name = "update_reorder_queue",
    srcs = ["update_reorder_queue.cc"],
    hdrs = ["update_reorder_queue.h"],
    deps = [
        ":client_update_service_cc_proto",
        "//public:map_util",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/meta:type_traits",
        "@abseil-cpp//absl/types:optional",
    ],
)

cc_library(
    name = "completion_profile",
    srcs = ["completion_profile.cc"],
    hdrs = ["completion_profile.h"],
    deps = [
        ":system_libibverbs",
        ":types",
        "//public:map_util",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@magic_enum",
    ],
)

cc_library(
    name = "system_libibverbs",
    linkopts = ["-libverbs"],
)

proto_library(
    name = "client_update_service_proto",
    srcs = ["client_update_service.proto"],
)

cc_proto_library(
    name = "client_update_service_cc_proto",
    deps = [
        ":client_update_service_proto",
    ],
)

proto_library(
    name = "random_walk_config_proto",
    srcs = ["random_walk_config.proto"],
)

cc_proto_library(
    name = "random_walk_config_cc_proto",
    deps = [":random_walk_config_proto"],
)

cc_grpc_library(
    name = "client_update_service_grpc_proto",
    srcs = [":client_update_service_proto"],
    grpc_only = ["True"],
    deps = [":client_update_service_cc_proto"],
)
