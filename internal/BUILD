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
        "@com_google_absl//absl/container:flat_hash_set",
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
        "@com_google_absl//absl/container:flat_hash_set",
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
        "@libibverbs",
    ],
)

cc_library(
    name = "introspection_rxe",
    hdrs = ["introspection_rxe.h"],
    deps = [
        ":introspection_registrar",
        "//public:introspection",
        "@com_google_absl//absl/container:flat_hash_set",
        "@libibverbs",
    ],
    alwayslink = 1,
)

cc_library(
    name = "roce_extension",
    srcs = ["roce_extension.cc"],
    hdrs = ["roce_extension.h"],
    deps = [
        ":verbs_extension_interface",
        "//public:rdma_memblock",
        "//public:verbs_util",
        "@libibverbs",
    ],
)

cc_library(
    name = "roce_backend",
    srcs = ["roce_backend.cc"],
    hdrs = ["roce_backend.h"],
    deps = [
        ":verbs_backend",
        "//public:flags",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@libibverbs",
    ],
)

cc_library(
    name = "verbs_extension_interface",
    hdrs = ["verbs_extension_interface.h"],
    deps = [
        "//public:rdma_memblock",
        "//public:verbs_util",
        "@libibverbs",
    ],
)

cc_library(
    name = "verbs_backend",
    srcs = ["verbs_backend.cc"],
    hdrs = ["verbs_backend.h"],
    deps = [
        "//public:status_matchers",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
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
        "@libibverbs",
    ],
)
