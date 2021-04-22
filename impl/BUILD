# Implementation of RDMA unit test framework.
# Targets in this packages is designed to be direct dependencies of targets in ../public/ but not ../cases/.

package(
    default_testonly = True,
    default_visibility = ["//visibility:public"],
    licenses = ["notice"],
)

cc_library(
    name = "introspection_extras",
    deps = [
        ":introspection_mlx4",
        ":introspection_mlx5",
        ":introspection_rxe",
    ],
)

cc_library(
    name = "introspection_mlx4",
    hdrs = ["introspection_mlx4.h"],
    deps = [
        ":introspection_registrar",
        "//public:introspection",
    ],
    alwayslink = 1,
)

cc_library(
    name = "introspection_mlx5",
    hdrs = ["introspection_mlx5.h"],
    deps = [
        ":introspection_registrar",
        "//public:introspection",
    ],
    alwayslink = 1,
)

cc_library(
    name = "introspection_registrar",
    srcs = ["introspection_registrar.cc"],
    hdrs = ["introspection_registrar.h"],
    deps = ["@com_glog_glog//:glog"],
)

cc_library(
    name = "introspection_rxe",
    hdrs = ["introspection_rxe.h"],
    deps = [
        ":introspection_registrar",
        "//public:introspection",
    ],
    alwayslink = 1,
)

cc_library(
    name = "roce_allocator",
    srcs = ["roce_allocator.cc"],
    hdrs = ["roce_allocator.h"],
    deps = [
        ":verbs_allocator",
        "//public:util",
    ],
)

cc_library(
    name = "roce_backend",
    srcs = ["roce_backend.cc"],
    hdrs = ["roce_backend.h"],
    deps = [
        ":verbs_backend",
        "//public:flags",
        "//public:util",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "verbs_allocator",
    srcs = ["verbs_allocator.cc"],
    hdrs = ["verbs_allocator.h"],
    deps = [
        "//public:flags",
        "//public:rdma-memblock",
        "//public:util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "verbs_backend",
    srcs = ["verbs_backend.cc"],
    hdrs = ["verbs_backend.h"],
    deps = [
        "//public:util",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
    ],
)
