# Public Interface for rdma unit tests:
# Note: These are the target directly referenced by test fixtures and test cases.

package(
    default_testonly = True,
    default_visibility = ["//visibility:public"],
    licenses = ["notice"],
)

cc_library(
    name = "flags",
    srcs = ["flags.cc"],
    hdrs = ["flags.h"],
    deps = [
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/strings",
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
        ":verbs_util",
        "//internal:introspection_registrar",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@libibverbs",
    ],
)

cc_library(
    name = "rdma_memblock",
    srcs = ["rdma_memblock.cc"],
    hdrs = ["rdma_memblock.h"],
    deps = [
        ":page_size",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "map_util",
    hdrs = ["map_util.h"],
    deps = [
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
    ],
)

cc_library(
    name = "verbs_util",
    srcs = ["verbs_util.cc"],
    hdrs = ["verbs_util.h"],
    deps = [
        ":flags",
        ":status_matchers",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest",
        "@libibverbs",
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
        ":verbs_util",
        "//internal:roce_backend",
        "//internal:roce_extension",
        "//internal:verbs_backend",
        "//internal:verbs_cleanup",
        "//internal:verbs_extension_interface",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/synchronization",
        "@libibverbs",
    ],
)

cc_library(
    name = "status_matchers",
    testonly = 1,
    srcs = ["status_matchers.cc"],
    hdrs = ["status_matchers.h"],
    deps = [
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "page_size",
    hdrs = ["page_size.h"],
)
