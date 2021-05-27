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
        ":util",
        "//impl:introspection_registrar",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "rdma_memblock",
    srcs = ["rdma_memblock.cc"],
    hdrs = ["rdma_memblock.h"],
    deps = [
        ":util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "util",
    srcs = ["util.cc"],
    hdrs = ["util.h"],
    deps = [
        ":flags",
        ":status_matchers",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "verbs_helper_suite",
    srcs = ["verbs_helper_suite.cc"],
    hdrs = ["verbs_helper_suite.h"],
    deps = [
        ":flags",
        ":util",
        "//impl:roce_allocator",
        "//impl:roce_backend",
        "//impl:verbs_allocator",
        "//impl:verbs_backend",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "status_matchers",
    testonly = 1,
    hdrs = ["status_matchers.h"],
    deps = ["@com_google_googletest//:gtest"],
)
