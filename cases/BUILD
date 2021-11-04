# Description:
#   Collection of typed test cases.
#
# linkstatic = 1 is to facilitate running on real hardware.

package(
    default_testonly = True,
    default_visibility = ["//visibility:public"],
    licenses = ["notice"],
)

cc_test(
    name = "access_test",
    srcs = ["access_test.cc"],
    linkstatic = 1,
    deps = [
        ":basic_fixture",
        ":gunit_main",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@libibverbs",
    ],
)

cc_test(
    name = "ah_test",
    srcs = ["ah_test.cc"],
    linkstatic = 1,
    deps = [
        ":basic_fixture",
        ":gunit_main",
        "//internal:handle_garble",
        "//public:flags",
        "//public:introspection",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@libibverbs",
    ],
)

cc_library(
    name = "basic_fixture",
    srcs = ["basic_fixture.cc"],
    hdrs = ["basic_fixture.h"],
    deps = [
        "//public:flags",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/flags:flag",
        "@com_google_googletest//:gtest",
        "@libibverbs",
    ],
)

cc_library(
    name = "batch_op_fixture",
    srcs = ["batch_op_fixture.cc"],
    hdrs = ["batch_op_fixture.h"],
    deps = [
        ":basic_fixture",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@libibverbs",
    ],
)

cc_test(
    name = "buffer_test",
    srcs = ["buffer_test.cc"],
    linkstatic = 1,
    deps = [
        ":basic_fixture",
        ":gunit_main",
        "//public:introspection",
        "//public:page_size",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@libibverbs",
    ],
)

cc_test(
    name = "comp_channel_test",
    srcs = ["comp_channel_test.cc"],
    linkstatic = 1,
    deps = [
        ":basic_fixture",
        ":gunit_main",
        "//internal:handle_garble",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/time",
        "@libibverbs",
    ],
)

cc_test(
    name = "cq_test",
    srcs = ["cq_test.cc"],
    linkstatic = 1,
    deps = [
        ":basic_fixture",
        ":batch_op_fixture",
        ":gunit_main",
        "//internal:handle_garble",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@libibverbs",
    ],
)

cc_test(
    name = "cq_ex_test",
    srcs = ["cq_ex_test.cc"],
    linkstatic = 1,
    deps = [
        ":basic_fixture",
        ":batch_op_fixture",
        ":gunit_main",
        "//internal:handle_garble",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@libibverbs",
    ],
)

cc_test(
    name = "device_test",
    srcs = ["device_test.cc"],
    linkstatic = 1,
    deps = [
        ":basic_fixture",
        ":gunit_main",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status:statusor",
        "@libibverbs",
    ],
)

# The gunit_main is linked into the opensource version of the above tests.  rdma_unit_test depends
# on the following libraries: glog/gflag/gtest
# The standard opensource gunit_main does not initialize gflags or glog.  This target contains a
# 'main' that initializes all the dependencies before calling RUN_ALL_TESTS.
cc_library(
    name = "gunit_main",
    testonly = 1,
    srcs = ["gunit_main.cc"],
    deps = [
        "//internal:introspection_mlx4",
        "//internal:introspection_mlx5",
        "//internal:introspection_rxe",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/debugging:failure_signal_handler",
        "@com_google_absl//absl/flags:parse",
        "@com_google_googletest//:gtest",
    ],
)

cc_binary(
    name = "run_unit_test",
    srcs = [],
    deps = [
        ":gunit_main",
        ":test_collection",
    ],
)

cc_test(
    name = "hugepage_test",
    srcs = ["hugepage_test.cc"],
    linkstatic = 1,
    deps = [
        ":basic_fixture",
        ":gunit_main",
        "//public:introspection",
        "//public:page_size",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@libibverbs",
    ],
)

cc_test(
    name = "rc_test",
    srcs = ["rc_test.cc"],
    linkstatic = 1,
    shard_count = 2,
    deps = [
        ":gunit_main",
        ":loopback_fixture",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@libibverbs",
    ],
)

cc_test(
    name = "mr_test",
    srcs = ["mr_test.cc"],
    linkstatic = 1,
    deps = [
        ":basic_fixture",
        ":gunit_main",
        "//internal:handle_garble",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/synchronization",
        "@libibverbs",
    ],
)

cc_test(
    name = "mw_test",
    srcs = ["mw_test.cc"],
    linkstatic = 1,
    deps = [
        ":basic_fixture",
        ":gunit_main",
        "//internal:handle_garble",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@libibverbs",
    ],
)

cc_test(
    name = "pd_test",
    srcs = ["pd_test.cc"],
    linkstatic = 1,
    deps = [
        ":basic_fixture",
        ":gunit_main",
        "//internal:handle_garble",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@libibverbs",
    ],
)

cc_test(
    name = "qp_test",
    srcs = ["qp_test.cc"],
    linkstatic = 1,
    deps = [
        ":basic_fixture",
        ":gunit_main",
        "//internal:handle_garble",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/time",
        "@libibverbs",
    ],
)

cc_test(
    name = "rendezvous_test",
    srcs = ["rendezvous_test.cc"],
    linkstatic = 1,
    shard_count = 3,
    deps = [
        ":basic_fixture",
        ":gunit_main",
        "//public:introspection",
        "//public:page_size",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:fixed_array",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@libibverbs",
    ],
)

cc_test(
    name = "srq_test",
    srcs = ["srq_test.cc"],
    linkstatic = 1,
    deps = [
        ":basic_fixture",
        ":gunit_main",
        "//internal:handle_garble",
        "//public:introspection",
        "//public:page_size",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@libibverbs",
    ],
)

cc_test(
    name = "threaded_test",
    srcs = ["threaded_test.cc"],
    linkstatic = 1,
    deps = [
        ":basic_fixture",
        ":gunit_main",
        "//internal:verbs_extension_interface",
        "//public:flags",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@libibverbs",
    ],
)

cc_test(
    name = "ud_test",
    srcs = ["ud_test.cc"],
    linkstatic = 1,
    deps = [
        ":gunit_main",
        ":loopback_fixture",
        "//public:introspection",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@libibverbs",
    ],
)

# test_collection is a library to support reuse of unit tests.
cc_library(
    name = "test_collection",
    testonly = 1,
    srcs = [
        "access_test.cc",
        "ah_test.cc",
        "buffer_test.cc",
        "comp_channel_test.cc",
        "cq_ex_test.cc",
        "cq_test.cc",
        "device_test.cc",
        "hugepage_test.cc",
        "mr_test.cc",
        "mw_test.cc",
        "pd_test.cc",
        "qp_test.cc",
        "rc_test.cc",
        "rendezvous_test.cc",
        "srq_test.cc",
        "threaded_test.cc",
        "ud_test.cc",
    ],
    deps = [
        ":basic_fixture",
        ":batch_op_fixture",
        ":loopback_fixture",
        "//internal:handle_garble",
        "//internal:verbs_extension_interface",
        "//public:flags",
        "//public:introspection",
        "//public:page_size",
        "//public:rdma_memblock",
        "//public:status_matchers",
        "//public:verbs_helper_suite",
        "//public:verbs_util",
        "@com_glog_glog//:glog",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:fixed_array",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest",
        "@libibverbs",
    ],
    alwayslink = 1,
)

cc_library(
    name = "loopback_fixture",
    srcs = ["loopback_fixture.cc"],
    hdrs = ["loopback_fixture.h"],
    deps = [
        ":basic_fixture",
        "//public:status_matchers",
        "//public:verbs_util",
        "@com_google_absl//absl/status:statusor",
        "@libibverbs",
    ],
)
