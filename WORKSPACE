workspace(
    name = "rdma_unit_test",
)
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Google Test. Official release 1.10.0.
http_archive(
    name = "com_google_googletest",
    sha256 = "9dc9157a9a1551ec7a7e43daea9a694a0bb5fb8bec81235d8a1e6ef64c716dcb",
    strip_prefix = "googletest-release-1.10.0",
    urls = ["https://github.com/google/googletest/archive/release-1.10.0.tar.gz"],
)

# Abseil. Latest feature not released yet(specifically absl::cleanup).
# Picked up a commit from Sep 2, 2020
http_archive(
    name = "com_google_absl",
    sha256 = "5ec35586b685eea11f198bb6e75f870e37fde62d15b95a3897c37b2d0bbd9017",
    strip_prefix = "abseil-cpp-143a27800eb35f4568b9be51647726281916aac9",
    urls = ["https://github.com/abseil/abseil-cpp/archive/143a27800eb35f4568b9be51647726281916aac9.zip"],
)

# glog
http_archive(
    name = "com_glog_glog",
    sha256 = "f28359aeba12f30d73d9e4711ef356dc842886968112162bc73002645139c39c",
    strip_prefix = "glog-0.4.0",
    url = "https://github.com/google/glog/archive/v0.4.0.tar.gz",
)

# gflags(required by glog)
http_archive(
    name = "com_github_gflags_gflags",
    strip_prefix = "gflags-2.2.2",
    sha256 = "19713a36c9f32b33df59d1c79b4958434cb005b5b47dc5400a7a4b078111d9b5",
    url = "https://github.com/gflags/gflags/archive/v2.2.2.zip",
)

# libibverbs
new_local_repository(
    name = "libibverbs",
    path = "/usr/lib64",
    build_file_content = """
cc_library(
    name = "libibverbs",
    srcs = [
         "libibverbs.so",
         "libnl-3.so.200",
         "libnl-route-3.so.200",
    ],
    visibility = ["//visibility:public"],
)
""",
)
