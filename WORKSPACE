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

# gRPC-1.39.1
http_archive(
    name = "com_github_grpc_grpc",
    sha256 = "024118069912358e60722a2b7e507e9c3b51eeaeee06e2dd9d95d9c16f6639ec",
    strip_prefix = "grpc-1.39.1",
    url = "https://github.com/grpc/grpc/archive/refs/tags/v1.39.1.tar.gz",
)
load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")
grpc_deps()

# Needed for gRPC.
http_archive(
    name = "build_bazel_rules_swift",
    sha256 = "d0833bc6dad817a367936a5f902a0c11318160b5e80a20ece35fb85a5675c886",
    strip_prefix = "rules_swift-3eeeb53cebda55b349d64c9fc144e18c5f7c0eb8",
    urls = ["https://github.com/bazelbuild/rules_swift/archive/3eeeb53cebda55b349d64c9fc144e18c5f7c0eb8.tar.gz"],
)

# Protocol Buffers C++ v3.17.3
http_archive(
    name = "com_google_protobuf",
    sha256 = "51cec99f108b83422b7af1170afd7aeb2dd77d2bcbb7b6bad1f92509e9ccf8cb",
    strip_prefix = "protobuf-3.17.3",
    url = "https://github.com/protocolbuffers/protobuf/releases/download/v3.17.3/protobuf-cpp-3.17.3.tar.gz",
)
# Needed for protobuf.
http_archive(
    name = "rules_python",
    sha256 = "e5470e92a18aa51830db99a4d9c492cc613761d5bdb7131c04bd92b9834380f6",
    strip_prefix = "rules_python-4b84ad270387a7c439ebdccfd530e2339601ef27",
    urls = ["https://github.com/bazelbuild/rules_python/archive/4b84ad270387a7c439ebdccfd530e2339601ef27.tar.gz"],
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
