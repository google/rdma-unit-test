workspace(
    name = "rdma_unit_test",
)

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Google Test. Official release 1.13.0.
http_archive(
    name = "com_google_googletest",
    sha256 = "ad7fdba11ea011c1d925b3289cf4af2c66a352e18d4c7264392fead75e919363",
    strip_prefix = "googletest-1.13.0",
    urls = ["https://github.com/google/googletest/archive/refs/tags/v1.13.0.tar.gz"],
)

# Abseil. Latest feature not released yet(specifically absl::cleanup).
# Picked up the latest stable version (date: Jun 29, 2023)
http_archive(
    name = "com_google_absl",
    sha256 = "51d676b6846440210da48899e4df618a357e6e44ecde7106f1e44ea16ae8adc7",
    strip_prefix = "abseil-cpp-20230125.3",
    urls = ["https://github.com/abseil/abseil-cpp/archive/20230125.3.zip"],
)

# glog
http_archive(
    name = "com_glog_glog",
    sha256 = "8a83bf982f37bb70825df71a9709fa90ea9f4447fb3c099e1d720a439d88bad6",
    strip_prefix = "glog-0.6.0",
    url = "https://github.com/google/glog/archive/v0.6.0.tar.gz",
)

# gflags(required by glog)
http_archive(
    name = "com_github_gflags_gflags",
    sha256 = "19713a36c9f32b33df59d1c79b4958434cb005b5b47dc5400a7a4b078111d9b5",
    strip_prefix = "gflags-2.2.2",
    url = "https://github.com/gflags/gflags/archive/v2.2.2.zip",
)

# gRPC-1.56.1
http_archive(
    name = "com_github_grpc_grpc",
    sha256 = "cc3e039aedd7b76f59cf922215adc7c308347a662be1e5e26711ffbc7fd3ce48",
    strip_prefix = "grpc-1.56.1",
    url = "https://github.com/grpc/grpc/archive/refs/tags/v1.56.1.tar.gz",
)

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

grpc_extra_deps()

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

# License dependency: rules_license
http_archive(
    name = "rules_license",
    sha256 = "7626bea5473d3b11d44269c5b510a210f11a78bca1ed639b0f846af955b0fe31",
    strip_prefix = "rules_license-0.0.7",
    urls = ["https://github.com/bazelbuild/rules_license/archive/0.0.7.tar.gz"],
)

# Needed for protobuf.
http_archive(
    name = "rules_python",
    sha256 = "e5470e92a18aa51830db99a4d9c492cc613761d5bdb7131c04bd92b9834380f6",
    strip_prefix = "rules_python-4b84ad270387a7c439ebdccfd530e2339601ef27",
    urls = ["https://github.com/bazelbuild/rules_python/archive/4b84ad270387a7c439ebdccfd530e2339601ef27.tar.gz"],
)

# Magic_enum
http_archive(
    name = "magic_enum",
    sha256 = "2ac5f5f0591c8f587b53b89c3ef64c85cc24ebaaa389a659c6bf36a0aa192fe6",
    strip_prefix = "magic_enum-0.9.3",
    urls = ["https://github.com/Neargye/magic_enum/archive/v0.9.3.zip"],
)
