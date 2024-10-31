load("@rules_license//rules:license.bzl", "license")

package(
    default_applicable_licenses = [":package_license"],
    default_visibility = ["//visibility:public"],
    licenses = ["notice"],
)

license(
    name = "package_license",
    package_name = "rdma_unit_test",
    license_kinds = ["@rules_license//licenses/spdx:Apache-2.0"],
    license_text = "LICENSE",
)

exports_files(["LICENSE"])

cc_library(
    name = "system_libibverbs",
    linkopts = ["-libverbs"],
)
