"""Compile the proto definitions into Python.

This tooling should be invoked to regenerate the Python grpc artifacts by running:

    python -m dagster._grpc.compile
"""
import os
import shutil
import subprocess
import sys

import pkg_resources

from dagster._utils import file_relative_path, safe_tempfile_path

PROTOS_DIR = file_relative_path(__file__, "protos")

PROTOS_PATH = os.path.join(PROTOS_DIR, "api.proto")

GENERATED_DIR = file_relative_path(__file__, "__generated__")

ISORT_SETTINGS_PATH = file_relative_path(__file__, "../../../../")

GENERATED_HEADER = [
    ("# @" + "generated\n"),  # This is to avoid matching the phab rule
    "\n",
    "# This file was generated by running `python -m dagster._grpc.compile`\n",
    "# Do not edit this file directly, and do not attempt to recompile it using\n",
    "# grpc_tools.protoc directly, as several changes must be made to the raw output\n",
    "\n",
]

GENERATED_GRPC_PYLINT_DIRECTIVE = [
    "# pylint: disable=no-member, unused-argument\n",
    "\n",
]

GENERATED_PB2_PYLINT_DIRECTIVE = [
    "# noqa: SLF001,no-name-in-module\n",
    "\n",
]


def protoc(generated_dir: str):
    generated_pb2_path = os.path.join(generated_dir, "api_pb2.py")
    generated_grpc_path = os.path.join(generated_dir, "api_pb2_grpc.py")

    # python -m grpc_tools.protoc \
    #   -I protos --python_out __generated__ --grpc_python_out __generated__ protos/api.proto
    _res = subprocess.check_output(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            "-I",
            PROTOS_DIR,
            "--python_out",
            generated_dir,
            "--grpc_python_out",
            generated_dir,
            PROTOS_PATH,
        ]
    )

    # The generated api_pb2_grpc.py file must be altered in two ways:
    # 1. Add a pylint directive, `disable=no-member, unused-argument`
    # 2. Change the import from `import api_pb2 as api__pb2` to `from . import api_pb2 as api__pb2`.
    #    See: https://github.com/grpc/grpc/issues/22914
    with safe_tempfile_path() as tempfile_path:
        shutil.copyfile(
            generated_grpc_path,
            tempfile_path,
        )
        with open(tempfile_path, "r", encoding="utf8") as generated:
            with open(generated_grpc_path, "w", encoding="utf8") as rewritten:
                for line in GENERATED_HEADER:
                    rewritten.write(line)

                for line in GENERATED_GRPC_PYLINT_DIRECTIVE:
                    rewritten.write(line)

                for line in generated.readlines():
                    if line == "import api_pb2 as api__pb2\n":
                        rewritten.write("from . import api_pb2 as api__pb2\n")
                    else:
                        rewritten.write(line)

    with safe_tempfile_path() as tempfile_path:
        shutil.copyfile(
            generated_pb2_path,
            tempfile_path,
        )
        with open(tempfile_path, "r", encoding="utf8") as generated:
            with open(generated_pb2_path, "w", encoding="utf8") as rewritten:
                for line in GENERATED_HEADER:
                    rewritten.write(line)

                for line in GENERATED_PB2_PYLINT_DIRECTIVE:
                    rewritten.write(line)

                for line in generated.readlines():
                    rewritten.write(line)

    installed_pkgs = {
        # pylint: disable=not-an-iterable
        pkg.key
        for pkg in pkg_resources.working_set
    }

    # Run black if it's available. This is under a conditional because black may not be available in
    # a test environment.
    if "black" in installed_pkgs:
        _res = subprocess.check_output(
            [
                sys.executable,
                "-m",
                "black",
                "-l",
                "100",
                "-t",
                "py35",
                "-t",
                "py36",
                "-t",
                "py37",
                "-t",
                "py38",
                generated_dir,
            ]
        )

    # Run isort if it's available. This is under a conditional because isort may not be available in
    # a test environment.
    if "isort" in installed_pkgs:
        _res = subprocess.check_output(
            [
                "isort",
                "--settings-path",
                ISORT_SETTINGS_PATH,
                generated_pb2_path,
                generated_grpc_path,
            ]
        )


if __name__ == "__main__":
    protoc(sys.argv[1] if len(sys.argv) > 1 else GENERATED_DIR)
