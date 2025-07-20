# BSD 2-Clause License
#
# Copyright (c) 2024, ZhengYu, Xu
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from typing import NamedTuple

class VersionInfo(NamedTuple):
    major: int
    minor: int
    patch: int

class BuildInfo(NamedTuple):
    version: str
    version_info: VersionInfo
    so_version: str
    full_so_version: str
    compiler_id: str
    compiler_version: str
    compiler_flags: str
    git_id: str
    git_description: str
    package_kind: str
    build_type: str

class RuntimeInfo(NamedTuple):
    simd_level: str
    detected_simd_level: str

cpp_build_info: BuildInfo
cpp_version: str
cpp_version_info: VersionInfo

def runtime_info() -> RuntimeInfo: ...
def set_timezone_db_path(path: str) -> None: ...

__all__ = [
    "VersionInfo",
    "BuildInfo",
    "RuntimeInfo",
    "cpp_build_info",
    "cpp_version",
    "cpp_version_info",
    "runtime_info",
    "set_timezone_db_path",
]
