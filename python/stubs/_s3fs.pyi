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

import enum

from typing import Literal, NotRequired, Required, TypedDict

from ._fs import FileSystem
from .lib import KeyValueMetadata

class _ProxyOptions(TypedDict):
    schema: Required[Literal["http", "https"]]
    host: Required[str]
    port: Required[int]
    username: NotRequired[str]
    password: NotRequired[str]

class S3LogLevel(enum.IntEnum):
    Off = enum.auto()
    Fatal = enum.auto()
    Error = enum.auto()
    Warn = enum.auto()
    Info = enum.auto()
    Debug = enum.auto()
    Trace = enum.auto()

Off = S3LogLevel.Off
Fatal = S3LogLevel.Fatal
Error = S3LogLevel.Error
Warn = S3LogLevel.Warn
Info = S3LogLevel.Info
Debug = S3LogLevel.Debug
Trace = S3LogLevel.Trace

def initialize_s3(
    log_level: S3LogLevel = S3LogLevel.Fatal, num_event_loop_threads: int = 1
) -> None: ...
def ensure_s3_initialized() -> None: ...
def finalize_s3() -> None: ...
def ensure_s3_finalized() -> None: ...
def resolve_s3_region(bucket: str) -> str: ...

class S3RetryStrategy:
    max_attempts: int
    def __init__(self, max_attempts=3) -> None: ...

class AwsStandardS3RetryStrategy(S3RetryStrategy): ...
class AwsDefaultS3RetryStrategy(S3RetryStrategy): ...

class S3FileSystem(FileSystem):
    def __init__(
        self,
        *,
        access_key: str | None = None,
        secret_key: str | None = None,
        session_token: str | None = None,
        anonymous: bool = False,
        region: str | None = None,
        request_timeout: float | None = None,
        connect_timeout: float | None = None,
        scheme: Literal["http", "https"] = "https",
        endpoint_override: str | None = None,
        background_writes: bool = True,
        default_metadata: dict | KeyValueMetadata | None = None,
        role_arn: str | None = None,
        session_name: str | None = None,
        external_id: str | None = None,
        load_frequency: int = 900,
        proxy_options: _ProxyOptions | str | None = None,
        allow_bucket_creation: bool = False,
        allow_bucket_deletion: bool = False,
        check_directory_existence_before_creation: bool = False,
        retry_strategy: S3RetryStrategy = AwsStandardS3RetryStrategy(max_attempts=3),
        force_virtual_addressing: bool = False,
    ): ...
    @property
    def region(self) -> str: ...
