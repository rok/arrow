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

from pyarrow._fs import (  # noqa
    FileSelector,
    FileType,
    FileInfo,
    FileSystem,
    LocalFileSystem,
    SubTreeFileSystem,
    _MockFileSystem,
    FileSystemHandler,
    PyFileSystem,
    SupportedFileSystem,
)
from pyarrow._azurefs import AzureFileSystem
from pyarrow._hdfs import HadoopFileSystem
from pyarrow._gcsfs import GcsFileSystem
from pyarrow._s3fs import (  # noqa
    AwsDefaultS3RetryStrategy,
    AwsStandardS3RetryStrategy,
    S3FileSystem,
    S3LogLevel,
    S3RetryStrategy,
    ensure_s3_initialized,
    finalize_s3,
    ensure_s3_finalized,
    initialize_s3,
    resolve_s3_region,
)

FileStats = FileInfo

def copy_files(
    source: str,
    destination: str,
    source_filesystem: SupportedFileSystem | None = None,
    destination_filesystem: SupportedFileSystem | None = None,
    *,
    chunk_size: int = 1024 * 1024,
    use_threads: bool = True,
) -> None: ...

class FSSpecHandler(FileSystemHandler):  # type: ignore[misc]
    fs: SupportedFileSystem
    def __init__(self, fs: SupportedFileSystem) -> None: ...

__all__ = [
    # _fs
    "FileSelector",
    "FileType",
    "FileInfo",
    "FileSystem",
    "LocalFileSystem",
    "SubTreeFileSystem",
    "_MockFileSystem",
    "FileSystemHandler",
    "PyFileSystem",
    # _azurefs
    "AzureFileSystem",
    # _hdfs
    "HadoopFileSystem",
    # _gcsfs
    "GcsFileSystem",
    # _s3fs
    "AwsDefaultS3RetryStrategy",
    "AwsStandardS3RetryStrategy",
    "S3FileSystem",
    "S3LogLevel",
    "S3RetryStrategy",
    "ensure_s3_initialized",
    "finalize_s3",
    "ensure_s3_finalized",
    "initialize_s3",
    "resolve_s3_region",
    # fs
    "FileStats",
    "copy_files",
    "FSSpecHandler",
]
