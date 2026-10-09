# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from io import BufferedReader, BufferedWriter, BytesIO
from pathlib import Path
from pyarrow.lib import Buffer, BufferOutputStream, BufferReader, NativeFile, OSFile
from pyarrow.tests.util import FSProtocolClass
from typing import Optional, Tuple, Union
import os
import re
import pyarrow
from typing import Any, Callable, IO, Iterable, Iterator, Literal, Mapping, NamedTuple, Self, Sequence
from _typeshed import Incomplete
from pyarrow._azurefs import AzureFileSystem as AzureFileSystem
from pyarrow._fs import FileInfo as FileInfo, FileSelector as FileSelector, FileSystem as FileSystem, FileSystemHandler as FileSystemHandler, FileType as FileType, LocalFileSystem as LocalFileSystem, PyFileSystem as PyFileSystem, SubTreeFileSystem as SubTreeFileSystem, _MockFileSystem as _MockFileSystem, _copy_files as _copy_files, _copy_files_selector as _copy_files_selector
from pyarrow._gcsfs import GcsFileSystem as GcsFileSystem
from pyarrow._hdfs import HadoopFileSystem as HadoopFileSystem
from pyarrow._s3fs import AwsDefaultS3RetryStrategy as AwsDefaultS3RetryStrategy, AwsStandardS3RetryStrategy as AwsStandardS3RetryStrategy, S3FileSystem as S3FileSystem, S3LogLevel as S3LogLevel, S3RetryStrategy as S3RetryStrategy, ensure_s3_finalized as ensure_s3_finalized, ensure_s3_initialized as ensure_s3_initialized, finalize_s3 as finalize_s3, initialize_s3 as initialize_s3, resolve_s3_region as resolve_s3_region
from pyarrow.util import _is_path_like as _is_path_like, _stringify_path as _stringify_path
FileStats = FileInfo
_not_imported: Incomplete

def __getattr__(name: str) -> None:
    ...

def _ensure_filesystem(filesystem: PyFileSystem | _MockFileSystem | str | SubTreeFileSystem | LocalFileSystem, *, use_mmap: bool=False) -> PyFileSystem | SubTreeFileSystem | _MockFileSystem | LocalFileSystem:
    ...

def _resolve_filesystem_and_path(path, filesystem: _MockFileSystem | PyFileSystem | str | SubTreeFileSystem | LocalFileSystem | None=None, *, memory_map: bool=False) -> tuple[PyFileSystem, str] | tuple[None, BufferedReader] | tuple[None, NativeFile] | tuple[None, BufferOutputStream] | tuple[LocalFileSystem, str] | tuple[_MockFileSystem, str] | tuple[None, None] | tuple[None, BytesIO] | tuple[None, OSFile] | tuple[SubTreeFileSystem, str] | tuple[None, Buffer] | tuple[None, BufferedWriter] | tuple[None, BufferReader]:
    ...

def copy_files(source: str, destination: str, source_filesystem: FileSystem | None=None, destination_filesystem: FileSystem | None=None, *, chunk_size: int=..., use_threads: bool=True) -> None:
    ...

class FSSpecHandler(FileSystemHandler):
    fs: Incomplete

    def __init__(self, fs) -> None:
        ...

    def __eq__(self, other):
        ...

    def __ne__(self, other):
        ...

    def get_type_name(self):
        ...

    def normalize_path(self, path: str):
        ...

    @staticmethod
    def _create_file_info(path, info):
        ...

    def get_file_info(self, paths: list[str]):
        ...

    def get_file_info_selector(self, selector: FileSelector):
        ...

    def create_dir(self, path: str, recursive: bool) -> None:
        ...

    def delete_dir(self, path: str) -> None:
        ...

    def _delete_dir_contents(self, path, missing_dir_ok) -> None:
        ...

    def delete_dir_contents(self, path: str, missing_dir_ok: bool) -> None:
        ...

    def delete_root_dir_contents(self) -> None:
        ...

    def delete_file(self, path: str) -> None:
        ...

    def move(self, src: str, dest: str) -> None:
        ...

    def copy_file(self, src: str, dest: str) -> None:
        ...

    def open_input_stream(self, path: str):
        ...

    def open_input_file(self, path: str):
        ...

    def open_output_stream(self, path: str, metadata: Mapping[Any, Any]):
        ...

    def open_append_stream(self, path: str, metadata: Mapping[Any, Any]):
        ...
