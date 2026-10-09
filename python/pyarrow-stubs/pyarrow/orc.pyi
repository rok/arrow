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

from io import BytesIO
from pathlib import Path
from pyarrow.lib import BufferReader, NativeFile, Schema
import pyarrow
from typing import Any, IO
import pyarrow.fs
from pyarrow.lib import __doc__ as __doc__
from _typeshed import Incomplete
from pyarrow.fs import _resolve_filesystem_and_path as _resolve_filesystem_and_path
from pyarrow.lib import Table as Table

_orc_writer_args_docs: str

def read_table(
    source: str | pyarrow.NativeFile | IO[Any],
    columns: list | None = None,
    filesystem: pyarrow.fs.FileSystem | None = None,
) -> Table: ...
def write_table(
    table: Table,
    where,
    *,
    file_version: str = "0.12",
    batch_size: int = 1024,
    stripe_size: int = ...,
    compression: str = "uncompressed",
    compression_block_size: int = 65536,
    compression_strategy: str = "speed",
    row_index_stride: int = 10000,
    padding_tolerance: float = 0.0,
    dictionary_key_size_threshold: float = 0.0,
    bloom_filter_columns=None,
    bloom_filter_fpp: float = 0.05,
) -> None: ...

class ORCFile:
    reader: Incomplete

    def __init__(
        self, source: Path | BufferReader | str | BytesIO | NativeFile
    ) -> None: ...
    @property
    def metadata(self): ...
    @property
    def schema(self) -> Schema: ...
    @property
    def nrows(self) -> int: ...
    @property
    def nstripes(self) -> int: ...
    @property
    def file_version(self) -> str: ...
    @property
    def software_version(self): ...
    @property
    def compression(self) -> str: ...
    @property
    def compression_size(self) -> int: ...
    @property
    def writer(self): ...
    @property
    def writer_version(self): ...
    @property
    def row_index_stride(self) -> int: ...
    @property
    def nstripe_statistics(self): ...
    @property
    def content_length(self): ...
    @property
    def stripe_statistics_length(self): ...
    @property
    def file_footer_length(self): ...
    @property
    def file_postscript_length(self): ...
    @property
    def file_length(self): ...
    def _select_names(
        self, columns: list[int] | list[str] | None = None
    ) -> list[str]: ...
    def read_stripe(
        self, n: int, columns: list | None = None
    ) -> pyarrow.RecordBatch: ...
    def read(self, columns: list | None = None) -> Table: ...

class ORCWriter:
    __doc__: Incomplete
    is_open: bool
    writer: Incomplete

    def __init__(
        self,
        where,
        *,
        file_version: str = "0.12",
        batch_size: int = 1024,
        stripe_size=...,
        compression: str = "uncompressed",
        compression_block_size: int = 65536,
        compression_strategy: str = "speed",
        row_index_stride: int = 10000,
        padding_tolerance: float = 0.0,
        dictionary_key_size_threshold: float = 0.0,
        bloom_filter_columns=None,
        bloom_filter_fpp: float = 0.05,
    ) -> None: ...
    def __del__(self) -> None: ...
    def __enter__(self): ...
    def __exit__(self, *args, **kwargs) -> None: ...
    def write(self, table: Table) -> None: ...
    def close(self) -> None: ...
