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

from io import IOBase

import pandas as pd
import pyarrow.lib as lib

from pyarrow.lib import (
    IpcReadOptions,
    IpcWriteOptions,
    Message,
    MessageReader,
    MetadataVersion,
    ReadStats,
    RecordBatchReader,
    WriteStats,
    _ReadPandasMixin,
    get_record_batch_size,
    get_tensor_size,
    read_message,
    read_record_batch,
    read_schema,
    read_tensor,
    write_tensor,
)

class RecordBatchStreamReader(lib._RecordBatchStreamReader):
    def __init__(
        self,
        source: bytes | lib.Buffer | lib.NativeFile | IOBase,
        *,
        options: IpcReadOptions | None = None,
        memory_pool: lib.MemoryPool | None = None,
    ) -> None: ...

class RecordBatchStreamWriter(lib._RecordBatchStreamWriter):
    def __init__(
        self,
        sink: str | lib.NativeFile | IOBase,
        schema: lib.Schema,
        *,
        use_legacy_format: bool | None = None,
        options: IpcWriteOptions | None = None,
    ) -> None: ...

class RecordBatchFileReader(lib._RecordBatchFileReader):
    def __init__(
        self,
        source: bytes | lib.Buffer | lib.NativeFile | IOBase,
        footer_offset: int | None = None,
        *,
        options: IpcReadOptions | None,
        memory_pool: lib.MemoryPool | None = None,
    ) -> None: ...

class RecordBatchFileWriter(lib._RecordBatchFileWriter):
    def __init__(
        self,
        sink: str | lib.NativeFile | IOBase,
        schema: lib.Schema,
        *,
        use_legacy_format: bool | None = None,
        options: IpcWriteOptions | None = None,
    ) -> None: ...

def new_stream(
    sink: str | lib.NativeFile | IOBase,
    schema: lib.Schema,
    *,
    use_legacy_format: bool | None = None,
    options: IpcWriteOptions | None = None,
) -> RecordBatchStreamWriter: ...
def open_stream(
    source: bytes | lib.Buffer | lib.NativeFile | IOBase,
    *,
    options: IpcReadOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> RecordBatchStreamReader: ...
def new_file(
    sink: str | lib.NativeFile | IOBase,
    schema: lib.Schema,
    *,
    use_legacy_format: bool | None = None,
    options: IpcWriteOptions | None = None,
) -> RecordBatchFileWriter: ...
def open_file(
    source: bytes | lib.Buffer | lib.NativeFile | IOBase,
    footer_offset: int | None = None,
    *,
    options: IpcReadOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> RecordBatchFileReader: ...
def serialize_pandas(
    df: pd.DataFrame, *, nthreads: int | None = None, preserve_index: bool | None = None
) -> lib.Buffer: ...
def deserialize_pandas(buf: lib.Buffer, *, use_threads: bool = True) -> pd.DataFrame: ...

__all__ = [
    "IpcReadOptions",
    "IpcWriteOptions",
    "Message",
    "MessageReader",
    "MetadataVersion",
    "ReadStats",
    "RecordBatchReader",
    "WriteStats",
    "_ReadPandasMixin",
    "get_record_batch_size",
    "get_tensor_size",
    "read_message",
    "read_record_batch",
    "read_schema",
    "read_tensor",
    "write_tensor",
    "RecordBatchStreamReader",
    "RecordBatchStreamWriter",
    "RecordBatchFileReader",
    "RecordBatchFileWriter",
    "new_stream",
    "open_stream",
    "new_file",
    "open_file",
    "serialize_pandas",
    "deserialize_pandas",
]
