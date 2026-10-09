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

from io import BufferedWriter, BytesIO
from pathlib import Path
from pyarrow.lib import (
    Buffer,
    BufferOutputStream,
    BufferReader,
    CompressedInputStream,
    CompressedOutputStream,
    MockOutputStream,
    NativeFile,
    OSFile,
    Schema,
)
import pyarrow
from typing import Any, IO
import pyarrow.ipc
from pyarrow.lib import __doc__ as __doc__
import pyarrow.lib as lib
from _typeshed import Incomplete
from pyarrow.lib import (
    Alignment as Alignment,
    IpcReadOptions as IpcReadOptions,
    IpcWriteOptions as IpcWriteOptions,
    Message as Message,
    MessageReader as MessageReader,
    MetadataVersion as MetadataVersion,
    ReadStats as ReadStats,
    RecordBatchReader as RecordBatchReader,
    WriteStats as WriteStats,
    _ReadPandasMixin as _ReadPandasMixin,
    get_record_batch_size as get_record_batch_size,
    get_tensor_size as get_tensor_size,
    read_message as read_message,
    read_record_batch as read_record_batch,
    read_schema as read_schema,
    read_tensor as read_tensor,
    write_tensor as write_tensor,
)

_ipc_writer_class_doc: str
_ipc_file_writer_class_doc: Incomplete

def _get_legacy_format_default(options: IpcWriteOptions | None) -> IpcWriteOptions: ...
def _ensure_default_ipc_read_options(
    options: IpcReadOptions | bool | None,
) -> IpcReadOptions: ...
def new_stream(
    sink: str | pyarrow.NativeFile | IO[Any],
    schema: pyarrow.Schema,
    *,
    options: pyarrow.ipc.IpcWriteOptions | None = None,
) -> RecordBatchStreamWriter: ...
def open_stream(
    source: BytesIO | bytes | BufferReader | Buffer,
    *,
    options: pyarrow.ipc.IpcReadOptions | None = None,
    memory_pool: pyarrow.MemoryPool | None = None,
) -> RecordBatchStreamReader: ...
def new_file(
    sink: str | pyarrow.NativeFile | IO[Any],
    schema: pyarrow.Schema,
    *,
    options: pyarrow.ipc.IpcWriteOptions | None = None,
    metadata: dict | pyarrow.KeyValueMetadata | None = None,
) -> RecordBatchFileWriter: ...
def open_file(
    source: Path | BufferReader | Buffer | BytesIO | OSFile | bytes,
    footer_offset: int | None = None,
    *,
    options: pyarrow.ipc.IpcReadOptions | None = None,
    memory_pool: pyarrow.MemoryPool | None = None,
) -> RecordBatchFileReader: ...
def serialize_pandas(
    df, *, nthreads: int | None = None, preserve_index: bool | None = None
) -> Buffer: ...
def deserialize_pandas(buf: Buffer, *, use_threads: bool = True): ...

class RecordBatchStreamReader(lib._RecordBatchStreamReader):
    def __init__(
        self,
        source: BufferReader | CompressedInputStream | Buffer | BytesIO | bytes,
        *,
        options=None,
        memory_pool=None,
    ) -> None: ...

class RecordBatchStreamWriter(lib._RecordBatchStreamWriter):
    __doc__: Incomplete

    def __init__(
        self,
        sink: BufferOutputStream
        | BufferedWriter
        | MockOutputStream
        | CompressedOutputStream
        | BytesIO,
        schema: Schema,
        *,
        options=None,
    ) -> None: ...

class RecordBatchFileReader(lib._RecordBatchFileReader):
    def __init__(
        self,
        source: Path | BufferReader | Buffer | BytesIO | OSFile | bytes,
        footer_offset=None,
        *,
        options=None,
        memory_pool=None,
    ) -> None: ...

class RecordBatchFileWriter(lib._RecordBatchFileWriter):
    __doc__: Incomplete

    def __init__(
        self,
        sink: BufferOutputStream
        | MockOutputStream
        | str
        | BytesIO
        | NativeFile
        | OSFile,
        schema: Schema,
        *,
        options=None,
        metadata=None,
    ) -> None: ...
