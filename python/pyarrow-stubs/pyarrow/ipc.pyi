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

import pyarrow.lib as lib
from _typeshed import Incomplete
from pyarrow.lib import Alignment as Alignment, IpcReadOptions as IpcReadOptions, IpcWriteOptions as IpcWriteOptions, Message as Message, MessageReader as MessageReader, MetadataVersion as MetadataVersion, ReadStats as ReadStats, RecordBatchReader as RecordBatchReader, WriteStats as WriteStats, _ReadPandasMixin as _ReadPandasMixin, get_record_batch_size as get_record_batch_size, get_tensor_size as get_tensor_size, read_message as read_message, read_record_batch as read_record_batch, read_schema as read_schema, read_tensor as read_tensor, write_tensor as write_tensor

class RecordBatchStreamReader(lib._RecordBatchStreamReader):
    def __init__(self, source, *, options=None, memory_pool=None) -> None: ...

_ipc_writer_class_doc: str
_ipc_file_writer_class_doc: Incomplete

class RecordBatchStreamWriter(lib._RecordBatchStreamWriter):
    __doc__: Incomplete
    def __init__(self, sink, schema, *, options=None) -> None: ...

class RecordBatchFileReader(lib._RecordBatchFileReader):
    def __init__(self, source, footer_offset=None, *, options=None, memory_pool=None) -> None: ...

class RecordBatchFileWriter(lib._RecordBatchFileWriter):
    __doc__: Incomplete
    def __init__(self, sink, schema, *, options=None, metadata=None) -> None: ...

def _get_legacy_format_default(options): ...
def _ensure_default_ipc_read_options(options): ...
def new_stream(sink, schema, *, options=None): ...
def open_stream(source, *, options=None, memory_pool=None): ...
def new_file(sink, schema, *, options=None, metadata=None): ...
def open_file(source, footer_offset=None, *, options=None, memory_pool=None): ...
def serialize_pandas(df, *, nthreads=None, preserve_index=None): ...
def deserialize_pandas(buf, *, use_threads: bool = True): ...
