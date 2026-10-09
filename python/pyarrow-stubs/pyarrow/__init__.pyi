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

import os
import re
import pyarrow
from typing import Any, Callable, IO, Iterable, Iterator, Literal, Mapping, NamedTuple, Self, Sequence
from _typeshed import Incomplete
from pyarrow.ipc import Message as Message, MessageReader as MessageReader, MetadataVersion as MetadataVersion, RecordBatchFileReader as RecordBatchFileReader, RecordBatchFileWriter as RecordBatchFileWriter, RecordBatchStreamReader as RecordBatchStreamReader, RecordBatchStreamWriter as RecordBatchStreamWriter, deserialize_pandas as deserialize_pandas, serialize_pandas as serialize_pandas
from pyarrow.lib import Array as Array, ArrowCancelled as ArrowCancelled, ArrowCapacityError as ArrowCapacityError, ArrowException as ArrowException, ArrowIOError as ArrowIOError, ArrowIndexError as ArrowIndexError, ArrowInvalid as ArrowInvalid, ArrowKeyError as ArrowKeyError, ArrowMemoryError as ArrowMemoryError, ArrowNotImplementedError as ArrowNotImplementedError, ArrowSerializationError as ArrowSerializationError, ArrowTypeError as ArrowTypeError, BaseExtensionType as BaseExtensionType, BinaryArray as BinaryArray, BinaryScalar as BinaryScalar, BinaryViewArray as BinaryViewArray, BinaryViewScalar as BinaryViewScalar, Bool8Array as Bool8Array, Bool8Scalar as Bool8Scalar, Bool8Type as Bool8Type, BooleanArray as BooleanArray, BooleanScalar as BooleanScalar, Buffer as Buffer, BufferOutputStream as BufferOutputStream, BufferReader as BufferReader, BufferedInputStream as BufferedInputStream, BufferedOutputStream as BufferedOutputStream, BuildInfo as BuildInfo, CacheOptions as CacheOptions, ChunkedArray as ChunkedArray, Codec as Codec, CompressedInputStream as CompressedInputStream, CompressedOutputStream as CompressedOutputStream, CppBuildInfo as CppBuildInfo, DataType as DataType, Date32Array as Date32Array, Date32Scalar as Date32Scalar, Date64Array as Date64Array, Date64Scalar as Date64Scalar, Decimal128Array as Decimal128Array, Decimal128Scalar as Decimal128Scalar, Decimal128Type as Decimal128Type, Decimal256Array as Decimal256Array, Decimal256Scalar as Decimal256Scalar, Decimal256Type as Decimal256Type, Decimal32Array as Decimal32Array, Decimal32Scalar as Decimal32Scalar, Decimal32Type as Decimal32Type, Decimal64Array as Decimal64Array, Decimal64Scalar as Decimal64Scalar, Decimal64Type as Decimal64Type, DenseUnionType as DenseUnionType, Device as Device, DeviceAllocationType as DeviceAllocationType, DictionaryArray as DictionaryArray, DictionaryMemo as DictionaryMemo, DictionaryScalar as DictionaryScalar, DictionaryType as DictionaryType, DoubleArray as DoubleArray, DoubleScalar as DoubleScalar, DurationArray as DurationArray, DurationScalar as DurationScalar, DurationType as DurationType, ExtensionArray as ExtensionArray, ExtensionScalar as ExtensionScalar, ExtensionType as ExtensionType, Field as Field, FixedShapeTensorArray as FixedShapeTensorArray, FixedShapeTensorScalar as FixedShapeTensorScalar, FixedShapeTensorType as FixedShapeTensorType, FixedSizeBinaryArray as FixedSizeBinaryArray, FixedSizeBinaryScalar as FixedSizeBinaryScalar, FixedSizeBinaryType as FixedSizeBinaryType, FixedSizeBufferWriter as FixedSizeBufferWriter, FixedSizeListArray as FixedSizeListArray, FixedSizeListScalar as FixedSizeListScalar, FixedSizeListType as FixedSizeListType, FloatArray as FloatArray, FloatScalar as FloatScalar, FloatingPointArray as FloatingPointArray, HalfFloatArray as HalfFloatArray, HalfFloatScalar as HalfFloatScalar, Int16Array as Int16Array, Int16Scalar as Int16Scalar, Int32Array as Int32Array, Int32Scalar as Int32Scalar, Int64Array as Int64Array, Int64Scalar as Int64Scalar, Int8Array as Int8Array, Int8Scalar as Int8Scalar, IntegerArray as IntegerArray, JsonArray as JsonArray, JsonScalar as JsonScalar, JsonType as JsonType, KeyValueMetadata as KeyValueMetadata, LargeBinaryArray as LargeBinaryArray, LargeBinaryScalar as LargeBinaryScalar, LargeListArray as LargeListArray, LargeListScalar as LargeListScalar, LargeListType as LargeListType, LargeListViewArray as LargeListViewArray, LargeListViewScalar as LargeListViewScalar, LargeListViewType as LargeListViewType, LargeStringArray as LargeStringArray, LargeStringScalar as LargeStringScalar, ListArray as ListArray, ListScalar as ListScalar, ListType as ListType, ListViewArray as ListViewArray, ListViewScalar as ListViewScalar, ListViewType as ListViewType, LoggingMemoryPool as LoggingMemoryPool, MapArray as MapArray, MapScalar as MapScalar, MapType as MapType, MemoryManager as MemoryManager, MemoryMappedFile as MemoryMappedFile, MemoryPool as MemoryPool, MockOutputStream as MockOutputStream, MonthDayNano as MonthDayNano, MonthDayNanoIntervalArray as MonthDayNanoIntervalArray, MonthDayNanoIntervalScalar as MonthDayNanoIntervalScalar, NA as NA, NativeFile as NativeFile, NullArray as NullArray, NullScalar as NullScalar, NumericArray as NumericArray, OSFile as OSFile, OpaqueArray as OpaqueArray, OpaqueScalar as OpaqueScalar, OpaqueType as OpaqueType, ProxyMemoryPool as ProxyMemoryPool, PythonFile as PythonFile, RecordBatch as RecordBatch, RecordBatchReader as RecordBatchReader, ResizableBuffer as ResizableBuffer, RunEndEncodedArray as RunEndEncodedArray, RunEndEncodedScalar as RunEndEncodedScalar, RunEndEncodedType as RunEndEncodedType, RuntimeInfo as RuntimeInfo, Scalar as Scalar, Schema as Schema, SparseCOOTensor as SparseCOOTensor, SparseCSCMatrix as SparseCSCMatrix, SparseCSFTensor as SparseCSFTensor, SparseCSRMatrix as SparseCSRMatrix, SparseUnionType as SparseUnionType, StringArray as StringArray, StringScalar as StringScalar, StringViewArray as StringViewArray, StringViewScalar as StringViewScalar, StructArray as StructArray, StructScalar as StructScalar, StructType as StructType, Table as Table, TableGroupBy as TableGroupBy, Tensor as Tensor, Time32Array as Time32Array, Time32Scalar as Time32Scalar, Time32Type as Time32Type, Time64Array as Time64Array, Time64Scalar as Time64Scalar, Time64Type as Time64Type, TimestampArray as TimestampArray, TimestampScalar as TimestampScalar, TimestampType as TimestampType, TransformInputStream as TransformInputStream, UInt16Array as UInt16Array, UInt16Scalar as UInt16Scalar, UInt32Array as UInt32Array, UInt32Scalar as UInt32Scalar, UInt64Array as UInt64Array, UInt64Scalar as UInt64Scalar, UInt8Array as UInt8Array, UInt8Scalar as UInt8Scalar, UnionArray as UnionArray, UnionScalar as UnionScalar, UnionType as UnionType, UnknownExtensionType as UnknownExtensionType, UuidArray as UuidArray, UuidScalar as UuidScalar, UuidType as UuidType, VersionInfo as VersionInfo, allocate_buffer as allocate_buffer, arange as arange, array as array, binary as binary, binary_view as binary_view, bool8 as bool8, bool_ as bool_, build_info as build_info, chunked_array as chunked_array, compress as compress, concat_arrays as concat_arrays, concat_batches as concat_batches, concat_tables as concat_tables, cpp_build_info as cpp_build_info, cpp_version as cpp_version, cpp_version_info as cpp_version_info, cpu_count as cpu_count, create_memory_map as create_memory_map, date32 as date32, date64 as date64, decimal128 as decimal128, decimal256 as decimal256, decimal32 as decimal32, decimal64 as decimal64, decompress as decompress, default_cpu_memory_manager as default_cpu_memory_manager, default_memory_pool as default_memory_pool, dense_union as dense_union, dictionary as dictionary, duration as duration, enable_signal_handlers as enable_signal_handlers, field as field, fixed_shape_tensor as fixed_shape_tensor, float16 as float16, float32 as float32, float64 as float64, foreign_buffer as foreign_buffer, from_numpy_dtype as from_numpy_dtype, infer_type as infer_type, input_stream as input_stream, int16 as int16, int32 as int32, int64 as int64, int8 as int8, io_thread_count as io_thread_count, is_opentelemetry_enabled as is_opentelemetry_enabled, jemalloc_memory_pool as jemalloc_memory_pool, jemalloc_set_decay_ms as jemalloc_set_decay_ms, json_ as json_, large_binary as large_binary, large_list as large_list, large_list_view as large_list_view, large_string as large_string, large_utf8 as large_utf8, list_ as list_, list_view as list_view, log_memory_allocations as log_memory_allocations, logging_memory_pool as logging_memory_pool, map_ as map_, memory_map as memory_map, mimalloc_memory_pool as mimalloc_memory_pool, month_day_nano_interval as month_day_nano_interval, null as null, nulls as nulls, opaque as opaque, output_stream as output_stream, proxy_memory_pool as proxy_memory_pool, py_buffer as py_buffer, record_batch as record_batch, register_extension_type as register_extension_type, repeat as repeat, run_end_encoded as run_end_encoded, runtime_info as runtime_info, scalar as scalar, schema as schema, set_cpu_count as set_cpu_count, set_io_thread_count as set_io_thread_count, set_memory_pool as set_memory_pool, set_timezone_db_path as set_timezone_db_path, sparse_union as sparse_union, string as string, string_view as string_view, struct as struct, supported_memory_backends as supported_memory_backends, system_memory_pool as system_memory_pool, table as table, time32 as time32, time64 as time64, timestamp as timestamp, total_allocated_bytes as total_allocated_bytes, transcoding_input_stream as transcoding_input_stream, type_for_alias as type_for_alias, uint16 as uint16, uint32 as uint32, uint64 as uint64, uint8 as uint8, unify_schemas as unify_schemas, union as union, unregister_extension_type as unregister_extension_type, utf8 as utf8, uuid as uuid
from pyarrow.util import _deprecate_api as _deprecate_api, _deprecate_class as _deprecate_class
from pyarrow.lib import _NULL as NULL

def parse_git(root, **kwargs):
    ...

def show_versions() -> None:
    ...

def _module_is_available(module):
    ...

def _filesystem_is_available(fs):
    ...

def have_libhdfs():
    ...

def show_info() -> None:
    ...

def get_include():
    ...

def _get_pkg_config_executable():
    ...

def _has_pkg_config(pkgname):
    ...

def _read_pkg_config_variable(pkgname, cli_args):
    ...

def get_libraries():
    ...

def create_library_symlinks():
    ...

def get_library_dirs():
    ...
