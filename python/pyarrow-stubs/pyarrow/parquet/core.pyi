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
import pyarrow.compute
import pyarrow.dataset
import pyarrow.fs
from pyarrow.lib import __doc__ as __doc__
import types
from _typeshed import Incomplete
from pyarrow._parquet import ColumnChunkMetaData as ColumnChunkMetaData, ColumnSchema as ColumnSchema, FileDecryptionProperties as FileDecryptionProperties, FileEncryptionProperties as FileEncryptionProperties, FileMetaData as FileMetaData, ParquetLogicalType as ParquetLogicalType, ParquetReader as ParquetReader, ParquetSchema as ParquetSchema, RowGroupMetaData as RowGroupMetaData, SortingColumn as SortingColumn, Statistics as Statistics
__all__ = ['ParquetReader', 'Statistics', 'FileMetaData', 'RowGroupMetaData', 'ColumnChunkMetaData', 'ParquetSchema', 'ColumnSchema', 'ParquetLogicalType', 'FileEncryptionProperties', 'FileDecryptionProperties', 'SortingColumn', 'filters_to_expression', '_filters_to_expression', 'ParquetFile', 'ParquetWriter', 'ParquetDataset', 'read_table', 'read_pandas', 'write_table', 'write_to_dataset', 'write_metadata', 'read_metadata', 'read_schema']

def filters_to_expression(filters: list[tuple] | list[list[tuple]]) -> pyarrow.compute.Expression:
    ...
_filters_to_expression: Incomplete

def read_table(source: str | list[str] | pyarrow.NativeFile | IO[Any], *, columns: list | None=None, use_threads: bool=True, schema: pyarrow.Schema | None=None, use_pandas_metadata: bool=False, read_dictionary: list | None=None, binary_type: pyarrow.DataType | None=None, list_type=None, memory_map: bool=False, buffer_size: int=0, partitioning: str='hive', filesystem: pyarrow.fs.FileSystem | None=None, filters: pyarrow.compute.Expression | list[tuple] | list[list[tuple]] | None=None, ignore_prefixes: list | None=None, pre_buffer: bool=True, coerce_int96_timestamp_unit: str | None=None, decryption_properties: FileDecryptionProperties | None=None, thrift_string_size_limit: int | None=None, thrift_container_size_limit: int | None=None, schema_depth_limit: int | None=None, page_checksum_verification: bool=False, arrow_extensions_enabled: bool=True) -> pyarrow.Table:
    ...

def read_pandas(source: str | list[str] | pyarrow.NativeFile | IO[Any], columns: list | None=None, **kwargs) -> pyarrow.Table:
    ...

def write_table(table: pyarrow.Table, where: str | pyarrow.NativeFile, row_group_size: int | None=None, version: str='2.6', use_dictionary: bool=True, compression: str='snappy', write_statistics: bool=True, use_deprecated_int96_timestamps: bool | None=None, coerce_timestamps: str | None=None, allow_truncated_timestamps: bool=False, data_page_size: int | None=None, flavor: Literal['spark'] | None=None, filesystem: pyarrow.fs.FileSystem | None=None, compression_level: int | dict | None=None, use_byte_stream_split: bool=False, column_encoding: str | dict | None=None, data_page_version: str='1.0', use_compliant_nested_type: bool=True, encryption_properties: FileEncryptionProperties | None=None, write_batch_size: int | None=None, dictionary_pagesize_limit: int | None=None, store_schema: bool=True, write_page_index: bool=False, write_page_checksum: bool=False, sorting_columns: Sequence[SortingColumn] | None=None, store_decimal_as_integer: bool=False, write_time_adjusted_to_utc: bool=False, max_rows_per_page: int | None=None, bloom_filter_options: dict | None=None, use_content_defined_chunking: bool=False, **kwargs) -> None:
    ...

def write_to_dataset(table: pyarrow.Table, root_path, partition_cols: list | None=None, filesystem: pyarrow.fs.FileSystem | None=None, schema: pyarrow.Schema | None=None, partitioning: pyarrow.dataset.Partitioning | list[str] | None=None, basename_template: str | None=None, use_threads: bool | None=None, file_visitor: Callable[..., Any] | None=None, existing_data_behavior: Literal['overwrite_or_ignore'] | Literal['error'] | Literal['delete_matching'] | None=None, **kwargs) -> None:
    ...

def write_metadata(schema: pyarrow.Schema, where: str | pyarrow.NativeFile, metadata_collector: list | None=None, filesystem: pyarrow.fs.FileSystem | None=None, **kwargs) -> None:
    ...

def read_metadata(where: str | IO[Any], memory_map: bool=False, decryption_properties: FileDecryptionProperties | None=None, filesystem: pyarrow.fs.FileSystem | None=None, arrow_extensions_enabled: bool=True) -> FileMetaData:
    ...

def read_schema(where: str | IO[Any], memory_map: bool=False, decryption_properties: FileDecryptionProperties | None=None, filesystem: pyarrow.fs.FileSystem | None=None, arrow_extensions_enabled: bool=True) -> pyarrow.Schema:
    ...
EXCLUDED_PARQUET_PATHS: set[str]

class ParquetFile:
    _close_source: Incomplete
    reader: Incomplete
    common_metadata: Incomplete
    _nested_paths_by_prefix: Incomplete

    def __init__(self, source, *, metadata=None, common_metadata=None, read_dictionary=None, binary_type=None, list_type=None, memory_map: bool=False, buffer_size: int=0, pre_buffer: bool=True, coerce_int96_timestamp_unit=None, decryption_properties=None, thrift_string_size_limit=None, thrift_container_size_limit=None, schema_depth_limit=None, filesystem=None, page_checksum_verification: bool=False, arrow_extensions_enabled: bool=True) -> None:
        ...

    def __enter__(self):
        ...

    def __exit__(self, *args, **kwargs) -> None:
        ...

    def _build_nested_paths(self):
        ...

    @property
    def metadata(self):
        ...

    @property
    def schema(self):
        ...

    @property
    def schema_arrow(self):
        ...

    @property
    def num_row_groups(self):
        ...

    def close(self, force: bool=False):
        ...

    @property
    def closed(self) -> bool:
        ...

    def read_row_group(self, i: int, columns: list | None=None, use_threads: bool=True, use_pandas_metadata: bool=False):
        ...

    def read_row_groups(self, row_groups: list, columns: list | None=None, use_threads: bool=True, use_pandas_metadata: bool=False):
        ...

    def iter_batches(self, batch_size: int=65536, row_groups: list | None=None, columns: list | None=None, use_threads: bool=True, use_pandas_metadata: bool=False):
        ...

    def read(self, columns: list | None=None, use_threads: bool=True, use_pandas_metadata: bool=False):
        ...

    def scan_contents(self, columns: list[int] | None=None, batch_size: int=65536) -> int:
        ...

    def _get_column_indices(self, column_names, use_pandas_metadata: bool=False):
        ...

class ParquetWriter:
    __doc__: Incomplete
    flavor: Incomplete
    schema_changed: bool
    schema: Incomplete
    where: Incomplete
    file_handle: Incomplete
    _metadata_collector: Incomplete
    writer: Incomplete
    is_open: bool

    def __init__(self, where, schema, filesystem=None, flavor=None, version: str='2.6', use_dictionary: bool=True, compression: str='snappy', write_statistics: bool=True, use_deprecated_int96_timestamps=None, compression_level=None, use_byte_stream_split: bool=False, column_encoding=None, writer_engine_version=None, data_page_version: str='1.0', use_compliant_nested_type: bool=True, encryption_properties=None, write_batch_size=None, dictionary_pagesize_limit=None, store_schema: bool=True, write_page_index: bool=False, write_page_checksum: bool=False, sorting_columns=None, store_decimal_as_integer: bool=False, write_time_adjusted_to_utc: bool=False, max_rows_per_page=None, bloom_filter_options=None, use_content_defined_chunking: bool=False, **options) -> None:
        ...

    def __del__(self) -> None:
        ...

    def __enter__(self):
        ...

    def __exit__(self, *args, **kwargs):
        ...

    def write(self, table_or_batch, row_group_size: int | None=None) -> None:
        ...

    def write_batch(self, batch: pyarrow.RecordBatch, row_group_size: int | None=None) -> None:
        ...

    def write_table(self, table: pyarrow.Table, row_group_size: int | None=None) -> None:
        ...

    def close(self) -> None:
        ...

    def add_key_value_metadata(self, key_value_metadata: dict) -> None:
        ...

class ParquetDataset:
    __doc__: Incomplete
    _filter_expression: Incomplete
    _base_dir: Incomplete
    _dataset: Incomplete

    def __init__(self, path_or_paths, filesystem=None, schema=None, *, filters=None, read_dictionary=None, binary_type=None, list_type=None, memory_map: bool=False, buffer_size=None, partitioning: str='hive', ignore_prefixes=None, pre_buffer: bool=True, coerce_int96_timestamp_unit=None, decryption_properties=None, thrift_string_size_limit=None, thrift_container_size_limit=None, schema_depth_limit=None, page_checksum_verification: bool=False, arrow_extensions_enabled: bool=True) -> None:
        ...

    def equals(self, other):
        ...

    def __eq__(self, other):
        ...

    @property
    def schema(self):
        ...

    def read(self, columns: list[str] | None=None, use_threads: bool=True, use_pandas_metadata: bool=False) -> pyarrow.Table:
        ...

    def _get_common_pandas_metadata(self):
        ...

    def read_pandas(self, **kwargs):
        ...

    @property
    def fragments(self):
        ...

    @property
    def files(self):
        ...

    @property
    def filesystem(self):
        ...

    @property
    def partitioning(self):
        ...
