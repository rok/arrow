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

from pyarrow._fs import (
    FileSelector,
    LocalFileSystem,
    PyFileSystem,
    SubTreeFileSystem,
    _MockFileSystem,
)
from pyarrow.lib import RecordBatch, RecordBatchReader, Schema, Table
import os
import pyarrow
from typing import Any
from collections.abc import Callable
import pyarrow.dataset
import pyarrow.fs
from pyarrow._dataset import (
    CsvFileFormat as CsvFileFormat,
    CsvFragmentScanOptions as CsvFragmentScanOptions,
    Dataset as Dataset,
    DatasetFactory as DatasetFactory,
    DirectoryPartitioning as DirectoryPartitioning,
    FeatherFileFormat as FeatherFileFormat,
    FileFormat as FileFormat,
    FileFragment as FileFragment,
    FileSystemDataset as FileSystemDataset,
    FileSystemDatasetFactory as FileSystemDatasetFactory,
    FileSystemFactoryOptions as FileSystemFactoryOptions,
    FileWriteOptions as FileWriteOptions,
    FilenamePartitioning as FilenamePartitioning,
    Fragment as Fragment,
    FragmentScanOptions as FragmentScanOptions,
    HivePartitioning as HivePartitioning,
    InMemoryDataset as InMemoryDataset,
    IpcFileFormat as IpcFileFormat,
    IpcFileWriteOptions as IpcFileWriteOptions,
    JsonFileFormat as JsonFileFormat,
    JsonFragmentScanOptions as JsonFragmentScanOptions,
    Partitioning as Partitioning,
    PartitioningFactory as PartitioningFactory,
    Scanner as Scanner,
    TaggedRecordBatch as TaggedRecordBatch,
    UnionDataset as UnionDataset,
    UnionDatasetFactory as UnionDatasetFactory,
    WrittenFile as WrittenFile,
    _filesystemdataset_write as _filesystemdataset_write,
    get_partition_keys as get_partition_keys,
)
from pyarrow._dataset_orc import OrcFileFormat as OrcFileFormat
from pyarrow._dataset_parquet import (
    ParquetDatasetFactory as ParquetDatasetFactory,
    ParquetFactoryOptions as ParquetFactoryOptions,
    ParquetFileFormat as ParquetFileFormat,
    ParquetFileFragment as ParquetFileFragment,
    ParquetFileWriteOptions as ParquetFileWriteOptions,
    ParquetFragmentScanOptions as ParquetFragmentScanOptions,
    ParquetReadOptions as ParquetReadOptions,
    RowGroupInfo as RowGroupInfo,
)
from pyarrow._dataset_parquet_encryption import (
    ParquetDecryptionConfig as ParquetDecryptionConfig,
    ParquetEncryptionConfig as ParquetEncryptionConfig,
)
from pyarrow.compute import Expression as Expression, field as field, scalar as scalar
from pyarrow.util import (
    _is_iterable as _is_iterable,
    _is_path_like as _is_path_like,
    _stringify_path as _stringify_path,
)

_orc_available: bool
_orc_msg: str
_parquet_available: bool
_parquet_msg: str

def __getattr__(name: str) -> None: ...
def partitioning(
    schema: pyarrow.Schema | None = None,
    field_names: list[str] | None = None,
    flavor: str | None = None,
    dictionaries: str | None = None,
) -> Partitioning | PartitioningFactory: ...
def _ensure_partitioning(
    scheme: str
    | list[str]
    | PartitioningFactory
    | FilenamePartitioning
    | DirectoryPartitioning
    | int
    | HivePartitioning
    | None,
) -> (
    FilenamePartitioning
    | PartitioningFactory
    | HivePartitioning
    | DirectoryPartitioning
    | None
): ...
def _ensure_format(
    obj: str
    | OrcFileFormat
    | JsonFileFormat
    | IpcFileFormat
    | CsvFileFormat
    | ParquetFileFormat,
) -> (
    OrcFileFormat
    | JsonFileFormat
    | IpcFileFormat
    | CsvFileFormat
    | FeatherFileFormat
    | ParquetFileFormat
): ...
def _ensure_multiple_sources(
    paths: list[os.PathLike[str]], filesystem: pyarrow.fs.FileSystem | str | None = None
) -> (
    tuple[_MockFileSystem, list[str]]
    | tuple[LocalFileSystem, list[Any]]
    | tuple[LocalFileSystem, list[str]]
    | tuple[SubTreeFileSystem, list[str]]
): ...
def _ensure_single_source(
    path: os.PathLike[str], filesystem: pyarrow.fs.FileSystem | str | None = None
) -> (
    tuple[PyFileSystem, FileSelector]
    | tuple[_MockFileSystem, FileSelector]
    | tuple[LocalFileSystem, list[str]]
    | tuple[LocalFileSystem, FileSelector]
    | tuple[SubTreeFileSystem, FileSelector]
    | tuple[SubTreeFileSystem, list[str]]
): ...
def _filesystem_dataset(
    source,
    schema: Schema | None = None,
    filesystem: _MockFileSystem
    | PyFileSystem
    | str
    | SubTreeFileSystem
    | LocalFileSystem
    | None = None,
    partitioning: FilenamePartitioning
    | str
    | list[str]
    | PartitioningFactory
    | HivePartitioning
    | DirectoryPartitioning
    | int
    | None = None,
    format: str
    | OrcFileFormat
    | JsonFileFormat
    | IpcFileFormat
    | CsvFileFormat
    | ParquetFileFormat
    | None = None,
    partition_base_dir=None,
    exclude_invalid_files=None,
    selector_ignore_prefixes: list[str] | None = None,
) -> FileSystemDataset: ...
def _in_memory_dataset(
    source: Table | list[RecordBatch] | RecordBatch | list[Table] | RecordBatchReader,
    schema=None,
    **kwargs,
) -> InMemoryDataset: ...
def _union_dataset(
    children: list[FileSystemDataset]
    | tuple[InMemoryDataset, InMemoryDataset]
    | tuple[FileSystemDataset, FileSystemDataset],
    schema: Schema | None = None,
    **kwargs,
) -> UnionDataset: ...
def parquet_dataset(
    metadata_path: str,
    schema: pyarrow.Schema | None = None,
    filesystem=None,
    format: ParquetFileFormat | None = None,
    partitioning: Partitioning | PartitioningFactory | str | list[str] | None = None,
    partition_base_dir: str | None = None,
) -> FileSystemDataset: ...
def dataset(
    source,
    schema: pyarrow.Schema | None = None,
    format: FileFormat | str | None = None,
    filesystem: _MockFileSystem
    | PyFileSystem
    | str
    | SubTreeFileSystem
    | LocalFileSystem
    | None = None,
    partitioning: Partitioning | PartitioningFactory | str | list[str] | None = None,
    partition_base_dir: str | None = None,
    exclude_invalid_files: bool | None = None,
    ignore_prefixes: list | None = None,
) -> Dataset: ...
def _ensure_write_partitioning(
    part: list[str]
    | FilenamePartitioning
    | DirectoryPartitioning
    | HivePartitioning
    | None,
    schema: Schema,
    flavor: str | None,
) -> DirectoryPartitioning | HivePartitioning | FilenamePartitioning: ...
def write_dataset(
    data: Table | FileSystemDataset | Scanner | list[Table] | InMemoryDataset,
    base_dir: str,
    *,
    basename_template: str | None = None,
    format: FileFormat | str | None = None,
    partitioning: Partitioning | list[str] | None = None,
    partitioning_flavor: str | None = None,
    schema: pyarrow.Schema | None = None,
    filesystem: pyarrow.fs.FileSystem | None = None,
    file_options: pyarrow.dataset.FileWriteOptions | None = None,
    use_threads: bool = True,
    preserve_order: bool = False,
    max_partitions: int | None = None,
    max_open_files: int | None = None,
    max_rows_per_file: int | None = None,
    min_rows_per_group: int | None = None,
    max_rows_per_group: int | None = None,
    file_visitor: Callable[..., Any] | None = None,
    existing_data_behavior: str = "error",
    create_dir: bool = True,
) -> None: ...
