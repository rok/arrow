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

# Arrow file and stream reader/writer classes, and other messaging tools

from collections.abc import Sequence
import operator
import os

import pyarrow as pa

from pyarrow.lib import (IpcReadOptions, IpcWriteOptions, ReadStats, WriteStats,  # noqa
                         Message, MessageReader,
                         RecordBatchReader, _ReadPandasMixin,
                         MetadataVersion, Alignment,
                         read_message, read_record_batch, read_schema,
                         read_tensor, write_tensor,
                         get_record_batch_size, get_tensor_size)
import pyarrow.lib as lib


class RecordBatchStreamReader(lib._RecordBatchStreamReader):
    """
    Reader for the Arrow streaming binary format.

    Parameters
    ----------
    source : bytes/buffer-like, pyarrow.NativeFile, or file-like Python object
        Either an in-memory buffer, or a readable file object.
        If you want to use memory map use MemoryMappedFile as source.
    options : pyarrow.ipc.IpcReadOptions
        Options for IPC deserialization.
        If None, default values will be used.
    memory_pool : MemoryPool, default None
        If None, default memory pool is used.
    """

    def __init__(self, source, *, options=None, memory_pool=None):
        options = _ensure_default_ipc_read_options(options)
        self._open(source, options=options, memory_pool=memory_pool)


_ipc_writer_class_doc = """\
Parameters
----------
sink : str, pyarrow.NativeFile, or file-like Python object
    Either a file path, or a writable file object.
schema : pyarrow.Schema
    The Arrow schema for data to be written to the file.
options : pyarrow.ipc.IpcWriteOptions
    Options for IPC serialization.

    If None, default values will be used: the legacy format will not
    be used unless overridden by setting the environment variable
    ARROW_PRE_0_15_IPC_FORMAT=1, and the V5 metadata version will be
    used unless overridden by setting the environment variable
    ARROW_PRE_1_0_METADATA_VERSION=1."""


_ipc_file_writer_class_doc = (
    _ipc_writer_class_doc
    + "\n"
    + """\
metadata : dict | pyarrow.KeyValueMetadata, optional
    Key/value pairs (both must be bytes-like) that will be stored
    in the file footer and are retrievable via
    pyarrow.ipc.open_file(...).metadata."""
)


class RecordBatchStreamWriter(lib._RecordBatchStreamWriter):
    __doc__ = f"""Writer for the Arrow streaming binary format

{_ipc_writer_class_doc}"""

    def __init__(self, sink, schema, *, options=None):
        options = _get_legacy_format_default(options)
        self._open(sink, schema, options=options)


class RecordBatchFileReader(lib._RecordBatchFileReader):
    """
    Class for reading Arrow record batch data from the Arrow binary file format

    Parameters
    ----------
    source : bytes/buffer-like, pyarrow.NativeFile, or file-like Python object
        Either an in-memory buffer, or a readable file object.
        If you want to use memory map use MemoryMappedFile as source.
    footer_offset : int, default None
        If the file is embedded in some larger file, this is the byte offset to
        the very end of the file data
    options : pyarrow.ipc.IpcReadOptions
        Options for IPC serialization.
        If None, default values will be used.
    memory_pool : MemoryPool, default None
        If None, default memory pool is used.
    """

    def __init__(self, source, footer_offset=None, *, options=None,
                 memory_pool=None):
        options = _ensure_default_ipc_read_options(options)
        self._open(source, footer_offset=footer_offset,
                   options=options, memory_pool=memory_pool)


class RecordBatchFileWriter(lib._RecordBatchFileWriter):

    __doc__ = f"""Writer to create the Arrow binary file format

{_ipc_file_writer_class_doc}"""

    def __init__(self, sink, schema, *, options=None, metadata=None):
        options = _get_legacy_format_default(options)
        self._open(sink, schema, options=options, metadata=metadata)


def _get_legacy_format_default(options):
    if options:
        if not isinstance(options, IpcWriteOptions):
            raise TypeError(f"expected IpcWriteOptions, got {type(options)}")
        return options

    metadata_version = MetadataVersion.V5
    use_legacy_format = \
        bool(int(os.environ.get('ARROW_PRE_0_15_IPC_FORMAT', '0')))
    if bool(int(os.environ.get('ARROW_PRE_1_0_METADATA_VERSION', '0'))):
        metadata_version = MetadataVersion.V4
    return IpcWriteOptions(use_legacy_format=use_legacy_format,
                           metadata_version=metadata_version)


def _ensure_default_ipc_read_options(options):
    if options and not isinstance(options, IpcReadOptions):
        raise TypeError(f"expected IpcReadOptions, got {type(options)}")
    return options or IpcReadOptions()


def new_stream(sink, schema, *, options=None):
    return RecordBatchStreamWriter(sink, schema,
                                   options=options)


new_stream.__doc__ = f"""\
Create an Arrow columnar IPC stream writer instance

{_ipc_writer_class_doc}

Returns
-------
writer : RecordBatchStreamWriter
    A writer for the given sink
"""


def open_stream(source, *, options=None, memory_pool=None):
    """
    Create reader for Arrow streaming format.

    Parameters
    ----------
    source : bytes/buffer-like, pyarrow.NativeFile, or file-like Python object
        Either an in-memory buffer, or a readable file object.
    options : pyarrow.ipc.IpcReadOptions
        Options for IPC serialization.
        If None, default values will be used.
    memory_pool : MemoryPool, default None
        If None, default memory pool is used.

    Returns
    -------
    reader : RecordBatchStreamReader
        A reader for the given source
    """
    return RecordBatchStreamReader(source, options=options,
                                   memory_pool=memory_pool)


def new_file(sink, schema, *, options=None, metadata=None):
    return RecordBatchFileWriter(sink, schema, options=options, metadata=metadata)


new_file.__doc__ = f"""\
Create an Arrow columnar IPC file writer instance

{_ipc_file_writer_class_doc}

Returns
-------
writer : RecordBatchFileWriter
    A writer for the given sink
"""


def open_file(source, footer_offset=None, *, options=None, memory_pool=None):
    """
    Create reader for Arrow file format.

    Parameters
    ----------
    source : bytes/buffer-like, pyarrow.NativeFile, or file-like Python object
        Either an in-memory buffer, or a readable file object.
    footer_offset : int, default None
        If the file is embedded in some larger file, this is the byte offset to
        the very end of the file data.
    options : pyarrow.ipc.IpcReadOptions
        Options for IPC serialization.
        If None, default values will be used.
    memory_pool : MemoryPool, default None
        If None, default memory pool is used.

    Returns
    -------
    reader : RecordBatchFileReader
        A reader for the given source
    """
    return RecordBatchFileReader(
        source, footer_offset=footer_offset,
        options=options, memory_pool=memory_pool)


_FILE_SUPPORTED_CODECS = {'lz4', 'zstd', 'uncompressed'}
_DEFAULT_MAX_CHUNKSIZE = 1 << 16


def _ensure_table(data, preserve_index=None):
    from pyarrow.pandas_compat import _pandas_api

    if _pandas_api.have_pandas:
        if (_pandas_api.has_sparse and
                isinstance(data, _pandas_api.pd.SparseDataFrame)):
            data = data.to_dense()

    if _pandas_api.is_data_frame(data):
        return pa.Table.from_pandas(data, preserve_index=preserve_index)
    return data


def _get_write_file_options(compression, compression_level):
    if (compression is not None and
            compression not in _FILE_SUPPORTED_CODECS):
        raise ValueError(
            f'compression="{compression}" not supported, must be one of '
            f'{_FILE_SUPPORTED_CODECS}'
        )

    if compression is None or compression == 'uncompressed':
        if compression_level is not None:
            raise pa.ArrowInvalid(
                "Codec 'uncompressed' doesn't support setting a compression "
                "level.")
        codec = None
    elif compression_level is None:
        codec = compression
    else:
        codec = pa.Codec(compression, compression_level=compression_level)

    return IpcWriteOptions(
        allow_64bit=True, compression=codec, unify_dictionaries=True)


def _normalize_max_chunksize(max_chunksize):
    if max_chunksize is None:
        return _DEFAULT_MAX_CHUNKSIZE
    if isinstance(max_chunksize, bool):
        raise TypeError("max_chunksize must be an integer")
    try:
        max_chunksize = operator.index(max_chunksize)
    except TypeError:
        raise TypeError("max_chunksize must be an integer") from None
    if max_chunksize <= 0:
        raise ValueError("max_chunksize must be greater than zero")
    return max_chunksize


def write_file(data, sink, *, compression=None, compression_level=None,
               max_chunksize=None):
    """
    Write a pandas.DataFrame or pyarrow.Table to an Arrow IPC file.

    Parameters
    ----------
    data : pandas.DataFrame or pyarrow.Table
        Data to write.
    sink : str, path-like or file-like object
        Destination path or writable file object.
    compression : str, default None
        Can be one of {"zstd", "lz4", "uncompressed"}. The default of None
        writes an uncompressed file.
    compression_level : int, default None
        Use a compression level particular to the chosen compressor. If None,
        use the default compression level.
    max_chunksize : int, default None
        Maximum size of Arrow RecordBatch chunks. Must be greater than zero.
        None uses the default of 64K.
    """
    table = _ensure_table(data)
    options = _get_write_file_options(compression, compression_level)
    max_chunksize = _normalize_max_chunksize(max_chunksize)

    try:
        with new_file(sink, table.schema, options=options) as writer:
            writer.write_table(table, max_chunksize=max_chunksize)
    except Exception:
        if isinstance(sink, (str, os.PathLike)):
            try:
                os.remove(sink)
            except OSError:
                pass
        raise


def _validate_columns(columns):
    if not isinstance(columns, Sequence):
        raise TypeError("Columns must be a sequence but, got {}"
                        .format(type(columns).__name__))

    column_types = [type(column) for column in columns]
    if all(column_type == int for column_type in column_types):
        return "indices"
    if all(column_type == str for column_type in column_types):
        return "names"

    column_type_names = [column_type.__name__
                         for column_type in column_types]
    raise TypeError("Columns must be indices or names. "
                    f"Got columns {columns} of types {column_type_names}")


def _read_ipc_file(reader, source, columns, use_threads):
    if columns is None:
        return reader.read_all()

    column_kind = _validate_columns(columns)
    if column_kind == "indices":
        column_indices = columns
    else:
        column_indices = []
        for column in columns:
            index = reader.schema.get_field_index(column)
            if index < 0:
                raise pa.ArrowInvalid(f"Field named {column} is not found")
            column_indices.append(index)

    # An empty included_fields option means all fields. Read the table before
    # selecting no columns so that the result retains the correct row count.
    if not column_indices:
        return reader.read_all().select([])

    included_fields = sorted(set(column_indices))
    options = IpcReadOptions(
        included_fields=included_fields, use_threads=use_threads)
    table = open_file(source, options=options).read_all()

    # IPC projection sorts and deduplicates field indices. Restore the exact
    # order and any repeated columns requested by the caller.
    projected_indices = {source_index: projected_index
                         for projected_index, source_index
                         in enumerate(included_fields)}
    return table.select([projected_indices[index] for index in column_indices])


def read_file(source, *, columns=None, memory_map=False, use_threads=True):
    """
    Read an Arrow IPC file as a pyarrow.Table.

    Parameters
    ----------
    source : str, path-like or file-like object
        Source path or readable file object. A file-like source must support
        seeking.
    columns : sequence, optional
        Column indices or names to read. If not provided, read all columns.
        An empty sequence reads no columns.
    memory_map : bool, default False
        Use memory mapping when opening a source path.
    use_threads : bool, default True
        Whether to parallelize reading using multiple threads.

    Returns
    -------
    pyarrow.Table
        The contents of the file.
    """
    if memory_map and isinstance(source, (str, os.PathLike)):
        source = pa.memory_map(os.fspath(source), 'r')

    options = IpcReadOptions(use_threads=use_threads)
    reader = open_file(source, options=options)
    return _read_ipc_file(reader, source, columns, use_threads)


def serialize_pandas(df, *, nthreads=None, preserve_index=None):
    """
    Serialize a pandas DataFrame into a buffer protocol compatible object.

    Parameters
    ----------
    df : pandas.DataFrame
    nthreads : int, default None
        Number of threads to use for conversion to Arrow, default all CPUs.
    preserve_index : bool, default None
        The default of None will store the index as a column, except for
        RangeIndex which is stored as metadata only. If True, always
        preserve the pandas index data as a column. If False, no index
        information is saved and the result will have a default RangeIndex.

    Returns
    -------
    buf : buffer
        An object compatible with the buffer protocol.
    """
    batch = pa.RecordBatch.from_pandas(df, nthreads=nthreads,
                                       preserve_index=preserve_index)
    sink = pa.BufferOutputStream()
    with pa.RecordBatchStreamWriter(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue()


def deserialize_pandas(buf, *, use_threads=True):
    """Deserialize a buffer protocol compatible object into a pandas DataFrame.

    Parameters
    ----------
    buf : buffer
        An object compatible with the buffer protocol.
    use_threads : bool, default True
        Whether to parallelize the conversion using multiple threads.

    Returns
    -------
    df : pandas.DataFrame
        The buffer deserialized as pandas DataFrame
    """
    buffer_reader = pa.BufferReader(buf)
    with pa.RecordBatchStreamReader(buffer_reader) as reader:
        table = reader.read_all()
    return table.to_pandas(use_threads=use_threads)
