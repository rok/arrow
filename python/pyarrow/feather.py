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


from collections.abc import Sequence
import os
import warnings

import pyarrow as pa
from pyarrow import _feather
import pyarrow.ipc as ipc
from pyarrow.lib import Codec, concat_tables
from pyarrow._feather import FeatherError  # noqa: F401


class FeatherDataset:
    """
    Encapsulates details of reading a list of Feather files.

    Parameters
    ----------
    path_or_paths : List[str]
        A list of file names
    validate_schema : bool, default True
        Check that individual file schemas are all the same / compatible
    """

    def __init__(self, path_or_paths, validate_schema=True):
        self.paths = path_or_paths
        self.validate_schema = validate_schema

    def read_table(self, columns=None):
        """
        Read multiple feather files as a single pyarrow.Table

        Parameters
        ----------
        columns : List[str]
            Names of columns to read from the file

        Returns
        -------
        pyarrow.Table
            Content of the file as a table (of columns)
        """
        _fil = _read_table_internal(self.paths[0], columns=columns)
        self._tables = [_fil]
        self.schema = _fil.schema

        for path in self.paths[1:]:
            table = _read_table_internal(path, columns=columns)
            if self.validate_schema:
                self.validate_schemas(path, table)
            self._tables.append(table)
        return concat_tables(self._tables)

    def validate_schemas(self, piece, table):
        if not self.schema.equals(table.schema):
            raise ValueError(f'Schema in {piece} was different. \n'
                             f'{self.schema}\n\nvs\n\n{table.schema}')

    def read_pandas(self, columns=None, use_threads=True):
        """
        Read multiple Feather files as a single pandas DataFrame

        Parameters
        ----------
        columns : List[str]
            Names of columns to read from the file
        use_threads : bool, default True
            Use multiple threads when converting to pandas

        Returns
        -------
        pandas.DataFrame
            Content of the file as a pandas DataFrame (of columns)
        """
        return self.read_table(columns=columns).to_pandas(
            use_threads=use_threads)


def check_chunked_overflow(name, col):
    if col.num_chunks == 1:
        return

    if col.type in (pa.binary(), pa.string()):
        raise ValueError(f"Column '{name}' exceeds 2GB maximum capacity of "
                         "a Feather binary column. This restriction may be "
                         "lifted in the future")
    # TODO(wesm): Not sure when else this might be reached
    raise ValueError(
        f"Column '{name}' of type {col.type} was chunked on conversion "
        "to Arrow and cannot be currently written to Feather format"
    )


_FEATHER_V1_MAGIC = b'FEA1'


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


def _read_magic(source):
    """Read file magic without changing a file-like source's position."""
    if isinstance(source, (str, os.PathLike)):
        with pa.OSFile(os.fspath(source), 'rb') as file:
            return file.read_at(len(_FEATHER_V1_MAGIC), 0)

    if hasattr(source, 'read_at'):
        return bytes(source.read_at(len(_FEATHER_V1_MAGIC), 0))

    try:
        return memoryview(source)[:len(_FEATHER_V1_MAGIC)].tobytes()
    except TypeError:
        pass

    if all(hasattr(source, method) for method in ('read', 'seek', 'tell')):
        position = source.tell()
        try:
            source.seek(0)
            return bytes(source.read(len(_FEATHER_V1_MAGIC)))
        finally:
            source.seek(position)

    return None


def _write_feather_v1(data, dest, compression, compression_level,
                      chunksize):
    table = ipc._ensure_table(data, preserve_index=False)
    if table is not data:
        # Feather V1 does not support chunked columns created during pandas
        # conversion.
        for i, name in enumerate(table.schema.names):
            check_chunked_overflow(name, table[i])

    if len(table.column_names) > len(set(table.column_names)):
        raise ValueError("cannot serialize duplicate column names")
    if compression is not None:
        raise ValueError("Feather V1 files do not support compression option")
    if chunksize is not None:
        raise ValueError("Feather V1 files do not support chunksize option")

    try:
        _feather.write_feather(
            table, dest, compression_level=compression_level, version=1)
    except Exception:
        if isinstance(dest, (str, os.PathLike)):
            try:
                os.remove(dest)
            except OSError:
                pass
        raise


def write_feather(df, dest, compression=None, compression_level=None,
                  chunksize=None, version=2):
    """
    Write a pandas.DataFrame to Feather format.

    Parameters
    ----------
    df : pandas.DataFrame or pyarrow.Table
        Data to write out as Feather format.
    dest : str
        Local destination path.
    compression : string, default None
        Can be one of {"zstd", "lz4", "uncompressed"}. The default of None uses
        LZ4 for V2 files if it is available, otherwise uncompressed.
    compression_level : int, default None
        Use a compression level particular to the chosen compressor. If None
        use the default compression level
    chunksize : int, default None
        For V2 files, the internal maximum size of Arrow RecordBatch chunks
        when writing the Arrow IPC file format. None means use the default,
        which is currently 64K
    version : int, default 2
        Feather file version. Version 2 is the current. Version 1 is the more
        limited legacy format.

        .. deprecated:: 25.0.0
           Writing Feather V1 files is deprecated. Use the default
           ``version=2`` to write Arrow IPC files instead.
    """
    if version not in (1, 2):
        raise ValueError("Version value should either be 1 or 2")

    if version == 1:
        warnings.warn(
            "Feather V1 writing is deprecated as of 25.0.0. Use version=2, "
            "the Arrow IPC file format, instead.",
            DeprecationWarning,
            stacklevel=2
        )
        return _write_feather_v1(
            df, dest, compression, compression_level, chunksize)

    if compression is None:
        if Codec.is_available('lz4_frame'):
            compression = 'lz4'
        else:
            compression = 'uncompressed'

    return ipc.write_file(
        df, dest, compression=compression,
        compression_level=compression_level, max_chunksize=chunksize)


def read_feather(source, columns=None, use_threads=True,
                 memory_map=False, **kwargs):
    """
    Read a pandas.DataFrame from Feather format. To read as pyarrow.Table use
    feather.read_table.

    Parameters
    ----------
    source : str file path, or file-like object
        You can use MemoryMappedFile as source, for explicitly use memory map.
    columns : sequence, optional
        Only read a specific set of columns. If not provided, all columns are
        read.
    use_threads : bool, default True
        Whether to parallelize reading using multiple threads. If false the
        restriction is used in the conversion to Pandas as well as in the
        reading from Feather format.
    memory_map : boolean, default False
        Use memory mapping when opening file on disk, when source is a str.
    **kwargs
        Additional keyword arguments passed on to `pyarrow.Table.to_pandas`.

    Returns
    -------
    df : pandas.DataFrame
        The contents of the Feather file as a pandas.DataFrame
    """
    return (_read_table_internal(
        source, columns=columns, memory_map=memory_map,
        use_threads=use_threads).to_pandas(use_threads=use_threads, **kwargs))


def _read_feather_v1(reader, columns):
    if columns is None:
        return reader.read()

    column_kind = _validate_columns(columns)
    if column_kind == "indices":
        return reader.read_indices(columns)
    return reader.read_names(columns)


def _read_table_internal(source, columns=None, memory_map=False,
                         use_threads=True):
    """Internal implementation for reading a Feather file."""
    if _read_magic(source) == _FEATHER_V1_MAGIC:
        reader = _feather.FeatherReader(
            source, use_memory_map=memory_map, use_threads=use_threads)
        warnings.warn(
            "Feather V1 reading is deprecated as of 25.0.0. Consider "
            "rewriting the file using the Arrow IPC file format.",
            DeprecationWarning,
            stacklevel=3
        )
        return _read_feather_v1(reader, columns)

    # Preserve Feather V2's historical behavior: an empty list selects all
    # columns, while other empty sequences select no columns.
    if columns is not None and not columns:
        _validate_columns(columns)
        if sorted(set(columns)) == columns:
            columns = None

    return ipc.read_file(
        source, columns=columns, memory_map=memory_map,
        use_threads=use_threads)


def read_table(source, columns=None, memory_map=False, use_threads=True):
    """
    Read a pyarrow.Table from Feather format

    Parameters
    ----------
    source : str file path, or file-like object
        You can use MemoryMappedFile as source, for explicitly use memory map.
    columns : sequence, optional
        Only read a specific set of columns. If not provided, all columns are
        read.
    memory_map : boolean, default False
        Use memory mapping when opening file on disk, when source is a str
    use_threads : bool, default True
        Whether to parallelize reading using multiple threads.

    Returns
    -------
    table : pyarrow.Table
        The contents of the Feather file as a pyarrow.Table
    """
    return _read_table_internal(source, columns=columns,
                                memory_map=memory_map,
                                use_threads=use_threads)
