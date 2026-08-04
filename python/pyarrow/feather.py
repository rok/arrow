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


import warnings

from pyarrow.lib import concat_tables
import pyarrow.ipc as ipc
from pyarrow._feather import FeatherError  # noqa: F401


def _warn_deprecated(name, replacement):
    warnings.warn(
        f"pyarrow.feather.{name} is deprecated as of 26.0.0. "
        f"Use {replacement} instead.",
        DeprecationWarning,
        stacklevel=3
    )


class FeatherDataset:
    """
    Encapsulates details of reading a list of Feather files.

    .. deprecated:: 26.0.0
       Use :func:`pyarrow.dataset.dataset` with ``format='ipc'`` instead.

    Parameters
    ----------
    path_or_paths : List[str]
        A list of file names
    validate_schema : bool, default True
        Check that individual file schemas are all the same / compatible
    """

    def __init__(self, path_or_paths, validate_schema=True):
        _warn_deprecated(
            "FeatherDataset",
            "pyarrow.dataset.dataset() with format='ipc'"
        )
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
        Read multiple Parquet files as a single pandas DataFrame

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


# Preserve the existing helper name for compatibility.
check_chunked_overflow = ipc._check_chunked_overflow


def write_feather(df, dest, compression=None, compression_level=None,
                  chunksize=None, version=2):
    """
    Write a pandas.DataFrame to Feather format.

    .. deprecated:: 26.0.0
       Use :func:`pyarrow.ipc.write_file` instead.

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

    _warn_deprecated("write_feather", "pyarrow.ipc.write_file")

    return ipc._write_file(
        df, dest, compression=compression,
        compression_level=compression_level, chunksize=chunksize,
        version=version)


def read_feather(source, columns=None, use_threads=True,
                 memory_map=False, **kwargs):
    """
    Read a pandas.DataFrame from Feather format. To read as pyarrow.Table use
    feather.read_table.

    .. deprecated:: 26.0.0
       Use :func:`pyarrow.ipc.read_file` and convert the resulting table with
       :meth:`pyarrow.Table.to_pandas` instead.

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
    _warn_deprecated(
        "read_feather", "pyarrow.ipc.read_file(...).to_pandas()")
    return (_read_table_internal(
        source, columns=columns, memory_map=memory_map,
        use_threads=use_threads).to_pandas(use_threads=use_threads, **kwargs))


def _read_table_internal(source, columns=None, memory_map=False,
                         use_threads=True):
    """Internal compatibility implementation without duplicate warnings."""
    return ipc._read_file(
        source, columns=columns, memory_map=memory_map,
        use_threads=use_threads, _warn_v1=False)


def read_table(source, columns=None, memory_map=False, use_threads=True):
    """
    Read a pyarrow.Table from Feather format

    .. deprecated:: 26.0.0
       Use :func:`pyarrow.ipc.read_file` instead.

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
    _warn_deprecated("read_table", "pyarrow.ipc.read_file")
    return _read_table_internal(source, columns=columns,
                                memory_map=memory_map,
                                use_threads=use_threads)
