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
from pyarrow import _feather as _feather
from pyarrow._feather import FeatherError as FeatherError
from pyarrow.lib import Codec as Codec, Table as Table, concat_tables as concat_tables, schema as schema
from pyarrow.pandas_compat import _pandas_api as _pandas_api

def check_chunked_overflow(name, col) -> None:
    ...
_FEATHER_SUPPORTED_CODECS: Incomplete

def write_feather(df, dest: str, compression: str | None=None, compression_level: int | None=None, chunksize: int | None=None, version: int=2) -> None:
    ...

def read_feather(source, columns: Sequence[Any] | None=None, use_threads: bool=True, memory_map: bool=False, **kwargs):
    ...

def _read_table_internal(source, columns=None, memory_map: bool=False, use_threads: bool=True):
    ...

def read_table(source, columns: Sequence[Any] | None=None, memory_map: bool=False, use_threads: bool=True) -> Table:
    ...

class FeatherDataset:
    paths: Incomplete
    validate_schema: Incomplete

    def __init__(self, path_or_paths, validate_schema: bool=True) -> None:
        ...
    _tables: Incomplete
    schema: Incomplete

    def read_table(self, columns: list[str] | None=None) -> Table:
        ...

    def validate_schemas(self, piece, table) -> None:
        ...

    def read_pandas(self, columns: list[str] | None=None, use_threads: bool=True):
        ...
