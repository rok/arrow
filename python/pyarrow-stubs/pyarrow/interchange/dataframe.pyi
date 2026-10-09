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

from typing import Optional
import os
import re
import pyarrow
from typing import Callable, IO, Iterator, Literal, Mapping, NamedTuple, Self
import pyarrow as pa
from _typeshed import Incomplete
from pyarrow.interchange.column import _PyArrowColumn as _PyArrowColumn
from typing import Any, Iterable, Sequence

class _PyArrowDataFrame:
    _df: Incomplete
    _nan_as_null: Incomplete
    _allow_copy: Incomplete

    def __init__(self, df: pa.Table | pa.RecordBatch, nan_as_null: bool=False, allow_copy: bool=True) -> None:
        ...

    def __dataframe__(self, nan_as_null: bool=False, allow_copy: bool=True) -> _PyArrowDataFrame:
        ...

    @property
    def metadata(self) -> dict[str, Any]:
        ...

    def num_columns(self) -> int:
        ...

    def num_rows(self) -> int:
        ...

    def num_chunks(self) -> int:
        ...

    def column_names(self) -> Iterable[str]:
        ...

    def get_column(self, i: int) -> _PyArrowColumn:
        ...

    def get_column_by_name(self, name: str) -> _PyArrowColumn:
        ...

    def get_columns(self) -> Iterable[_PyArrowColumn]:
        ...

    def select_columns(self, indices: Sequence[int]) -> _PyArrowDataFrame:
        ...

    def select_columns_by_name(self, names: Sequence[str]) -> _PyArrowDataFrame:
        ...

    def get_chunks(self, n_chunks: int | None=None) -> Iterable[_PyArrowDataFrame]:
        ...
