# BSD 2-Clause License
#
# Copyright (c) 2024, ZhengYu, Xu
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from typing import Any, TypedDict, TypeVar

import numpy as np
import pandas as pd

from pandas import DatetimeTZDtype

from .lib import Array, DataType, Schema, Table

_T = TypeVar("_T")

def get_logical_type_map() -> dict[int, str]: ...
def get_logical_type(arrow_type: DataType) -> str: ...
def get_numpy_logical_type_map() -> dict[type[np.generic], str]: ...
def get_logical_type_from_numpy(pandas_collection) -> str: ...
def get_extension_dtype_info(column) -> tuple[str, dict[str, Any]]: ...

class _ColumnMetadata(TypedDict):
    name: str
    field_name: str
    pandas_type: int
    numpy_type: str
    metadata: dict | None

def get_column_metadata(
    column: pd.Series | pd.Index, name: str, arrow_type: DataType, field_name: str
) -> _ColumnMetadata: ...
def construct_metadata(
    columns_to_convert: list[pd.Series],
    df: pd.DataFrame,
    column_names: list[str],
    index_levels: list[pd.Index],
    index_descriptors: list[dict],
    preserve_index: bool,
    types: list[DataType],
    column_field_names: list[str] = ...,
) -> dict[bytes, bytes]: ...
def dataframe_to_types(
    df: pd.DataFrame, preserve_index: bool | None, columns: list[str] | None = None
) -> tuple[list[str], list[DataType], dict[bytes, bytes]]: ...
def dataframe_to_arrays(
    df: pd.DataFrame,
    schema: Schema,
    preserve_index: bool | None,
    nthreads: int = 1,
    columns: list[str] | None = None,
    safe: bool = True,
) -> tuple[Array, Schema, int]: ...
def get_datetimetz_type(values: _T, dtype, type_) -> tuple[_T, DataType]: ...
def make_datetimetz(unit: str, tz: str) -> DatetimeTZDtype: ...
def table_to_dataframe(
    options, table: Table, categories=None, ignore_metadata: bool = False, types_mapper=None
) -> pd.DataFrame: ...
def make_tz_aware(series: pd.Series, tz: str) -> pd.Series: ...
