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

from pyarrow.lib import BinaryArray, BooleanArray, ChunkedArray, DataType, Date32Array, Decimal128Array, Decimal128Type, DictionaryArray, DictionaryType, DoubleArray, DurationArray, DurationType, ExtensionArray, Field, FixedSizeBinaryType, FixedSizeListType, FloatArray, HalfFloatArray, Int16Array, Int32Array, Int64Array, Int8Array, LargeListType, LargeListViewType, LargeStringArray, ListArray, ListType, ListViewType, MapArray, MapType, NullArray, RunEndEncodedType, Schema, StringArray, StringViewArray, StructArray, StructType, Table, Time32Type, Time64Type, TimestampArray, TimestampType, UInt16Array, UInt32Array, UInt64Array, UInt8Array, UuidArray, UuidType
from pyarrow.tests.test_pandas import DummyExtensionType
from typing import Dict, List, Optional, Tuple, Type, Union
import os
import re
import pyarrow
from typing import Any, Callable, IO, Iterable, Iterator, Literal, Mapping, NamedTuple, Self, Sequence
from _typeshed import Incomplete
from pyarrow.lib import _pandas_api as _pandas_api, frombytes as frombytes, is_threading_enabled as is_threading_enabled
_logical_type_map: Incomplete
_numpy_logical_type_map: Incomplete
_pandas_logical_type_map: Incomplete

def get_logical_type_map() -> dict[int, str]:
    ...

def get_logical_type(arrow_type) -> str:
    ...

def get_numpy_logical_type_map():
    ...

def get_logical_type_from_numpy(pandas_collection) -> str:
    ...

def get_extension_dtype_info(column) -> tuple[str, dict[str, str]] | tuple[str, dict[str, int | bool]] | tuple[str, None]:
    ...

def get_column_metadata(column, name: str, arrow_type: pyarrow.DataType, field_name: str) -> dict:
    ...

def construct_metadata(columns_to_convert, df, column_names: list[Any | str], index_levels, index_descriptors: list[dict], preserve_index: bool, types: list[pyarrow.DataType], column_field_names: list[Any | str] | None=None) -> dict:
    ...

def _get_simple_index_descriptor(level, name: str | None) -> dict[str, str | dict[str, str] | None] | dict[str, str | dict[str, str]] | dict[str, str | None]:
    ...

def _column_name_to_strings(name: str | tuple) -> str | tuple:
    ...

def _index_level_name(index, i: int, column_names: list[Any | str]) -> str:
    ...

def _get_columns_to_convert(df, schema: Schema | None, preserve_index: bool | None, columns: list[str] | None):
    ...

def _get_columns_to_convert_given_schema(df, schema: Schema, preserve_index: bool | None):
    ...

def _get_index_level(df, name: str):
    ...

def _level_name(name) -> int | str | None:
    ...

def _get_range_index_descriptor(level) -> dict[str, str | int] | dict[str, str | int | None]:
    ...

def _get_index_level_values(index):
    ...

def _resolve_columns_of_interest(df, schema: Schema | None, columns: list[str] | None):
    ...

def dataframe_to_types(df, preserve_index: bool | None, columns=None):
    ...

def dataframe_to_arrays(df, schema: Schema | None, preserve_index: bool | None, nthreads: int=1, columns: list[str] | None=None, safe: bool=True) -> tuple[list[MapArray | StringArray], Schema, None] | tuple[list[Int64Array | FloatArray | DoubleArray | BooleanArray | LargeStringArray], Schema, None] | tuple[list[LargeStringArray | Int64Array | TimestampArray], Schema, None] | tuple[list[DoubleArray], Schema, None] | tuple[list[DoubleArray | LargeStringArray], Schema, None] | tuple[list[UInt8Array | UInt16Array | UInt32Array | UInt64Array | Int16Array | Int32Array | Int64Array | HalfFloatArray | FloatArray | DoubleArray | BooleanArray | TimestampArray | DurationArray | LargeStringArray | NullArray | ListArray], Schema, None] | tuple[list[TimestampArray | DictionaryArray | LargeStringArray | BooleanArray | DoubleArray | Int64Array | ExtensionArray], Schema, None] | tuple[list[Date32Array], Schema, None] | tuple[list[StringArray | Int64Array | DoubleArray], Schema, None] | tuple[list[UInt32Array], Schema, None] | tuple[list[Int16Array], Schema, None] | tuple[list[Int64Array | LargeStringArray], Schema, None] | tuple[list[Date32Array | Int64Array | DoubleArray | StringArray], Schema, None] | tuple[list[Any], Schema, None] | tuple[list[ExtensionArray], Schema, None] | tuple[list[ListArray], Schema, None] | tuple[list[StructArray], Schema, None] | tuple[list[UInt8Array | UInt32Array | Int16Array | Int32Array | HalfFloatArray | DoubleArray | TimestampArray | LargeStringArray | ListArray], Schema, None] | tuple[list[Int64Array | DoubleArray], Schema, None] | tuple[list[TimestampArray], Schema, None] | tuple[list[Int64Array | UInt32Array | DoubleArray | LargeStringArray | BooleanArray], Schema, None] | tuple[list[UInt8Array | UInt16Array | UInt32Array | UInt64Array | Int8Array | Int16Array | Int32Array | Int64Array | FloatArray | DoubleArray | BooleanArray], Schema, None] | tuple[list[StructArray | LargeStringArray], Schema, None] | tuple[list[UInt8Array | UInt16Array | UInt32Array | UInt64Array], Schema, None] | tuple[list[DurationArray], Schema, None] | tuple[list[UInt8Array | UInt16Array | UInt32Array | UInt64Array | Int16Array | Int32Array | Int64Array | FloatArray | DoubleArray | BooleanArray | TimestampArray | DurationArray | LargeStringArray], Schema, None] | tuple[list[LargeStringArray | ListArray], Schema, None] | tuple[list[UInt64Array], Schema, None] | tuple[list[DoubleArray | ListArray | Int32Array], Schema, None] | tuple[list[LargeStringArray | Decimal128Array], Schema, None] | tuple[list[StringViewArray], Schema, None] | tuple[list[Int64Array | StructArray], Schema, None] | tuple[list[DoubleArray | LargeStringArray | Int64Array], Schema, None] | tuple[list[UInt8Array], Schema, None] | tuple[list[UInt8Array | UInt16Array | UInt32Array | UInt64Array | Int8Array | Int16Array | Int32Array | Int64Array | FloatArray | DoubleArray | BooleanArray | LargeStringArray | NullArray], Schema, None] | tuple[list[UInt32Array | DoubleArray | BooleanArray | LargeStringArray], Schema, None] | tuple[list[FloatArray | DoubleArray], Schema, None] | tuple[list[Int16Array | FloatArray | StringArray], Schema, None] | tuple[list[UInt8Array | UInt16Array | Int64Array | UInt64Array | Int8Array | Int16Array | Int32Array | FloatArray | DoubleArray | BooleanArray | LargeStringArray | NullArray], Schema, None] | tuple[list[Int64Array | BooleanArray], Schema, None] | tuple[list[LargeStringArray | Int64Array | UInt8Array | DoubleArray | BooleanArray | DictionaryArray | TimestampArray], Schema, None] | tuple[list[Int64Array | ListArray | DoubleArray], Schema, None] | tuple[list[UInt8Array | UInt16Array | UInt32Array | UInt64Array | Int16Array | Int32Array | Int64Array | FloatArray | DoubleArray | BooleanArray], Schema, None] | tuple[list[LargeStringArray], Schema, None] | tuple[list[NullArray], Schema, None] | tuple[list[DictionaryArray], Schema, None] | tuple[list[Int32Array | StringArray], Schema, None] | tuple[list[DictionaryArray | Int64Array | LargeStringArray | BinaryArray], Schema, None] | tuple[list[FloatArray | TimestampArray], Schema, None] | tuple[list[StringArray | Int64Array | Int32Array | TimestampArray], Schema, None] | tuple[list[BooleanArray], Schema, None] | tuple[list[LargeStringArray | Int64Array | DoubleArray | TimestampArray], Schema, None] | tuple[list[Int64Array | ListArray], Schema, None] | tuple[list[DictionaryArray | Int64Array], Schema, None] | tuple[list[UInt8Array | UInt16Array | UInt32Array | UInt64Array | Int16Array | Int32Array | Int64Array | FloatArray | DoubleArray | BooleanArray | LargeStringArray], Schema, None] | tuple[list[StringArray], Schema, None] | tuple[list[Any], Schema, int] | tuple[list[Int8Array], Schema, None] | tuple[list[Decimal128Array], Schema, None] | tuple[list[NullArray | DoubleArray], Schema, None] | tuple[list[UInt16Array], Schema, None] | tuple[list[UuidArray], Schema, None] | tuple[list[BinaryArray], Schema, None] | tuple[list[LargeStringArray | DictionaryArray], Schema, None] | tuple[list[UInt8Array | UInt16Array | UInt32Array | UInt64Array | Int16Array | Int32Array | Int64Array | HalfFloatArray | FloatArray | DoubleArray | BooleanArray | TimestampArray | DurationArray | LargeStringArray | NullArray | ListArray | DictionaryArray], Schema, None] | tuple[list[Int32Array], Schema, None] | tuple[list[DoubleArray | DictionaryArray], Schema, None] | tuple[list[StringArray | FloatArray], Schema, None] | tuple[list[Int8Array | Int16Array | Int32Array | Int64Array | UInt8Array | UInt16Array | UInt32Array | UInt64Array], Schema, None] | tuple[list[FloatArray], Schema, None] | tuple[list[Int64Array], Schema, None] | tuple[list[ChunkedArray | StringArray], Schema, None] | tuple[list[TimestampArray | Int64Array], Schema, None]:
    ...

def get_datetimetz_type(values, dtype, type_: Decimal128Type | LargeListViewType | StructType | LargeListType | MapType | RunEndEncodedType | DictionaryType | ListType | TimestampType | FixedSizeListType | DataType | FixedSizeBinaryType | DurationType | Time64Type | Time32Type | ListViewType | None):
    ...

def _reconstruct_block(item: dict, columns: list[str] | None=None, extension_columns: dict | None=None, return_block: bool=True):
    ...

def make_datetimetz(unit: str, tz: str):
    ...

def table_to_dataframe(options: dict[str, bool | None], table: Table, categories=None, ignore_metadata: bool=False, types_mapper=None):
    ...
_pandas_supported_numpy_types: Incomplete

def _get_extension_dtypes(table: Table, columns_metadata: list[dict[str, str | dict[str, int]] | dict[str, str | None] | dict[str, str | dict[str, int | bool]] | Any | dict[str, str | dict[str, str]] | dict[str, str | dict[str, str] | None]], types_mapper, options: dict[str, bool | None], categories):
    ...

def _check_data_column_metadata_consistency(all_columns: list[dict[str, str | dict[str, int]] | dict[str, str | None] | dict[str, str | dict[str, int | bool]] | Any | dict[str, str | dict[str, str]] | dict[str, str | dict[str, str] | None]]) -> None:
    ...

def _deserialize_column_index(block_table: Table, all_columns: list[dict[str, str | dict[str, int]] | dict[str, str | None] | dict[str, str | dict[str, int | bool]] | Any | dict[str, str | dict[str, str]] | dict[str, str | dict[str, str] | None]], column_indexes: list[dict[str, str | dict[str, str]] | dict[str, str | dict[str, str] | None] | dict[str, str | None] | Any]):
    ...

def _reconstruct_index(table: Table, index_descriptors: list[Any | dict[str, str | int] | dict[str, str | int | None] | str], all_columns: list[dict[str, str | dict[str, int]] | dict[str, str | None] | dict[str, str | dict[str, int | bool]] | Any | dict[str, str | dict[str, str]] | dict[str, str | dict[str, str] | None]], types_mapper=None):
    ...

def _extract_index_level(table: Table, result_table: Table, field_name: str, field_name_to_metadata: dict[str, dict[str, str | None] | dict[str, str | dict[str, int | bool]]] | dict[str, dict[str, str | None] | dict[str, str | dict[str, str]]] | dict[str, dict[str, str | dict[str, str]] | dict[str, str | dict[str, str] | None]] | dict[str, dict[str, str | None]], types_mapper=None):
    ...

def _backwards_compatible_index_name(raw_name: str, logical_name: str) -> str:
    ...

def _is_generated_index_name(name: str) -> bool:
    ...

def get_pandas_logical_type_map():
    ...

def _pandas_type_to_numpy_type(pandas_type: str):
    ...

def _reconstruct_columns_from_metadata(columns, column_indexes: list[dict[str, str | dict[str, str]] | dict[str, str | None] | dict[str, str | dict[str, str] | None]]):
    ...

def _add_any_metadata(table: Table, pandas_metadata: dict[str, list[str] | str | list[dict[str, str | None] | dict[str, str | dict[str, str]]]] | dict[str, list[dict[str, str | int | None]] | list[dict[str, str | dict[str, str] | None]] | list[dict[str, str | None] | dict[str, str | dict[str, str]]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[dict[str, str | int | None]] | list[dict[str, str | dict[str, str] | None]] | list[dict[str, str | dict[str, int]]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[Any] | list[dict[str, str | None] | dict[str, str | dict[str, int | bool]]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[Any] | list[dict[str, str | dict[str, str] | None]] | list[dict[str, str | None]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[str] | list[dict[str, str | None]] | str] | dict[str, list[str] | list[dict[str, str | dict[str, str] | None]] | list[dict[str, str | None]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[dict[str, str | None]] | list[dict[str, str | int]] | str] | dict[str, list[str] | list[dict[str, str | dict[str, str] | None]] | list[dict[str, str | None]] | str] | dict[str, list[Any] | list[dict[str, str | dict[str, int | bool]]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[dict[str, str | int | None]] | list[dict[str, str | dict[str, str]]] | list[dict[str, str | None]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[str] | list[dict[str, str | dict[str, str] | None]] | list[dict[str, str | None] | dict[str, str | dict[str, int | bool]]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[Any] | list[dict[str, str | None] | dict[str, str | dict[str, int]]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[dict[str, str | int | None]] | list[dict[str, str | None]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[str] | list[dict[str, str | dict[str, str] | None]] | list[dict[str, str | dict[str, str]] | dict[str, str | dict[str, str] | None]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[str] | list[dict[str, str | None]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[dict[str, str | int | None]] | list[dict[str, str | dict[str, str] | None]] | list[dict[str, str | dict[str, str]]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[dict[str, str | int | None]] | list[dict[str, str | dict[str, str] | None]] | list[dict[str, str | None]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[dict[str, str | int | None]] | list[dict[str, str | dict[str, str] | None]] | list[dict[str, str | None] | dict[str, str | dict[str, int | bool]]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[Any] | list[dict[str, str | dict[str, str] | None]] | list[dict[str, str | None] | dict[str, str | dict[str, str]]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[dict[str, str | int | None]] | list[dict[str, str | dict[str, str] | None]] | list[dict[str, str | dict[str, int | bool]]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[dict[str, str | int | None]] | list[dict[str, str | None]] | list[Any] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[Any] | list[dict[str, str | None]] | dict[Any, Any] | dict[str, str] | str] | dict[str, list[Any] | list[dict[str, str | dict[str, str]]] | dict[Any, Any] | dict[str, str] | str]) -> Table:
    ...

def make_tz_aware(series, tz: str):
    ...
