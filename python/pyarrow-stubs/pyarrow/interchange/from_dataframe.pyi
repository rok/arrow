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

from pyarrow.lib import DataType, TimestampType
import pyarrow
import pyarrow as pa
from pyarrow.interchange.column import (
    ColumnBuffers as ColumnBuffers,
    ColumnNullType as ColumnNullType,
    Dtype as Dtype,
    DtypeKind as DtypeKind,
)
from typing import Any, TypeAlias

DataFrameObject: TypeAlias = Any
ColumnObject: TypeAlias = Any
BufferObject: TypeAlias = Any
_PYARROW_DTYPES: dict[DtypeKind, dict[int, Any]]

def from_dataframe(df: DataFrameObject, allow_copy: bool = True) -> pa.Table: ...
def _from_dataframe(df: DataFrameObject, allow_copy: bool = True) -> pyarrow.Table: ...
def protocol_df_chunk_to_pyarrow(
    df: DataFrameObject, allow_copy: bool = True
) -> pa.RecordBatch: ...
def column_to_array(col: ColumnObject, allow_copy: bool = True) -> pa.Array: ...
def bool_column_to_array(col: ColumnObject, allow_copy: bool = True) -> pa.Array: ...
def categorical_column_to_dictionary(
    col: ColumnObject, allow_copy: bool = True
) -> pa.DictionaryArray: ...
def parse_datetime_format_str(format_str: str) -> tuple[str, str]: ...
def map_date_type(
    data_type: tuple[DtypeKind, int, str, str],
) -> DataType | TimestampType: ...
def buffers_to_array(
    buffers: ColumnBuffers,
    data_type: tuple[DtypeKind, int, str, str],
    length: int,
    describe_null: ColumnNullType,
    offset: int = 0,
    allow_copy: bool = True,
) -> pa.Array: ...
def validity_buffer_from_mask(
    validity_buff: BufferObject,
    validity_dtype: Dtype,
    describe_null: ColumnNullType,
    length: int,
    offset: int = 0,
    allow_copy: bool = True,
) -> pa.Buffer: ...
def validity_buffer_nan_sentinel(
    data_pa_buffer: BufferObject,
    data_type: Dtype,
    describe_null: ColumnNullType,
    length: int,
    offset: int = 0,
    allow_copy: bool = True,
) -> pa.Buffer: ...
