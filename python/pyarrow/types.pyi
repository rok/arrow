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

import sys

from typing import Any

if sys.version_info >= (3, 13):
    from typing import TypeIs
else:
    from typing_extensions import TypeIs
if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

from pyarrow.lib import (
    BinaryType,
    BinaryViewType,
    BoolType,
    DataType,
    Date32Type,
    Date64Type,
    Decimal32Type,
    Decimal64Type,
    Decimal128Type,
    Decimal256Type,
    DenseUnionType,
    DictionaryType,
    DurationType,
    FixedSizeBinaryType,
    FixedSizeListType,
    Float16Type,
    Float32Type,
    Float64Type,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    LargeBinaryType,
    LargeListType,
    LargeListViewType,
    LargeStringType,
    ListType,
    ListViewType,
    MapType,
    MonthDayNanoIntervalType,
    NullType,
    RunEndEncodedType,
    SparseUnionType,
    StringType,
    StringViewType,
    StructType,
    Time32Type,
    Time64Type,
    TimestampType,
    UInt8Type,
    UInt16Type,
    Uint32Type,
    UInt64Type,
)

_SignedInteger: TypeAlias = Int8Type | Int16Type | Int32Type | Int64Type
_UnsignedInteger: TypeAlias = UInt8Type | UInt16Type | Uint32Type | UInt64Type
_Integer: TypeAlias = _SignedInteger | _UnsignedInteger
_Floating: TypeAlias = Float16Type | Float32Type | Float64Type
_Decimal: TypeAlias = (
    Decimal32Type[Any, Any]
    | Decimal64Type[Any, Any]
    | Decimal128Type[Any, Any]
    | Decimal256Type[Any, Any]
)
_Date: TypeAlias = Date32Type | Date64Type
_Time: TypeAlias = Time32Type[Any] | Time64Type[Any]
_Interval: TypeAlias = MonthDayNanoIntervalType
_Temporal: TypeAlias = TimestampType[Any, Any] | DurationType[Any] | _Time | _Date | _Interval
_Union: TypeAlias = SparseUnionType | DenseUnionType
_Nested: TypeAlias = (
    ListType[Any]
    | FixedSizeListType[Any, Any]
    | LargeListType[Any]
    | ListViewType[Any]
    | LargeListViewType[Any]
    | StructType
    | MapType[Any, Any, Any]
    | _Union
)

def is_null(t: DataType) -> TypeIs[NullType]:
    """
    Return True if value is an instance of type: null.

    Parameters
    ----------
    t : DataType
    """
def is_boolean(t: DataType) -> TypeIs[BoolType]:
    """
    Return True if value is an instance of type: boolean.

    Parameters
    ----------
    t : DataType
    """
def is_integer(t: DataType) -> TypeIs[_Integer]:
    """
    Return True if value is an instance of type: any integer.

    Parameters
    ----------
    t : DataType
    """
def is_signed_integer(t: DataType) -> TypeIs[_SignedInteger]:
    """
    Return True if value is an instance of type: signed integer.

    Parameters
    ----------
    t : DataType
    """
def is_unsigned_integer(t: DataType) -> TypeIs[_UnsignedInteger]:
    """
    Return True if value is an instance of type: unsigned integer.

    Parameters
    ----------
    t : DataType
    """
def is_int8(t: DataType) -> TypeIs[Int8Type]:
    """
    Return True if value is an instance of type: int8.

    Parameters
    ----------
    t : DataType
    """
def is_int16(t: DataType) -> TypeIs[Int16Type]:
    """
    Return True if value is an instance of type: int16.

    Parameters
    ----------
    t : DataType
    """
def is_int32(t: DataType) -> TypeIs[Int32Type]:
    """
    Return True if value is an instance of type: int32.

    Parameters
    ----------
    t : DataType
    """
def is_int64(t: DataType) -> TypeIs[Int64Type]:
    """
    Return True if value is an instance of type: int64.

    Parameters
    ----------
    t : DataType
    """
def is_uint8(t: DataType) -> TypeIs[UInt8Type]:
    """
    Return True if value is an instance of type: uint8.

    Parameters
    ----------
    t : DataType
    """
def is_uint16(t: DataType) -> TypeIs[UInt16Type]:
    """
    Return True if value is an instance of type: uint16.

    Parameters
    ----------
    t : DataType
    """
def is_uint32(t: DataType) -> TypeIs[Uint32Type]:
    """
    Return True if value is an instance of type: uint32.

    Parameters
    ----------
    t : DataType
    """
def is_uint64(t: DataType) -> TypeIs[UInt64Type]:
    """
    Return True if value is an instance of type: uint64.

    Parameters
    ----------
    t : DataType
    """
def is_floating(t: DataType) -> TypeIs[_Floating]:
    """
    Return True if value is an instance of type: floating point numeric.

    Parameters
    ----------
    t : DataType
    """
def is_float16(t: DataType) -> TypeIs[Float16Type]:
    """
    Return True if value is an instance of type: float16 (half-precision).

    Parameters
    ----------
    t : DataType
    """
def is_float32(t: DataType) -> TypeIs[Float32Type]:
    """
    Return True if value is an instance of type: float32 (single precision).

    Parameters
    ----------
    t : DataType
    """
def is_float64(t: DataType) -> TypeIs[Float64Type]:
    """
    Return True if value is an instance of type: float64 (double precision).

    Parameters
    ----------
    t : DataType
    """
def is_list(t: DataType) -> TypeIs[ListType[Any]]:
    """
    Return True if value is an instance of type: list.

    Parameters
    ----------
    t : DataType
    """
def is_large_list(t: DataType) -> TypeIs[LargeListType[Any]]:
    """
    Return True if value is an instance of type: large list.

    Parameters
    ----------
    t : DataType
    """
def is_fixed_size_list(t: DataType) -> TypeIs[FixedSizeListType[Any, Any]]:
    """
    Return True if value is an instance of type: fixed size list.

    Parameters
    ----------
    t : DataType
    """
def is_list_view(t: DataType) -> TypeIs[ListViewType[Any]]:
    """
    Return True if value is an instance of type: list view.

    Parameters
    ----------
    t : DataType
    """
def is_large_list_view(t: DataType) -> TypeIs[LargeListViewType[Any]]:
    """
    Return True if value is an instance of type: large list view.

    Parameters
    ----------
    t : DataType
    """
def is_struct(t: DataType) -> TypeIs[StructType]:
    """
    Return True if value is an instance of type: struct.

    Parameters
    ----------
    t : DataType
    """
def is_union(t: DataType) -> TypeIs[_Union]:
    """
    Return True if value is an instance of type: union.

    Parameters
    ----------
    t : DataType
    """
def is_nested(t: DataType) -> TypeIs[_Nested]:
    """
    Return True if value is an instance of type: nested type.

    Parameters
    ----------
    t : DataType
    """
def is_run_end_encoded(t: DataType) -> TypeIs[RunEndEncodedType[Any, Any]]:
    """
    Return True if value is an instance of type: run-end encoded.

    Parameters
    ----------
    t : DataType
    """
def is_temporal(t: DataType) -> TypeIs[_Temporal]:
    """
    Return True if value is an instance of type: date, time, timestamp or duration.

    Parameters
    ----------
    t : DataType
    """
def is_timestamp(t: DataType) -> TypeIs[TimestampType[Any, Any]]:
    """
    Return True if value is an instance of type: timestamp.

    Parameters
    ----------
    t : DataType
    """
def is_duration(t: DataType) -> TypeIs[DurationType[Any]]:
    """
    Return True if value is an instance of type: duration.

    Parameters
    ----------
    t : DataType
    """
def is_time(t: DataType) -> TypeIs[_Time]:
    """
    Return True if value is an instance of type: time.

    Parameters
    ----------
    t : DataType
    """
def is_time32(t: DataType) -> TypeIs[Time32Type[Any]]:
    """
    Return True if value is an instance of type: time32.

    Parameters
    ----------
    t : DataType
    """
def is_time64(t: DataType) -> TypeIs[Time64Type[Any]]:
    """
    Return True if value is an instance of type: time64.

    Parameters
    ----------
    t : DataType
    """
def is_binary(t: DataType) -> TypeIs[BinaryType]:
    """
    Return True if value is an instance of type: variable-length binary.

    Parameters
    ----------
    t : DataType
    """
def is_large_binary(t: DataType) -> TypeIs[LargeBinaryType]:
    """
    Return True if value is an instance of type: large variable-length binary.

    Parameters
    ----------
    t : DataType
    """
def is_unicode(t: DataType) -> TypeIs[StringType]:
    """
    Alias for is_string.

    Parameters
    ----------
    t : DataType
    """
def is_string(t: DataType) -> TypeIs[StringType]:
    """
    Return True if value is an instance of type: string (utf8 unicode).

    Parameters
    ----------
    t : DataType
    """
def is_large_unicode(t: DataType) -> TypeIs[LargeStringType]:
    """
    Alias for is_large_string.

    Parameters
    ----------
    t : DataType
    """
def is_large_string(t: DataType) -> TypeIs[LargeStringType]:
    """
    Return True if value is an instance of type: large string (utf8 unicode).

    Parameters
    ----------
    t : DataType
    """
def is_fixed_size_binary(t: DataType) -> TypeIs[FixedSizeBinaryType]:
    """
    Return True if value is an instance of type: fixed size binary.

    Parameters
    ----------
    t : DataType
    """
def is_binary_view(t: DataType) -> TypeIs[BinaryViewType]:
    """
    Return True if value is an instance of type: variable-length binary view.

    Parameters
    ----------
    t : DataType
    """
def is_string_view(t: DataType) -> TypeIs[StringViewType]:
    """
    Return True if value is an instance of type: variable-length string (utf-8) view.

    Parameters
    ----------
    t : DataType
    """
def is_date(t: DataType) -> TypeIs[_Date]:
    """
    Return True if value is an instance of type: date.

    Parameters
    ----------
    t : DataType
    """
def is_date32(t: DataType) -> TypeIs[Date32Type]:
    """
    Return True if value is an instance of type: date32 (days).

    Parameters
    ----------
    t : DataType
    """
def is_date64(t: DataType) -> TypeIs[Date64Type]:
    """
    Return True if value is an instance of type: date64 (milliseconds).

    Parameters
    ----------
    t : DataType
    """
def is_map(t: DataType) -> TypeIs[MapType[Any, Any, Any]]:
    """
    Return True if value is an instance of type: map.

    Parameters
    ----------
    t : DataType
    """
def is_decimal(t: DataType) -> TypeIs[_Decimal]:
    """
    Return True if value is an instance of type: decimal.

    Parameters
    ----------
    t : DataType
    """
def is_decimal32(t: DataType) -> TypeIs[Decimal32Type[Any, Any]]:
    """
    Return True if value is an instance of type: decimal32.

    Parameters
    ----------
    t : DataType
    """
def is_decimal64(t: DataType) -> TypeIs[Decimal64Type[Any, Any]]:
    """
    Return True if value is an instance of type: decimal64.

    Parameters
    ----------
    t : DataType
    """
def is_decimal128(t: DataType) -> TypeIs[Decimal128Type[Any, Any]]:
    """
    Return True if value is an instance of type: decimal128.

    Parameters
    ----------
    t : DataType
    """
def is_decimal256(t: DataType) -> TypeIs[Decimal256Type[Any, Any]]:
    """
    Return True if value is an instance of type: decimal256.

    Parameters
    ----------
    t : DataType
    """
def is_dictionary(t: DataType) -> TypeIs[DictionaryType[Any, Any, Any]]:
    """
    Return True if value is an instance of type: dictionary-encoded.

    Parameters
    ----------
    t : DataType
    """
def is_interval(t: DataType) -> TypeIs[_Interval]:
    """
    Return True if value is an instance of type: interval.

    Parameters
    ----------
    t : DataType
    """
def is_primitive(t: DataType) -> bool:
    """
    Return True if value is an instance of type: primitive type.

    Parameters
    ----------
    t : DataType
    """
def is_boolean_value(obj: Any) -> bool:
    """
    Check if the object is a boolean.

    Parameters
    ----------
    obj : object
        The object to check
    """

def is_integer_value(obj: Any) -> bool:
    """
    Check if the object is an integer.

    Parameters
    ----------
    obj : object
        The object to check
    """

def is_float_value(obj: Any) -> bool:
    """
    Check if the object is a float.

    Parameters
    ----------
    obj : object
        The object to check
    """

__all__ = [
    "is_binary",
    "is_binary_view",
    "is_boolean",
    "is_date",
    "is_date32",
    "is_date64",
    "is_decimal",
    "is_decimal128",
    "is_decimal256",
    "is_decimal32",
    "is_decimal64",
    "is_dictionary",
    "is_duration",
    "is_fixed_size_binary",
    "is_fixed_size_list",
    "is_float16",
    "is_float32",
    "is_float64",
    "is_floating",
    "is_int16",
    "is_int32",
    "is_int64",
    "is_int8",
    "is_integer",
    "is_interval",
    "is_large_binary",
    "is_large_list",
    "is_large_list_view",
    "is_large_string",
    "is_large_unicode",
    "is_list",
    "is_list_view",
    "is_map",
    "is_nested",
    "is_null",
    "is_primitive",
    "is_run_end_encoded",
    "is_signed_integer",
    "is_string",
    "is_string_view",
    "is_struct",
    "is_temporal",
    "is_time",
    "is_time32",
    "is_time64",
    "is_timestamp",
    "is_uint16",
    "is_uint32",
    "is_uint64",
    "is_uint8",
    "is_unicode",
    "is_union",
    "is_unsigned_integer",
]
