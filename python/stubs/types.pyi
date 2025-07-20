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

def is_null(t: DataType) -> TypeIs[NullType]: ...
def is_boolean(t: DataType) -> TypeIs[BoolType]: ...
def is_integer(t: DataType) -> TypeIs[_Integer]: ...
def is_signed_integer(t: DataType) -> TypeIs[_SignedInteger]: ...
def is_unsigned_integer(t: DataType) -> TypeIs[_UnsignedInteger]: ...
def is_int8(t: DataType) -> TypeIs[Int8Type]: ...
def is_int16(t: DataType) -> TypeIs[Int16Type]: ...
def is_int32(t: DataType) -> TypeIs[Int32Type]: ...
def is_int64(t: DataType) -> TypeIs[Int64Type]: ...
def is_uint8(t: DataType) -> TypeIs[UInt8Type]: ...
def is_uint16(t: DataType) -> TypeIs[UInt16Type]: ...
def is_uint32(t: DataType) -> TypeIs[Uint32Type]: ...
def is_uint64(t: DataType) -> TypeIs[UInt64Type]: ...
def is_floating(t: DataType) -> TypeIs[_Floating]: ...
def is_float16(t: DataType) -> TypeIs[Float16Type]: ...
def is_float32(t: DataType) -> TypeIs[Float32Type]: ...
def is_float64(t: DataType) -> TypeIs[Float64Type]: ...
def is_list(t: DataType) -> TypeIs[ListType[Any]]: ...
def is_large_list(t: DataType) -> TypeIs[LargeListType[Any]]: ...
def is_fixed_size_list(t: DataType) -> TypeIs[FixedSizeListType[Any, Any]]: ...
def is_list_view(t: DataType) -> TypeIs[ListViewType[Any]]: ...
def is_large_list_view(t: DataType) -> TypeIs[LargeListViewType[Any]]: ...
def is_struct(t: DataType) -> TypeIs[StructType]: ...
def is_union(t: DataType) -> TypeIs[_Union]: ...
def is_nested(t: DataType) -> TypeIs[_Nested]: ...
def is_run_end_encoded(t: DataType) -> TypeIs[RunEndEncodedType[Any, Any]]: ...
def is_temporal(t: DataType) -> TypeIs[_Temporal]: ...
def is_timestamp(t: DataType) -> TypeIs[TimestampType[Any, Any]]: ...
def is_duration(t: DataType) -> TypeIs[DurationType[Any]]: ...
def is_time(t: DataType) -> TypeIs[_Time]: ...
def is_time32(t: DataType) -> TypeIs[Time32Type[Any]]: ...
def is_time64(t: DataType) -> TypeIs[Time64Type[Any]]: ...
def is_binary(t: DataType) -> TypeIs[BinaryType]: ...
def is_large_binary(t: DataType) -> TypeIs[LargeBinaryType]: ...
def is_unicode(t: DataType) -> TypeIs[StringType]: ...
def is_string(t: DataType) -> TypeIs[StringType]: ...
def is_large_unicode(t: DataType) -> TypeIs[LargeStringType]: ...
def is_large_string(t: DataType) -> TypeIs[LargeStringType]: ...
def is_fixed_size_binary(t: DataType) -> TypeIs[FixedSizeBinaryType]: ...
def is_binary_view(t: DataType) -> TypeIs[BinaryViewType]: ...
def is_string_view(t: DataType) -> TypeIs[StringViewType]: ...
def is_date(t: DataType) -> TypeIs[_Date]: ...
def is_date32(t: DataType) -> TypeIs[Date32Type]: ...
def is_date64(t: DataType) -> TypeIs[Date64Type]: ...
def is_map(t: DataType) -> TypeIs[MapType[Any, Any, Any]]: ...
def is_decimal(t: DataType) -> TypeIs[_Decimal]: ...
def is_decimal32(t: DataType) -> TypeIs[Decimal32Type[Any, Any]]: ...
def is_decimal64(t: DataType) -> TypeIs[Decimal64Type[Any, Any]]: ...
def is_decimal128(t: DataType) -> TypeIs[Decimal128Type[Any, Any]]: ...
def is_decimal256(t: DataType) -> TypeIs[Decimal256Type[Any, Any]]: ...
def is_dictionary(t: DataType) -> TypeIs[DictionaryType[Any, Any, Any]]: ...
def is_interval(t: DataType) -> TypeIs[_Interval]: ...
def is_primitive(t: DataType) -> bool: ...

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
