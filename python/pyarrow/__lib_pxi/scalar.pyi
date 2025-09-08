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

import collections.abc
import datetime as dt
import sys

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self
if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias
from typing import Any, Generic, Iterator, Literal, overload

import numpy as np

from pyarrow._compute import CastOptions  # type: ignore[import-not-found]
from pyarrow.lib import Array, Buffer, MemoryPool, MonthDayNano, Tensor, _Weakrefable
from typing_extensions import Protocol, TypeVar

from . import types
from .types import (
    # _AsPyType,
    _DataTypeT,
    _Time32Unit,
    _Time64Unit,
    _Tz,
    _Unit,
)

_AsPyTypeK = TypeVar("_AsPyTypeK")
_AsPyTypeV = TypeVar("_AsPyTypeV")
_DataType_co = TypeVar("_DataType_co", bound=types.DataType, covariant=True)

class Scalar(_Weakrefable, Generic[_DataType_co]):
    """
    The base class for scalars.
    """
    @property
    def type(self) -> _DataType_co:
        """
        Data type of the Scalar object.
        """
    @property
    def is_valid(self) -> bool:
        """
        Holds a valid (non-null) value.
        """
    def cast(
        self,
        target_type: None | _DataTypeT,
        safe: bool = True,
        options: CastOptions | None = None,
        memory_pool: MemoryPool | None = None,
    ) -> Self | Scalar[_DataTypeT]:
        """
        Cast scalar value to another data type.

        See :func:`pyarrow.compute.cast` for usage.

        Parameters
        ----------
        target_type : DataType, default None
            Type to cast scalar to.
        safe : boolean, default True
            Whether to check for conversion errors such as overflow.
        options : CastOptions, default None
            Additional checks pass by CastOptions
        memory_pool : MemoryPool, optional
            memory pool to use for allocations during function execution.

        Returns
        -------
        scalar : A Scalar of the given target data type.
        """
    def validate(self, *, full: bool = False) -> None:
        """
        Perform validation checks.  An exception is raised if validation fails.

        By default only cheap validation checks are run.  Pass `full=True`
        for thorough validation checks (potentially O(n)).

        Parameters
        ----------
        full : bool, default False
            If True, run expensive checks, otherwise cheap checks only.

        Raises
        ------
        ArrowInvalid
        """
    def equals(self, other: Scalar) -> bool: ...
    def __hash__(self) -> int: ...
    def as_py(self: Scalar[Any], *, maps_as_pydicts: Literal["lossy", "strict"] | None = None) -> Any:
        """
        Return this value as a Python representation.

        Parameters
        ----------
        maps_as_pydicts : str, optional, default `None`
            Valid values are `None`, 'lossy', or 'strict'.
            The default behavior (`None`), is to convert Arrow Map arrays to
            Python association lists (list-of-tuples) in the same order as the
            Arrow Map, as in [(key1, value1), (key2, value2), ...].

            If 'lossy' or 'strict', convert Arrow Map arrays to native Python dicts.

            If 'lossy', whenever duplicate keys are detected, a warning will be printed.
            The last seen value of a duplicate key will be in the Python dictionary.
            If 'strict', this instead results in an exception being raised when detected.
        """

_NULL: TypeAlias = None
NA = _NULL

class NullScalar(Scalar[types.NullType]): ...
class BooleanScalar(Scalar[types.BoolType]): ...
class UInt8Scalar(Scalar[types.UInt8Type]): ...
class Int8Scalar(Scalar[types.Int8Type]): ...
class UInt16Scalar(Scalar[types.UInt16Type]): ...
class Int16Scalar(Scalar[types.Int16Type]): ...
class UInt32Scalar(Scalar[types.Uint32Type]): ...
class Int32Scalar(Scalar[types.Int32Type]): ...
class UInt64Scalar(Scalar[types.UInt64Type]): ...
class Int64Scalar(Scalar[types.Int64Type]): ...
class HalfFloatScalar(Scalar[types.Float16Type]): ...
class FloatScalar(Scalar[types.Float32Type]): ...
class DoubleScalar(Scalar[types.Float64Type]): ...
class Decimal32Scalar(Scalar[types.Decimal32Type[types._Precision, types._Scale]]): ...
class Decimal64Scalar(Scalar[types.Decimal64Type[types._Precision, types._Scale]]): ...
class Decimal128Scalar(Scalar[types.Decimal128Type[types._Precision, types._Scale]]): ...
class Decimal256Scalar(Scalar[types.Decimal256Type[types._Precision, types._Scale]]): ...
class Date32Scalar(Scalar[types.Date32Type]): ...

class Date64Scalar(Scalar[types.Date64Type]):
    @property
    def value(self) -> dt.date | None: ...

class Time32Scalar(Scalar[types.Time32Type[_Time32Unit]]):
    @property
    def value(self) -> dt.time | None: ...

class Time64Scalar(Scalar[types.Time64Type[_Time64Unit]]):
    @property
    def value(self) -> dt.time | None: ...

class TimestampScalar(Scalar[types.TimestampType[_Unit, _Tz]]):
    @property
    def value(self) -> int | None: ...

class DurationScalar(Scalar[types.DurationType[_Unit]]):
    @property
    def value(self) -> dt.timedelta | None: ...

class MonthDayNanoIntervalScalar(Scalar[types.MonthDayNanoIntervalType]):
    @property
    def value(self) -> MonthDayNano | None: ...

class BinaryScalar(Scalar[types.BinaryType]):
    def as_buffer(self) -> Buffer: ...

class LargeBinaryScalar(Scalar[types.LargeBinaryType]):
    def as_buffer(self) -> Buffer: ...

class FixedSizeBinaryScalar(Scalar[types.FixedSizeBinaryType]):
    def as_buffer(self) -> Buffer: ...

class StringScalar(Scalar[types.StringType]):
    def as_buffer(self) -> Buffer: ...

class LargeStringScalar(Scalar[types.LargeStringType]):
    def as_buffer(self) -> Buffer: ...

class BinaryViewScalar(Scalar[types.BinaryViewType]):
    def as_buffer(self) -> Buffer: ...

class StringViewScalar(Scalar[types.StringViewType]):
    def as_buffer(self) -> Buffer: ...

class ListScalar(Scalar[types.ListType[_DataTypeT]]):
    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int: ...
    def __getitem__(self, i: int) -> Scalar[_DataTypeT]: ...
    def __iter__(self) -> Iterator[Array]: ...

class FixedSizeListScalar(Scalar[types.FixedSizeListType[_DataTypeT, types._Size]]):
    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int: ...
    def __getitem__(self, i: int) -> Scalar[_DataTypeT]: ...
    def __iter__(self) -> Iterator[Array]: ...

class LargeListScalar(Scalar[types.LargeListType[_DataTypeT]]):
    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int: ...
    def __getitem__(self, i: int) -> Scalar[_DataTypeT]: ...
    def __iter__(self) -> Iterator[Array]: ...

class ListViewScalar(Scalar[types.ListViewType[_DataTypeT]]):
    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int: ...
    def __getitem__(self, i: int) -> Scalar[_DataTypeT]: ...
    def __iter__(self) -> Iterator[Array]: ...

class LargeListViewScalar(Scalar[types.LargeListViewType[_DataTypeT]]):
    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int: ...
    def __getitem__(self, i: int) -> Scalar[_DataTypeT]: ...
    def __iter__(self) -> Iterator[Array]: ...

class StructScalar(Scalar[types.StructType], collections.abc.Mapping[str, Scalar]):
    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[str]: ...
    def __getitem__(self, __key: str) -> Scalar[Any]: ...  # type: ignore[override]
    def _as_py_tuple(self) -> list[tuple[str, Any]]: ...

class MapScalar(Scalar[types.MapType[types._K, types._ValueT]]):
    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int: ...
    def __getitem__(self, i: int) -> tuple[Scalar[types._K], types._ValueT, Any]: ...
    @overload
    def __iter__(
        self: Scalar[
            types.MapType[types._BasicDataType[_AsPyTypeK], types._BasicDataType[_AsPyTypeV]]
        ],
    ) -> Iterator[tuple[_AsPyTypeK, _AsPyTypeV]]: ...
    @overload
    def __iter__(
        self: Scalar[types.MapType[Any, types._BasicDataType[_AsPyTypeV]],],
    ) -> Iterator[tuple[Any, _AsPyTypeV]]: ...
    @overload
    def __iter__(
        self: Scalar[types.MapType[types._BasicDataType[_AsPyTypeK], Any],],
    ) -> Iterator[tuple[_AsPyTypeK, Any]]: ...

class DictionaryScalar(Scalar[types.DictionaryType[types._IndexT, types._BasicValueT]]):
    @property
    def index(self) -> Scalar[types._IndexT]: ...
    @property
    def value(self) -> Scalar[types._BasicValueT]: ...
    @property
    def dictionary(self) -> Array: ...

class RunEndEncodedScalar(Scalar[types.RunEndEncodedType[types._RunEndType, types._BasicValueT]]):
    @property
    def value(self) -> tuple[int, types._BasicValueT] | None: ...

class UnionScalar(Scalar[types.UnionType]):
    @property
    def value(self) -> Any | None: ...
    @property
    def type_code(self) -> str: ...

class ExtensionScalar(Scalar[types.ExtensionType]):
    @property
    def value(self) -> Any | None: ...
    @staticmethod
    def from_storage(typ: types.BaseExtensionType, value) -> ExtensionScalar:
        """
        Construct ExtensionScalar from type and storage value.

        Parameters
        ----------
        typ : DataType
            The extension type for the result scalar.
        value : object
            The storage value for the result scalar.

        Returns
        -------
        ext_scalar : ExtensionScalar
        """

class Bool8Scalar(Scalar[types.Bool8Type]): ...
class UuidScalar(Scalar[types.UuidType]): ...
class JsonScalar(Scalar[types.JsonType]): ...
class OpaqueScalar(Scalar[types.OpaqueType]): ...

class FixedShapeTensorScalar(ExtensionScalar):
    def to_numpy(self) -> np.ndarray:
        """
        Convert fixed shape tensor scalar to a numpy.ndarray.

        The resulting ndarray's shape matches the permuted shape of the
        fixed shape tensor scalar.
        The conversion is zero-copy.

        Returns
        -------
        numpy.ndarray
        """
    def to_tensor(self) -> Tensor:
        """
        Convert fixed shape tensor extension scalar to a pyarrow.Tensor, using shape
        and strides derived from corresponding FixedShapeTensorType.

        The conversion is zero-copy.

        Returns
        -------
        pyarrow.Tensor
            Tensor represented stored in FixedShapeTensorScalar.
        """

_V = TypeVar("_V", covariant=True)

class NullableCollection(Protocol[_V]):  # pyright: ignore[reportInvalidTypeVarUse]
    def __iter__(self) -> Iterator[_V] | Iterator[_V | None]: ...
    def __len__(self) -> int: ...
    def __contains__(self, item: Any, /) -> bool: ...

def scalar(
    value: Any,
    type: _DataTypeT,
    *,
    from_pandas: bool | None = None,
    memory_pool: MemoryPool | None = None,
) -> Scalar[_DataTypeT]:
    """
    Create a pyarrow.Scalar instance from a Python object.

    Parameters
    ----------
    value : Any
        Python object coercible to arrow's type system.
    type : pyarrow.DataType
        Explicit type to attempt to coerce to, otherwise will be inferred from
        the value.
    from_pandas : bool, default None
        Use pandas's semantics for inferring nulls from values in
        ndarray-like data. Defaults to False if not passed explicitly by user,
        or True if a pandas object is passed in.
    memory_pool : pyarrow.MemoryPool, optional
        If not passed, will allocate memory from the currently-set default
        memory pool.

    Returns
    -------
    scalar : pyarrow.Scalar

    Examples
    --------
    >>> import pyarrow as pa

    >>> pa.scalar(42)
    <pyarrow.Int64Scalar: 42>

    >>> pa.scalar("string")
    <pyarrow.StringScalar: 'string'>

    >>> pa.scalar([1, 2])
    <pyarrow.ListScalar: [1, 2]>

    >>> pa.scalar([1, 2], type=pa.list_(pa.int16()))
    <pyarrow.ListScalar: [1, 2]>
    """

__all__ = [
    "Scalar",
    "_NULL",
    "NA",
    "NullScalar",
    "BooleanScalar",
    "UInt8Scalar",
    "Int8Scalar",
    "UInt16Scalar",
    "Int16Scalar",
    "UInt32Scalar",
    "Int32Scalar",
    "UInt64Scalar",
    "Int64Scalar",
    "HalfFloatScalar",
    "FloatScalar",
    "DoubleScalar",
    "Decimal32Scalar",
    "Decimal64Scalar",
    "Decimal128Scalar",
    "Decimal256Scalar",
    "Date32Scalar",
    "Date64Scalar",
    "Time32Scalar",
    "Time64Scalar",
    "TimestampScalar",
    "DurationScalar",
    "MonthDayNanoIntervalScalar",
    "BinaryScalar",
    "LargeBinaryScalar",
    "FixedSizeBinaryScalar",
    "StringScalar",
    "LargeStringScalar",
    "BinaryViewScalar",
    "StringViewScalar",
    "ListScalar",
    "FixedSizeListScalar",
    "LargeListScalar",
    "ListViewScalar",
    "LargeListViewScalar",
    "StructScalar",
    "MapScalar",
    "DictionaryScalar",
    "RunEndEncodedScalar",
    "UnionScalar",
    "ExtensionScalar",
    "FixedShapeTensorScalar",
    "Bool8Scalar",
    "UuidScalar",
    "JsonScalar",
    "OpaqueScalar",
    "scalar",
]
