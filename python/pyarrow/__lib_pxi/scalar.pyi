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
from typing import Any, Generic, Iterator, Literal

import numpy as np

from pyarrow._compute import CastOptions  # type: ignore[import-not-found]
from pyarrow.lib import Array, Buffer, MemoryPool, MonthDayNano, Tensor, _Weakrefable
from typing_extensions import  TypeVar

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
    def equals(self, other: Scalar) -> bool:
        """
        Parameters
        ----------
        other : pyarrow.Scalar

        Returns
        -------
        bool
        """
    def __hash__(self) -> int:
        """
        Return hash(self).
        """
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

class NullScalar(Scalar[types.NullType]):
    """
    Concrete class for null scalars.
    """
class BooleanScalar(Scalar[types.BoolType]):
    """
    Concrete class for boolean scalars.
    """
class UInt8Scalar(Scalar[types.UInt8Type]):
    """
    Concrete class for uint8 scalars.
    """
class Int8Scalar(Scalar[types.Int8Type]):
    """
    Concrete class for int8 scalars.
    """
class UInt16Scalar(Scalar[types.UInt16Type]):
    """
    Concrete class for uint16 scalars.
    """
class Int16Scalar(Scalar[types.Int16Type]):
    """
    Concrete class for int16 scalars.
    """
class UInt32Scalar(Scalar[types.Uint32Type]):
    """
    Concrete class for uint32 scalars.
    """
class Int32Scalar(Scalar[types.Int32Type]):
    """
    Concrete class for int32 scalars.
    """
class UInt64Scalar(Scalar[types.UInt64Type]):
    """
    Concrete class for uint64 scalars.
    """
class Int64Scalar(Scalar[types.Int64Type]):
    """
    Concrete class for int64 scalars.
    """
class HalfFloatScalar(Scalar[types.Float16Type]):
    """
    Concrete class for float scalars.
    """
class FloatScalar(Scalar[types.Float32Type]):
    """
    Concrete class for float scalars.
    """
class DoubleScalar(Scalar[types.Float64Type]):
    """
    Concrete class for double scalars.
    """
class Decimal32Scalar(Scalar[types.Decimal32Type[types._Precision, types._Scale]]):
    """
    Concrete class for decimal32 scalars.
    """
class Decimal64Scalar(Scalar[types.Decimal64Type[types._Precision, types._Scale]]):
    """
    Concrete class for decimal64 scalars.
    """
class Decimal128Scalar(Scalar[types.Decimal128Type[types._Precision, types._Scale]]):
    """
    Concrete class for decimal128 scalars.
    """
class Decimal256Scalar(Scalar[types.Decimal256Type[types._Precision, types._Scale]]):
    """
    Concrete class for decimal256 scalars.
    """
class Date32Scalar(Scalar[types.Date32Type]):
    """
    Concrete class for date32 scalars.
    """

class Date64Scalar(Scalar[types.Date64Type]):
    """
    Concrete class for date64 scalars.
    """
    @property
    def value(self) -> dt.date | None: ...

class Time32Scalar(Scalar[types.Time32Type[_Time32Unit]]):
    """
    Concrete class for time32 scalars.
    """
    @property
    def value(self) -> dt.time | None: ...

class Time64Scalar(Scalar[types.Time64Type[_Time64Unit]]):
    """
    Concrete class for time64 scalars.
    """
    @property
    def value(self) -> dt.time | None: ...

class TimestampScalar(Scalar[types.TimestampType[_Unit, _Tz]]):
    """
    Concrete class for timestamp scalars.
    """
    @property
    def value(self) -> int | None: ...

class DurationScalar(Scalar[types.DurationType[_Unit]]):
    """
    Concrete class for duration scalars.
    """
    @property
    def value(self) -> dt.timedelta | None: ...

class MonthDayNanoIntervalScalar(Scalar[types.MonthDayNanoIntervalType]):
    """
    Concrete class for month, day, nanosecond interval scalars.
    """
    @property
    def value(self) -> MonthDayNano | None:
        """
        Same as self.as_py()
        """

class BinaryScalar(Scalar[types.BinaryType]):
    """
    Concrete class for binary-like scalars.
    """
    def as_buffer(self) -> Buffer:
        """
        Return a view over this value as a Buffer object.
        """

class LargeBinaryScalar(Scalar[types.LargeBinaryType]):
    """
    """
    def as_buffer(self) -> Buffer:
        """
        BinaryScalar.as_buffer(self)

        Return a view over this value as a Buffer object.
        """

class FixedSizeBinaryScalar(Scalar[types.FixedSizeBinaryType]):
    """
    """
    def as_buffer(self) -> Buffer:
        """
        BinaryScalar.as_buffer(self)

        Return a view over this value as a Buffer object.
        """

class StringScalar(Scalar[types.StringType]):
    """
    Concrete class for string-like (utf8) scalars.
    """
    def as_buffer(self) -> Buffer:
        """
        BinaryScalar.as_buffer(self)

        Return a view over this value as a Buffer object.
        """

class LargeStringScalar(Scalar[types.LargeStringType]):
    """
    """
    def as_buffer(self) -> Buffer:
        """
        BinaryScalar.as_buffer(self)

        Return a view over this value as a Buffer object.
        """

class BinaryViewScalar(Scalar[types.BinaryViewType]):
    """
    """
    def as_buffer(self) -> Buffer:
        """
        BinaryScalar.as_buffer(self)

        Return a view over this value as a Buffer object.
        """

class StringViewScalar(Scalar[types.StringViewType]):
    """
    """
    def as_buffer(self) -> Buffer:
        """
        BinaryScalar.as_buffer(self)

        Return a view over this value as a Buffer object.
        """

class ListScalar(Scalar[types.ListType[_DataTypeT]]):
    """
    Concrete class for list-like scalars.
    """
    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int:
        """
        Return the number of values.
        """
    def __getitem__(self, i: int) -> Scalar[_DataTypeT]:
        """
        Return the value at the given index.
        """
    def __iter__(self) -> Iterator[Array]:
        """
        Iterate over this element's values.
        """

class FixedSizeListScalar(Scalar[types.FixedSizeListType[_DataTypeT, types._Size]]):
    """
    """
    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int:
        """
        ListScalar.__len__(self)

        Return the number of values.
        """
    def __getitem__(self, i: int) -> Scalar[_DataTypeT]:
        """
        ListScalar.__getitem__(self, i)

        Return the value at the given index.
        """
    def __iter__(self) -> Iterator[Array]:
        """
        ListScalar.__iter__(self)

        Iterate over this element's values.
        """

class LargeListScalar(Scalar[types.LargeListType[_DataTypeT]]):
    """
    """
    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int:
        """
        ListScalar.__len__(self)

        Return the number of values.
        """
    def __getitem__(self, i: int) -> Scalar[_DataTypeT]:
        """
        ListScalar.__getitem__(self, i)

        Return the value at the given index.
        """
    def __iter__(self) -> Iterator[Array]:
        """
        ListScalar.__iter__(self)

        Iterate over this element's values.
        """

class ListViewScalar(Scalar[types.ListViewType[_DataTypeT]]):
    """
    """
    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int:
        """
        ListScalar.__len__(self)

        Return the number of values.
        """
    def __getitem__(self, i: int) -> Scalar[_DataTypeT]:
        """
        ListScalar.__getitem__(self, i)

        Return the value at the given index.
        """
    def __iter__(self) -> Iterator[Array]:
        """
        ListScalar.__iter__(self)

        Iterate over this element's values.
        """

class LargeListViewScalar(Scalar[types.LargeListViewType[_DataTypeT]]):
    """
    """
    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int:
        """
        ListScalar.__len__(self)

        Return the number of values.
        """
    def __getitem__(self, i: int) -> Scalar[_DataTypeT]:
        """
        ListScalar.__getitem__(self, i)

        Return the value at the given index.
        """
    def __iter__(self) -> Iterator[Array]:
        """
        ListScalar.__iter__(self)

        Iterate over this element's values.
        """

class StructScalar(Scalar[types.StructType], collections.abc.Mapping[str, Scalar]):
    """
    Concrete class for struct scalars.
    """
    def __len__(self) -> int:
        """
        Return len(self).
        """
    def __iter__(self) -> Iterator[str]:
        """
        Implement iter(self).
        """
    def __getitem__(self, key: int | str) -> Scalar[Any]:
        """
        Return the child value for the given field.

        Parameters
        ----------
        key : Union[int, str]
            Index / position or name of the field.

        Returns
        -------
        result : Scalar
        """
    def _as_py_tuple(self) -> list[tuple[str, Any]]: ...

class MapScalar(Scalar[types.MapType[types._K, types._ValueT]]):
    """
    Concrete class for map scalars.
    """
    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int:
        """
        ListScalar.__len__(self)

        Return the number of values.
        """
    def __getitem__(self, i: int) -> tuple[Scalar[types._K], types._ValueT, Any]:
        """
        Return the value at the given index or key.
        """
    def __iter__(
        self: Scalar[
            types.MapType[types._BasicDataType[_AsPyTypeK], types._BasicDataType[_AsPyTypeV]],]
            | Scalar[types.MapType[Any, types._BasicDataType[_AsPyTypeV]]]
            | Scalar[types.MapType[types._BasicDataType[_AsPyTypeK], Any]]
    ) -> Iterator[tuple[_AsPyTypeK, _AsPyTypeV]] | Iterator[tuple[Any, _AsPyTypeV]] | Iterator[tuple[_AsPyTypeK, Any]]:
        """
        Iterate over this element's values.
        """

class DictionaryScalar(Scalar[types.DictionaryType[types._IndexT, types._BasicValueT]]):
    """
    Concrete class for dictionary-encoded scalars.
    """
    @property
    def index(self) -> Scalar[types._IndexT]:
        """
        Return this value's underlying index as a scalar.
        """
    @property
    def value(self) -> Scalar[types._BasicValueT]:
        """
        Return the encoded value as a scalar.
        """
    @property
    def dictionary(self) -> Array: ...

class RunEndEncodedScalar(Scalar[types.RunEndEncodedType[types._RunEndType, types._BasicValueT]]):
    """
    Concrete class for RunEndEncoded scalars.
    """
    @property
    def value(self) -> tuple[int, types._BasicValueT] | None:
        """
        Return underlying value as a scalar.
        """

class UnionScalar(Scalar[types.UnionType]):
    """
    Concrete class for Union scalars.
    """
    @property
    def value(self) -> Any | None:
        """
        Return underlying value as a scalar.
        """
    @property
    def type_code(self) -> str:
        """
        Return the union type code for this scalar.
        """

class ExtensionScalar(Scalar[types.ExtensionType]):
    """
    Concrete class for Extension scalars.
    """
    @property
    def value(self) -> Any | None:
        """
        Return storage value as a scalar.
        """
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

class Bool8Scalar(Scalar[types.Bool8Type]):
    """
    Concrete class for bool8 extension scalar.
    """
class UuidScalar(Scalar[types.UuidType]):
    """
    Concrete class for Uuid extension scalar.
    """
class JsonScalar(Scalar[types.JsonType]):
    """
    Concrete class for JSON extension scalar.
    """
class OpaqueScalar(Scalar[types.OpaqueType]):
    """
    Concrete class for opaque extension scalar.
    """

class FixedShapeTensorScalar(ExtensionScalar):
    """
    Concrete class for fixed shape tensor extension scalar.
    """
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
