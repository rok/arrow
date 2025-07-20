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

import datetime as dt

from collections.abc import Sequence
from decimal import Decimal
from typing import Any, Collection, Literal, Protocol, TypeAlias, TypeVar

import numpy as np

from numpy.typing import NDArray

from .compute import BooleanArray, IntegerArray

ArrayLike: TypeAlias = Any
ScalarLike: TypeAlias = Any
Order: TypeAlias = Literal["ascending", "descending"]
JoinType: TypeAlias = Literal[
    "left semi",
    "right semi",
    "left anti",
    "right anti",
    "inner",
    "left outer",
    "right outer",
    "full outer",
]
Compression: TypeAlias = Literal[
    "gzip", "bz2", "brotli", "lz4", "lz4_frame", "lz4_raw", "zstd", "snappy"
]
NullEncoding: TypeAlias = Literal["mask", "encode"]
NullSelectionBehavior: TypeAlias = Literal["drop", "emit_null"]
Mask: TypeAlias = Sequence[bool | None] | NDArray[np.bool_] | BooleanArray
Indices: TypeAlias = Sequence[int] | NDArray[np.integer[Any]] | IntegerArray
PyScalar: TypeAlias = (
    bool | int | float | Decimal | str | bytes | dt.date | dt.datetime | dt.time | dt.timedelta
)

_T = TypeVar("_T")
SingleOrList: TypeAlias = list[_T] | _T

class SupportEq(Protocol):
    def __eq__(self, other) -> bool: ...

class SupportLt(Protocol):
    def __lt__(self, other) -> bool: ...

class SupportGt(Protocol):
    def __gt__(self, other) -> bool: ...

class SupportLe(Protocol):
    def __le__(self, other) -> bool: ...

class SupportGe(Protocol):
    def __ge__(self, other) -> bool: ...

FilterTuple: TypeAlias = (
    tuple[str, Literal["=", "==", "!="], SupportEq]
    | tuple[str, Literal["<"], SupportLt]
    | tuple[str, Literal[">"], SupportGt]
    | tuple[str, Literal["<="], SupportLe]
    | tuple[str, Literal[">="], SupportGe]
    | tuple[str, Literal["in", "not in"], Collection]
)

class Buffer(Protocol):
    def __buffer__(self, flags: int, /) -> memoryview: ...

class SupportPyBuffer(Protocol):
    def __buffer__(self, flags: int, /) -> memoryview: ...

class SupportArrowStream(Protocol):
    def __arrow_c_stream__(self, requested_schema=None) -> Any: ...

class SupportArrowArray(Protocol):
    def __arrow_c_array__(self, requested_schema=None) -> Any: ...

class SupportArrowDeviceArray(Protocol):
    def __arrow_c_device_array__(self, requested_schema=None, **kwargs) -> Any: ...

class SupportArrowSchema(Protocol):
    def __arrow_c_schema(self) -> Any: ...
