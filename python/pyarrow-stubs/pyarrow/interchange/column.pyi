import enum
import pyarrow as pa
from _typeshed import Incomplete
from pyarrow.interchange.buffer import _PyArrowBuffer as _PyArrowBuffer
from typing import Any, Iterable, TypedDict

class DtypeKind(enum.IntEnum):
    INT = 0
    UINT = 1
    FLOAT = 2
    BOOL = 20
    STRING = 21
    DATETIME = 22
    CATEGORICAL = 23
Dtype = tuple[DtypeKind, int, str, str]
_PYARROW_KINDS: Incomplete

class ColumnNullType(enum.IntEnum):
    NON_NULLABLE = 0
    USE_NAN = 1
    USE_SENTINEL = 2
    USE_BITMASK = 3
    USE_BYTEMASK = 4

class ColumnBuffers(TypedDict):
    data: tuple[_PyArrowBuffer, Dtype]
    validity: tuple[_PyArrowBuffer, Dtype] | None
    offsets: tuple[_PyArrowBuffer, Dtype] | None

class CategoricalDescription(TypedDict):
    is_ordered: bool
    is_dictionary: bool
    categories: _PyArrowColumn | None

class Endianness:
    LITTLE: str
    BIG: str
    NATIVE: str
    NA: str

class NoBufferPresent(Exception): ...

class _PyArrowColumn:
    _allow_copy: Incomplete
    _dtype: Incomplete
    _col: Incomplete
    def __init__(self, column: pa.Array | pa.ChunkedArray, allow_copy: bool = True) -> None: ...
    def size(self) -> int: ...
    @property
    def offset(self) -> int: ...
    @property
    def dtype(self) -> tuple[DtypeKind, int, str, str]: ...
    def _dtype_from_arrowdtype(self, dtype: pa.DataType, bit_width: int) -> tuple[DtypeKind, int, str, str]: ...
    @property
    def describe_categorical(self) -> CategoricalDescription: ...
    @property
    def describe_null(self) -> tuple[ColumnNullType, Any]: ...
    @property
    def null_count(self) -> int: ...
    @property
    def metadata(self) -> dict[str, Any]: ...
    def num_chunks(self) -> int: ...
    def get_chunks(self, n_chunks: int | None = None) -> Iterable[_PyArrowColumn]: ...
    def get_buffers(self) -> ColumnBuffers: ...
    def _get_data_buffer(self) -> tuple[_PyArrowBuffer, Any]: ...
    def _get_validity_buffer(self) -> tuple[_PyArrowBuffer, Any]: ...
    def _get_offsets_buffer(self) -> tuple[_PyArrowBuffer, Any]: ...
