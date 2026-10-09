import enum
import pyarrow as pa
from _typeshed import Incomplete

class DlpackDeviceType(enum.IntEnum):
    CPU = 1
    CUDA = 2
    CPU_PINNED = 3
    OPENCL = 4
    VULKAN = 7
    METAL = 8
    VPI = 9
    ROCM = 10

class _PyArrowBuffer:
    _x: Incomplete
    def __init__(self, x: pa.Buffer, allow_copy: bool = True) -> None: ...
    @property
    def bufsize(self) -> int: ...
    @property
    def ptr(self) -> int: ...
    def __dlpack__(self) -> None: ...
    def __dlpack_device__(self) -> tuple[DlpackDeviceType, int | None]: ...
    def __repr__(self) -> str: ...
