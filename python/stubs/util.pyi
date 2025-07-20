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

from collections.abc import Callable
from os import PathLike
from typing import Any, Protocol, Sequence, TypeVar

_F = TypeVar("_F", bound=Callable)
_N = TypeVar("_N")

class _DocStringComponents(Protocol):
    _docstring_components: list[str]

def doc(
    *docstrings: str | _DocStringComponents | Callable | None, **params: Any
) -> Callable[[_F], _F]: ...
def _is_iterable(obj) -> bool: ...
def _is_path_like(path) -> bool: ...
def _stringify_path(path: str | PathLike) -> str: ...
def product(seq: Sequence[_N]) -> _N: ...
def get_contiguous_span(
    shape: tuple[int, ...], strides: tuple[int, ...], itemsize: int
) -> tuple[int, int]: ...
def find_free_port() -> int: ...
def guid() -> str: ...
def _download_urllib(url, out_path) -> None: ...
def _download_requests(url, out_path) -> None: ...
def download_tzdata_on_windows() -> None: ...
def _deprecate_api(old_name, new_name, api, next_version, type=...): ...
def _deprecate_class(old_name, new_class, next_version, instancecheck=True): ...
