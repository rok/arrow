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

from typing import Iterable

from pyarrow.lib import MemoryPool, _Weakrefable

from .array import StringArray, StringViewArray

class StringBuilder(_Weakrefable):
    """
    Builder class for UTF8 strings.

    This class exposes facilities for incrementally adding string values and
    building the null bitmap for a pyarrow.Array (type='string').
    """
    def __init__(self, memory_pool: MemoryPool | None = None) -> None: ...
    def append(self, value: str | bytes | None):
        """
        Append a single value to the builder.

        The value can either be a string/bytes object or a null value
        (np.nan or None).

        Parameters
        ----------
        value : string/bytes or np.nan/None
            The value to append to the string array builder.
        """
    def append_values(self, values: Iterable[str | bytes | None]):
        """
        Append all the values from an iterable.

        Parameters
        ----------
        values : iterable of string/bytes or np.nan/None values
            The values to append to the string array builder.
        """
    def finish(self) -> StringArray:
        """
        Return result of builder as an Array object; also resets the builder.

        Returns
        -------
        array : pyarrow.Array
        """
    @property
    def null_count(self) -> int: ...
    def __len__(self) -> int: ...

class StringViewBuilder(_Weakrefable):
    """
    Builder class for UTF8 string views.

    This class exposes facilities for incrementally adding string values and
    building the null bitmap for a pyarrow.Array (type='string_view').
    """
    def __init__(self, memory_pool: MemoryPool | None = None) -> None: ...
    def append(self, value: str | bytes | None):
        """
        Append a single value to the builder.

        The value can either be a string/bytes object or a null value
        (np.nan or None).

        Parameters
        ----------
        value : string/bytes or np.nan/None
            The value to append to the string array builder.
        """
    def append_values(self, values: Iterable[str | bytes | None]):
        """
        Append all the values from an iterable.

        Parameters
        ----------
        values : iterable of string/bytes or np.nan/None values
            The values to append to the string array builder.
        """
    def finish(self) -> StringViewArray:
        """
        Return result of builder as an Array object; also resets the builder.

        Returns
        -------
        array : pyarrow.Array
        """
    @property
    def null_count(self) -> int: ...
    def __len__(self) -> int: ...

__all__ = ["StringBuilder", "StringViewBuilder"]
