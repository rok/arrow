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

from typing import IO, Literal

import pandas as pd

from _typeshed import StrPath
from pyarrow._feather import FeatherError
from pyarrow.lib import Table

__all__ = [
    "FeatherError",
    "FeatherDataset",
    "check_chunked_overflow",
    "write_feather",
    "read_feather",
    "read_table",
]

class FeatherDataset:
    path_or_paths: str | list[str]
    validate_schema: bool

    def __init__(self, path_or_paths: str | list[str], validate_schema: bool = True) -> None: ...
    def read_table(self, columns: list[str] | None = None) -> Table: ...
    def validate_schemas(self, piece, table: Table) -> None: ...
    def read_pandas(
        self, columns: list[str] | None = None, use_threads: bool = True
    ) -> pd.DataFrame: ...

def check_chunked_overflow(name: str, col) -> None: ...
def write_feather(
    df: pd.DataFrame | Table,
    dest: StrPath | IO,
    compression: Literal["zstd", "lz4", "uncompressed"] | None = None,
    compression_level: int | None = None,
    chunksize: int | None = None,
    version: Literal[1, 2] = 2,
) -> None: ...
def read_feather(
    source: StrPath | IO,
    columns: list[str] | None = None,
    use_threads: bool = True,
    memory_map: bool = False,
    **kwargs,
) -> pd.DataFrame: ...
def read_table(
    source: StrPath | IO,
    columns: list[str] | None = None,
    memory_map: bool = False,
    use_threads: bool = True,
) -> Table: ...
