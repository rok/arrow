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

from typing import Any, Callable

from ._compute import Expression
from .lib import Buffer, RecordBatchReader, Schema, Table, _Weakrefable

def run_query(
    plan: Buffer | int,
    *,
    table_provider: Callable[[list[str], Schema], Table] | None = None,
    use_threads: bool = True,
) -> RecordBatchReader: ...
def _parse_json_plan(plan: bytes) -> Buffer: ...

class SubstraitSchema:
    schema: Schema
    expression: Expression
    def __init__(self, schema: Schema, expression: Expression) -> None: ...
    def to_pysubstrait(self) -> Any: ...

def serialize_schema(schema: Schema) -> SubstraitSchema: ...
def deserialize_schema(buf: Buffer | bytes) -> Schema: ...
def serialize_expressions(
    exprs: list[Expression],
    names: list[str],
    schema: Schema,
    *,
    allow_arrow_extensions: bool = False,
) -> Buffer: ...

class BoundExpressions(_Weakrefable):
    @property
    def schema(self) -> Schema: ...
    @property
    def expressions(self) -> dict[str, Expression]: ...
    @classmethod
    def from_substrait(cls, message: Buffer | bytes) -> BoundExpressions: ...

def deserialize_expressions(buf: Buffer | bytes) -> BoundExpressions: ...
def get_supported_functions() -> list[str]: ...
