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

from typing import Iterable, Literal

from .lib import Array, DataType, Field, MemoryPool, RecordBatch, Schema, _Weakrefable

class Node(_Weakrefable):
    def return_type(self) -> DataType: ...

class Expression(_Weakrefable):
    def root(self) -> Node: ...
    def result(self) -> Field: ...

class Condition(_Weakrefable):
    def root(self) -> Node: ...
    def result(self) -> Field: ...

class SelectionVector(_Weakrefable):
    def to_array(self) -> Array: ...

class Projector(_Weakrefable):
    @property
    def llvm_ir(self): ...
    def evaluate(
        self, batch: RecordBatch, selection: SelectionVector | None = None
    ) -> list[Array]: ...

class Filter(_Weakrefable):
    @property
    def llvm_ir(self): ...
    def evaluate(
        self, batch: RecordBatch, pool: MemoryPool, dtype: DataType | str = "int32"
    ) -> SelectionVector: ...

class TreeExprBuilder(_Weakrefable):
    def make_literal(self, value: float | str | bytes | bool, dtype: DataType) -> Node: ...
    def make_expression(self, root_node: Node, return_field: Field) -> Expression: ...
    def make_function(self, name: str, children: list[Node], return_type: DataType) -> Node: ...
    def make_field(self, field: Field) -> Node: ...
    def make_if(
        self, condition: Node, this_node: Node, else_node: Node, return_type: DataType
    ) -> Node: ...
    def make_and(self, children: list[Node]) -> Node: ...
    def make_or(self, children: list[Node]) -> Node: ...
    def make_in_expression(self, node: Node, values: Iterable, dtype: DataType) -> Node: ...
    def make_condition(self, condition: Node) -> Condition: ...

class Configuration(_Weakrefable):
    def __init__(self, optimize: bool = True, dump_ir: bool = False) -> None: ...

def make_projector(
    schema: Schema,
    children: list[Expression],
    pool: MemoryPool,
    selection_mode: Literal["NONE", "UINT16", "UINT32", "UINT64"] = "NONE",
    configuration: Configuration | None = None,
) -> Projector: ...
def make_filter(
    schema: Schema, condition: Condition, configuration: Configuration | None = None
) -> Filter: ...

class FunctionSignature(_Weakrefable):
    def return_type(self) -> DataType: ...
    def param_types(self) -> list[DataType]: ...
    def name(self) -> str: ...

def get_registered_function_signatures() -> list[FunctionSignature]: ...
