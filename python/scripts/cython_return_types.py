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

"""Derive return types of Cython ``def`` functions from their sources.

stubgen-pyx carries declared parameter types into the stubs, but Cython
functions rarely declare what they return. Their return statements are
regular enough to read: wrapper calls such as ``pyarrow_wrap_table(...)``,
constructors and ``cls(...)``, ``self``, literals, builtins, and locals that
were declared ``cdef Table result`` or assigned from one of those. A function
without a return value returns None, unless its body raises unconditionally,
which marks an abstract method whose overrides return something else.

Types are reported by the names used in the sources; the caller resolves
them against the stub they are written into.
"""

import re

from Cython.Compiler import ExprNodes, Nodes
from Cython.Compiler.Visitor import TreeVisitor
from stubgen_pyx.parsing.parser import parse_pyx

WRAPPERS = {"array": "Array", "chunked_array": "ChunkedArray", "table": "Table",
            "batch": "RecordBatch", "schema": "Schema", "field": "Field",
            "data_type": "DataType", "buffer": "Buffer",
            "resizable_buffer": "ResizableBuffer", "tensor": "Tensor",
            "scalar": "Scalar", "metadata": "KeyValueMetadata",
            "sparse_coo_tensor": "SparseCOOTensor",
            "sparse_csr_matrix": "SparseCSRMatrix",
            "sparse_csc_matrix": "SparseCSCMatrix",
            "sparse_csf_tensor": "SparseCSFTensor"}
HELPERS = {"frombytes": "str", "tobytes": "bytes", "primitive_type": "DataType",
           "isinstance": "bool", "callable": "bool", "len": "int", "str": "str",
           "repr": "str", "int": "int", "float": "float", "bool": "bool",
           "bytes": "bytes", "list": "list", "sorted": "list", "dict": "dict",
           "tuple": "tuple", "set": "set", "frozenset": "frozenset"}
LITERALS = {ExprNodes.NoneNode: "None", ExprNodes.BoolNode: "bool",
            ExprNodes.IntNode: "int", ExprNodes.FloatNode: "float",
            ExprNodes.UnicodeNode: "str", ExprNodes.JoinedStrNode: "str",
            ExprNodes.BytesNode: "bytes", ExprNodes.ListNode: "list",
            ExprNodes.DictNode: "dict", ExprNodes.TupleNode: "tuple",
            ExprNodes.SetNode: "set", ExprNodes.PrimaryCmpNode: "bool",
            ExprNodes.NotNode: "bool"}
C_SCALARS = {"bint": "bool", "double": "float", "float": "float",
             "c_string": "bytes", "unicode": "str"}
C_INTEGER = re.compile(
    r"^(u?int(8|16|32|64)?_t|unsigned .*|int|long|size_t|Py_ssize_t)$")
SKIPPED = ("__init__", "__cinit__", "__dealloc__")


def python_type(c_type, classes):
    """The Python type a C type converts to, or None."""
    if c_type in C_SCALARS:
        return C_SCALARS[c_type]
    if C_INTEGER.match(c_type):
        return "int"
    return c_type if c_type in classes else None


class Returns(TreeVisitor):
    """Collect the return types of def functions in one module."""

    def __init__(self, classes):
        super().__init__()
        self.classes = classes
        self.found = {}
        self.owner = None
        self.function = None

    def visit_Node(self, node):
        self.visitchildren(node)

    def visit_CClassDefNode(self, node):
        self.owner = node.class_name
        self.visitchildren(node)
        self.owner = None

    def visit_PyClassDefNode(self, node):
        self.owner = node.name
        self.visitchildren(node)
        self.owner = None

    def visit_DefNode(self, node):
        if self.function is not None or node.name in SKIPPED:
            return  # nested function, or a constructor
        self.function = (self.owner, node.name)
        self.locals = {}
        self.returns, self.yields = [], []
        self.visitchildren(node)
        statements = getattr(node.body, "stats", [node.body])
        raises = any(isinstance(s, Nodes.RaiseStatNode) for s in statements)
        self.found[self.function] = self.result(raises)
        self.function = None

    def result(self, raises):
        if self.yields:
            inner = self.union(self.yields) or "Any"
            return f"Iterator[{inner}]"
        if not self.returns:
            return None if raises else "None"
        return self.union(self.returns)

    @staticmethod
    def union(types):
        return " | ".join(dict.fromkeys(types)) if None not in types else None

    def visit_CVarDefNode(self, node):  # cdef Table result
        if self.function and isinstance(node.base_type, Nodes.CSimpleBaseTypeNode):
            declared = python_type(node.base_type.name, self.classes)
            for declarator in node.declarators:
                if declared and isinstance(declarator, Nodes.CNameDeclaratorNode):
                    self.locals[declarator.name] = declared
        self.visitchildren(node)

    def visit_SingleAssignmentNode(self, node):  # result = pyarrow_wrap_table(...)
        if self.function and isinstance(node.lhs, ExprNodes.NameNode):
            assigned = self.classify(node.rhs)
            if assigned and node.lhs.name not in self.locals:
                self.locals[node.lhs.name] = assigned
        self.visitchildren(node)

    def visit_ReturnStatNode(self, node):
        if self.function:
            value = "None" if node.value is None else self.classify(node.value)
            self.returns.append(value)

    def visit_YieldExprNode(self, node):
        if self.function:
            self.yields.append(self.classify(node.arg) if node.arg else None)
        self.visitchildren(node)

    def classify(self, node):
        """The type of an expression, or None when it cannot be told."""
        for kind, name in LITERALS.items():
            if isinstance(node, kind):
                return name
        if isinstance(node, ExprNodes.ComprehensionNode):
            return getattr(node.type, "name", None)
        if isinstance(node, ExprNodes.NameNode):
            if node.name == "self":
                return self.function[0]
            if node.name == "cls":
                return "Self"
            return self.locals.get(node.name)
        if isinstance(node, ExprNodes.TypecastNode):
            if isinstance(node.base_type, Nodes.CSimpleBaseTypeNode):
                return python_type(node.base_type.name, self.classes)
        if isinstance(node, ExprNodes.CondExprNode):
            return self.union([self.classify(node.true_val),
                               self.classify(node.false_val)])
        if isinstance(node, (ExprNodes.SimpleCallNode, ExprNodes.GeneralCallNode)):
            return self.classify_call(node.function)
        return None

    def classify_call(self, function):
        if isinstance(function, ExprNodes.NameNode):
            name = function.name
            if name.startswith("pyarrow_wrap_"):
                return WRAPPERS.get(name[len("pyarrow_wrap_"):])
            if name == "cls":
                return "Self"
            return HELPERS.get(name) or (name if name in self.classes else None)
        if isinstance(function, ExprNodes.AttributeNode):
            owner, method = function.obj, function.attribute
            if isinstance(owner, ExprNodes.UnicodeNode) and method == "join":
                return "str"
            if not isinstance(owner, ExprNodes.NameNode):
                return None
            constructs = (method in ("wrap", "_wrap", "__new__")
                          or method.startswith("from_"))
            if owner.name == "cls" and constructs:
                return "Self"
            if owner.name in self.classes and constructs:
                return owner.name
        return None


def module_classes(parsed):
    body = getattr(parsed.source_ast.body, "stats", [])
    return ({n.class_name for n in body if isinstance(n, Nodes.CClassDefNode)}
            | {n.name for n in body if isinstance(n, Nodes.PyClassDefNode)})


def return_types(paths):
    """Map ``pyarrow.<module>`` to ``{(class, function): type}`` for the
    functions in the given .pyx files whose return type can be read."""
    parsed = {path: parse_pyx(path.read_text(), pyx_path=path) for path in paths}
    classes = set().union(*(module_classes(p) for p in parsed.values()))
    result = {}
    for path, source in parsed.items():
        visitor = Returns(classes)
        visitor.visit(source.source_ast)
        result[f"pyarrow.{path.stem}"] = {
            key: value for key, value in visitor.found.items() if value}
    return result
