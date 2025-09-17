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

# Utility to extract docstrings from pyarrow and update
# docstrings in stubfiles.
#
# Usage
# =====
#
# python ../dev/update_stub_docstrings.py -f ./pyarrow/


from pathlib import Path
from textwrap import indent

import click
# TODO: perhaps replace griffe with importlib
import griffe
import libcst


class DocUpdater(libcst.CSTTransformer):
    def __init__(self, package, namespace):
        self.stack = [namespace] if namespace else []
        self._docstring = None
        self.indentation = 0
        self.package = package

    def _get_docstring(self, name):
        # print("extract_docstrings", name)
        try:
            obj = self.package.get_member(name)
        except KeyError:
            # Some cython __init__ symbols can't be found
            # e.g. pyarrow.lib.OSFile.__init__
            parent_name = ".".join(self.stack[:-1])

            try:
                obj = self.package.get_member(parent_name).all_members[self.stack[-1]]
            except KeyError:
                # print(f"{name} not found in {self.package.name}, it's probably ok.")
                return None

        if obj.has_docstring:
            docstring = obj.docstring.value
            # remove signature if present in docstring
            if docstring.startswith(obj.name) or (
                (hasattr(obj.parent, "name") and
                    docstring.startswith(f"{obj.parent.name}.{obj.name}"))):
                return "\n".join(docstring.splitlines()[2:])
            else:
                return docstring
        return None

    def visit_ClassDef(self, node):
        # TODO: class docstrings?
        self.stack.append(node.name.value)
        self.indentation += 1
        node_name = ".".join(self.stack)
        docstring = self._get_docstring(node_name)

        if docstring:
            if not node.get_docstring(clean=False):
                print("Missing docstring (in annotations) for:", node_name)
                return False
            self._docstring = f'"""{node.get_docstring(clean=False)}"""'
            return True
        return False

    def visit_FunctionDef(self, node):
        self.stack.append(node.name.value)
        self.indentation += 1
        node_name = ".".join(self.stack)
        docstring = self._get_docstring(node_name)

        if docstring:
            if not node.get_docstring(clean=False):
                print("Missing docstring (in annotations) for:", node_name)
                return False
            self._docstring = f'"""{node.get_docstring(clean=False)}"""'
            return True
        return False

    def leave_ClassDef(self, original_node, updated_node):
        self.stack.pop()
        self.indentation -= 1
        return updated_node

    def leave_FunctionDef(self, original_node, updated_node):
        self.stack.pop()
        self.indentation -= 1
        return updated_node

    def leave_SimpleString(self, original_node, updated_node):
        node_name = ".".join(self.stack)

        if original_node.value == self._docstring:
            indentation = self.indentation * "    "
            indented_docstring = indent(self._get_docstring(node_name), indentation)
            docstring = f'"""\n{indented_docstring}\n{indentation}"""'
            return updated_node.with_changes(value=docstring)

        return updated_node


class ReplaceEllipsis(libcst.CSTTransformer):
    def __init__(self, package, namespace):
        self.stack = [namespace] if namespace else []
        self.indentation = 0
        self.package = package

    def _get_docstring(self, name, indentation):
        # print(name)
        try:
            obj = self.package.get_member(name)
            if obj.has_docstring:
                indentation_prefix = indentation * "    "
                docstring = indent(obj.docstring.value, indentation_prefix)
                docstring = f'"""\n{docstring}\n{indentation_prefix}"""'
                # print(f"{name} has {len(docstring)} long docstring.")
                return docstring
        except KeyError:
            print(f"{name} has no docstring.")
            return ""

    def visit_FunctionDef(self, node):
        self.stack.append(node.name.value)
        self.indentation += 1

    def leave_FunctionDef(self, original_node, updated_node):
        node_name = ".".join(self.stack)
        indentation = self.indentation
        self.stack.pop()
        self.indentation -= 1

        if isinstance(updated_node.body.body[0].value, libcst.Ellipsis):
            print(node_name)
            docstring = self._get_docstring(node_name, indentation)
            if docstring and len(docstring) > 0:
                new_docstring = libcst.SimpleString(value=docstring)
                new_body = updated_node.body.with_changes(body=[libcst.Expr(value=new_docstring)])
                return updated_node.with_changes(body=new_body)
        return updated_node


@click.command()
@click.option('--pyarrow_folder', '-f', type=click.Path(resolve_path=True))
def update_stub_files(pyarrow_folder):
    print("Updating docstrings of stub files in:", pyarrow_folder)
    package = griffe.load("pyarrow", try_relative_path=True,
                          force_inspection=True, resolve_aliases=True)

    for stub_file in Path(pyarrow_folder).rglob('*.pyi'):
        if stub_file.name == "_stubs_typing.pyi":
            continue

        print(f"[{stub_file}]")

        with open(stub_file, 'r') as f:
            tree = libcst.parse_module(f.read())

        if stub_file.name != "__init__.pyi":
            modified_tree = tree.visit(DocUpdater(package, "lib"))
        else:
            modified_tree = tree.visit(DocUpdater(package, None))
        with open(stub_file, "w") as f:
            f.write(modified_tree.code)


if __name__ == "__main__":
    docstrings_map = {}
    update_stub_files(obj={})
