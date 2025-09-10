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
# python ../dev/update_stub_docstrings.py -s ./pyarrow/compute.pyi


from pathlib import Path
from textwrap import indent

import click
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
        except KeyError as _:
            # Some cython __init__ symbols can't be found
            # e.g. pyarrow.lib.OSFile.__init__
            parent_name = ".".join(self.stack[:-1])

            try:
                obj = self.package.get_member(parent_name).all_members[self.stack[-1]]
            except KeyError as _:
                if self.stack[-1] == "__init__" and \
                    hasattr(self.package.get_member(parent_name), "analysis") and self.package.get_member(parent_name).analysis == "dynamic":
                    print(f"{name} expected to not be found in {self.package.name}")
                else:
                    print(f"{name} not found in {self.package.name}")
                return None

        if obj.has_docstring:
            docstring = obj.docstring.value
            # remove signature if present in docstring
            if docstring.startswith(obj.name) or \
                    (hasattr(obj.parent, "name") and docstring.startswith(f"{obj.parent.name}.{obj.name}")):
                return "\n".join(docstring.splitlines()[2:])
            else:
                return docstring
        return None

    def visit_ClassDef(self, node):
        # TODO: class docstrings?
        self.stack.append(node.name.value)
        self.indentation += 1

    def leave_ClassDef(self, original_node, updated_node):
        self.stack.pop()
        self.indentation -= 1
        return updated_node

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

    def leave_FunctionDef(self, original_node, updated_node):
        self.stack.pop()
        self.indentation -= 1
        return updated_node

    def leave_SimpleString(self, original_node, updated_node):
        node_name = ".".join(self.stack)

        if original_node.value == self._docstring:
            indentation = self.indentation * "    "
            docstring = f'"""\n{indent(self._get_docstring(node_name), indentation)}\n{indentation}"""'
            return updated_node.with_changes(value=docstring)

        return updated_node


# def _is_docstring_node(node):
#     """Checks if a node is a docstring."""
#     return (
#         isinstance(node, cst.SimpleStatementLine) and
#         isinstance(node.body[0], cst.Expr) and
#         isinstance(node.body[0].value, cst.SimpleString)
#     )


# class ClonedSignatureDocstringTransformer(cst.CSTTransformer):
#     def __init__(self, docstrings_map, module_name):
#         self.docstrings_map = docstrings_map
#         self.module_name = module_name
#         self.name_of_function = None

#     def leave_Assign(self, original_node, updated_node):
#         target = original_node.targets[0].target
#         value = original_node.value

#         if isinstance(target, cst.Name) and isinstance(value, cst.Call) and \
#                 value.func.value == "_clone_signature":
#             self.name_of_function = f"{self.module_name}.{target.value}"
#         return updated_node

#     def leave_SimpleStatementLine(self, original_node, updated_node):
#         if self.name_of_function:
#             if len(updated_node.body) > 0 and _is_docstring_node(updated_node):
#                 comment_content = self.docstrings_map[self.name_of_function].strip()
#                 self.name_of_function = None

#                 new_string_node = cst.SimpleString(value=f'"""\n{comment_content}\n"""')
#                 new_expr_node = updated_node.body[0].with_changes(value=new_string_node)
#                 new_body = [new_expr_node] + list(updated_node.body[1:])
#                 updated_node = updated_node.with_changes(body=new_body)

#         return updated_node


# class FunctionDocstringTransformer(cst.CSTTransformer):
#     def __init__(self, docstrings_map, module_name):
#         self.docstrings_map = docstrings_map
#         self.module_name = module_name

#     def leave_FunctionDef(self, original_node, updated_node):
#         full_name = f"{self.module_name}.{original_node.name.value}"

#         # Check if we have a docstring for this function
#         if full_name in self.docstrings_map:
#             # Check if the function already has a docstring
#             body_list = list(updated_node.body.body)
#             has_docstring = len(body_list) > 0 and _is_docstring_node(body_list[0])

#             if has_docstring:
#                 # Replace existing docstring
#                 docstring = indent(self.docstrings_map[full_name], "    ").strip()
#                 docstring_value = f'"""\n    {docstring}\n    """'
#                 new_docstring_node = cst.SimpleStatementLine(
#                     body=[cst.Expr(value=cst.SimpleString(value=docstring_value))]
#                 )
#                 new_body = [new_docstring_node] + body_list[1:]
#                 return updated_node.with_changes(
#                     body=updated_node.body.with_changes(body=new_body)
#                 )

#         return updated_node


@click.command()
@click.option('--pyarrow_folder', '-s', type=click.Path(resolve_path=True))
def update_stub_files(pyarrow_folder):
    print("Updating docstrings of stub files in:", pyarrow_folder)
    package = griffe.load("pyarrow", try_relative_path=True, force_inspection=True, resolve_aliases=True)

    for stub_file in Path(pyarrow_folder).rglob('*.pyi'):
        if stub_file.name ==  "_stubs_typing.pyi":
            continue

        print(f"[{stub_file}]")

        with open(stub_file, 'r') as f:
            tree = libcst.parse_module(f.read())

        if stub_file.name !=  "__init__.pyi":
            modified_tree = tree.visit(DocUpdater(package, "lib"))
        else:
            modified_tree = tree.visit(DocUpdater(package, None))
        with open(stub_file, "w") as f:
            f.write(modified_tree.code)


if __name__ == "__main__":
    docstrings_map = {}
    update_stub_files(obj={})
