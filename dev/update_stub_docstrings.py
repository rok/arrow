# Utility to extract docstrings from pyarrow and update
# docstrings in stubfiles.
#
# Usage
# =====
#
# python ../dev/update_stub_docstrings.py -s ./pyarrow/compute.pyi


import os
from pathlib import Path
from textwrap import indent

import click
import griffe
import libcst as cst

docstrings_map = {}


def extract_docstrings(pckg, path=""):
    if "filepath" in pckg and pckg["filepath"].endswith(".pyi"):
        return
    if "docstring" in pckg:
        docstrings_map[path] = pckg["docstring"].value

    for name, pckg in pckg.get("members", {}).items():
        extract_docstrings(pckg, path=f"{path}.{name}")


def _is_docstring_node(node):
    """Checks if a node is a docstring."""
    return (
        isinstance(node, cst.SimpleStatementLine) and
        isinstance(node.body[0], cst.Expr) and
        isinstance(node.body[0].value, cst.SimpleString)
    )


class ClonedSignatureDocstringTransformer(cst.CSTTransformer):
    def __init__(self, docstrings_map, module_name):
        self.docstrings_map = docstrings_map
        self.module_name = module_name
        self.name_of_function = None

    def leave_Assign(self, original_node, updated_node):
        target = original_node.targets[0].target
        value = original_node.value

        if isinstance(target, cst.Name) and isinstance(value, cst.Call) and \
            value.func.value == "_clone_signature":
            self.name_of_function = f"{self.module_name}.{target.value}"
        return updated_node

    def leave_SimpleStatementLine(self, original_node, updated_node):
        if self.name_of_function:
            if len(updated_node.body) > 0 and _is_docstring_node(updated_node):
                comment_content = self.docstrings_map[self.name_of_function].strip()
                self.name_of_function = None

                new_string_node = cst.SimpleString(value=f'"""\n{comment_content}\n"""')
                new_expr_node = updated_node.body[0].with_changes(value=new_string_node)
                new_body = [new_expr_node] + list(updated_node.body[1:])
                updated_node = updated_node.with_changes(body=new_body)

        return updated_node


class FunctionDocstringTransformer(cst.CSTTransformer):
    def __init__(self, docstrings_map, module_name):
        self.docstrings_map = docstrings_map
        self.module_name = module_name

    def leave_FunctionDef(self, original_node, updated_node):
        full_name = f"{self.module_name}.{original_node.name.value}"

        # Check if we have a docstring for this function
        if full_name in self.docstrings_map:
            # Check if the function already has a docstring
            body_list = list(updated_node.body.body)
            has_docstring = len(body_list) > 0 and _is_docstring_node(body_list[0])

            if has_docstring:
                # Replace existing docstring
                docstring = indent(self.docstrings_map[full_name], "    ").strip()
                docstring_value = f'"""\n    {docstring}\n    """'
                new_docstring_node = cst.SimpleStatementLine(
                    body=[cst.Expr(value=cst.SimpleString(value=docstring_value))]
                )
                new_body = [new_docstring_node] + body_list[1:]
                return updated_node.with_changes(
                    body=updated_node.body.with_changes(body=new_body)
                )

        return updated_node

@click.command()
@click.option('--stub_file', '-s', type=click.Path(resolve_path=True))
def update_stub_file(stub_file):
    package = griffe.load("pyarrow", try_relative_path=False, force_inspection=True, resolve_aliases=True)
    extract_docstrings(package.as_dict(), "pyarrow")

    with open(stub_file, 'r') as f:
        tree = cst.parse_module(f.read())

    cloned_signature_transformer = ClonedSignatureDocstringTransformer(docstrings_map, "pyarrow.compute")
    function_docstring_transformer = FunctionDocstringTransformer(docstrings_map, "pyarrow.compute")

    modified_tree = tree.visit(function_docstring_transformer)
    modified_tree = modified_tree.visit(cloned_signature_transformer)


    # Write the modified code
    with open(stub_file, "w") as f:
        f.write(modified_tree.code)

if __name__ == "__main__":
    update_stub_file(obj={})
