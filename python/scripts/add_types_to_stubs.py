#!/usr/bin/env python3
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

"""Add type annotations to python/pyarrow-stubs.

Existing annotations are kept. Missing ones are filled from, in order:

1. what the Cython sources return, read from their return statements
   (see cython_return_types);
2. the numpydoc "Parameters" and "Returns" sections of docstrings, which
   cover the Cython modules as well;
3. default values of parameters;
4. MonkeyType traces of the test suite, which cover the pure Python modules.

Leaked Cython names are resolved first, since an unknown base class makes a
whole class unknown to type checkers. Module attributes are declared with the
type of their runtime value, and namedtuples as NamedTuple classes. The stubs
are then fixed, formatted and linted with ruff as configured in pyproject.toml,
which the pre-commit hooks run too. The script prints what it annotated, the
docstring type phrases it could not map, what ruff still reports, and pyright's
type completeness score for the result.

Run from the repository root with python/.venv-stubgen/bin/python after
scripts/generate_stubs.sh. Pass --no-traces to redo everything but the slow
test suite tracing, for instance after changing this script.
"""

import argparse
import ast
import builtins
import collections
import enum
import functools
import importlib
import inspect
import logging
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import types
import typing

import monkeytype
import pyarrow
from monkeytype.config import DefaultConfig
from monkeytype.stubs import build_module_stubs_from_traces
from pyarrow.vendored.docscrape import NumpyDocString

import cython_return_types

log = logging.getLogger("add_types_to_stubs")

STUBS = pathlib.Path("python/pyarrow-stubs")
SOURCES = pathlib.Path("python/pyarrow")
SUBMODULES = ("compute", "dataset", "fs", "parquet",
              "flight", "csv", "json", "ipc", "orc")
BUILTINS = {"int": "int", "integer": "int", "bool": "bool", "boolean": "bool",
            "str": "str", "string": "str", "float": "float", "bytes": "bytes",
            "dict": "dict", "list": "list", "tuple": "tuple", "set": "set",
            "object": "object", "none": "None", "any": "Any", "self": "Self",
            "callable": "Callable[..., Any]", "function": "Callable[..., Any]",
            "iterable": "Iterable[Any]", "iterator": "Iterator[Any]",
            "sequence": "Sequence[Any]", "mapping": "Mapping[Any, Any]",
            "integers": "int", "strings": "str", "file-like": "IO[Any]",
            "file-like object": "IO[Any]", "file-like python object": "IO[Any]",
            "path-like": "os.PathLike[str]"}
CONTAINERS = {"list of": "list[{}]", "sequence of": "Sequence[{}]",
              "iterable of": "Iterable[{}]", "iterator of": "Iterator[{}]",
              "tuple of": "tuple[{}, ...]", "set of": "set[{}]"}
GENERICS = {"list": "list", "tuple": "tuple", "dict": "dict", "set": "set",
            "sequence": "Sequence", "iterable": "Iterable", "iterator": "Iterator"}
# Modules whose names traced annotations may refer to; others would make the
# stubs depend on optional packages such as pandas.
TRACE_IMPORTS = ("pyarrow", "typing", "collections",
                 "datetime", "decimal", "pathlib", "os", "io")
TYPING_IMPORT = ("from typing import Any, Callable, IO, Iterable, Iterator, "
                 "Literal, Mapping, NamedTuple, Self, Sequence")
# Cython C types that leak into the stubs through declared parameter and
# attribute types, and what Python passes for them; `Type` and `TimeUnit` are
# C enums.
CYTHON_TYPES = {"c_bool": "bool", "c_string": "bytes", "std_vector": "list",
                "Type": "int", "TimeUnit": "int"}
# Annotations for module attributes, by the type of their runtime value.
VALUE_TYPES = {int: "int", float: "float", str: "str", bytes: "bytes",
               bool: "bool", list: "list[Incomplete]", set: "set[Incomplete]",
               frozenset: "frozenset[Incomplete]", tuple: "tuple[Incomplete, ...]",
               dict: "dict[Incomplete, Incomplete]", re.Pattern: "re.Pattern[str]"}

stats = collections.Counter()
unresolved = collections.Counter()
undefined = collections.Counter()


class TraceConfig(DefaultConfig):
    """MonkeyType configuration: trace the installed package, not its tests."""

    def code_filter(self):
        return lambda code: ("/pyarrow/" in code.co_filename
                             and "/tests/" not in code.co_filename)


CONFIG = TraceConfig()


# This module doubles as a pytest plugin so that every pytest-xdist worker
# traces the tests it runs, each into its own database under MT_DB_DIR.

def pytest_configure(config):
    worker = os.environ.get("PYTEST_XDIST_WORKER", "main")
    database = pathlib.Path(os.environ["MT_DB_DIR"]) / f"{worker}.sqlite3"
    os.environ["MT_DB_PATH"] = str(database)
    config.monkeytype = monkeytype.trace(CONFIG)
    config.monkeytype.__enter__()


def pytest_unconfigure(config):
    if hasattr(config, "monkeytype"):
        config.monkeytype.__exit__(None, None, None)


# --- docstring types -------------------------------------------------------

def resolve(text, local, imports):
    """Map one numpydoc type phrase to an annotation, or None if unknown."""
    text = re.sub(r"\s*\(.*?\)", "", text).strip(" `.")
    if text.lower() in BUILTINS:
        return BUILTINS[text.lower()]
    for prefix, template in CONTAINERS.items():
        if text.lower().startswith(prefix + " "):
            inner = resolve(text[len(prefix) + 1:], local, imports)
            return template.format(inner) if inner else None
    literal = re.fullmatch(
        r"\{(.*)\}", text) or re.fullmatch(r"('[^']*'|\"[^\"]*\")", text)
    if literal:
        items = [i.strip() for i in literal.group(1).split(",")]
        if all(re.fullmatch(r"'[^']*'|\"[^\"]*\"", i) for i in items):
            return f"Literal[{', '.join(items)}]"
    generic = re.fullmatch(r"(\w+)\[(.*)\]", text)
    if generic and generic.group(1).lower() in GENERICS:
        args = [resolve(a, local, imports) for a in generic.group(2).split(",")]
        outer = GENERICS[generic.group(1).lower()]
        return f"{outer}[{', '.join(args)}]" if all(args) else None
    name = re.sub(r"^(pyarrow|pa)\.(lib\.)?", "", text)
    if name in local:
        return name
    if re.fullmatch(r"[\w.]+", name):
        for prefix in ("", *(f"{sub}." for sub in SUBMODULES)):
            obj = pyarrow
            for part in f"{prefix}{name}".split("."):
                obj = getattr(obj, part, None)
            if isinstance(obj, type):
                if "." in prefix + name:
                    imports.add("pyarrow." + (prefix + name).rsplit(".", 1)[0])
                return f"pyarrow.{prefix}{name}"
    unresolved[text] += 1
    return None


def annotation(text, local, imports):
    """Map a numpydoc type string such as "int or str, optional"."""
    match = re.search(r",?\s+((optional|default)\b.*)$", text)
    body, tail = (text[:match.start()], match.group(1)) if match else (text, "")
    parts = [resolve(p, local, imports)
             for p in re.split(r",?\s+or\s+|,\s*|\s*\|\s*", body) if p.strip()]
    if not parts or None in parts:
        return None
    result = " | ".join(dict.fromkeys(parts))
    if re.match(r"optional|default\s+`?None`?$", tail) and "None" not in parts:
        result += " | None"
    return result


def from_default(node):
    if isinstance(node, ast.Constant):
        if type(node.value) in (bool, int, float, str, bytes):
            return type(node.value).__name__
    return None


# --- stub rewriting --------------------------------------------------------

class Cleanup(ast.NodeTransformer):
    """Drop cimports of .pxd declarations, and make imports from pyarrow
    explicit re-exports where the runtime module has the name, as
    `from pyarrow.lib import Table` in a stub is not one."""

    def __init__(self, module):
        self.module = module

    def visit_ImportFrom(self, node):
        origin = node.module or ""
        if origin.startswith("pyarrow.includes"):
            return None
        if origin.startswith("pyarrow"):
            for alias in node.names:
                if alias.asname is None and hasattr(self.module, alias.name):
                    alias.asname = alias.name
        return node


def is_namedtuple(node):
    return (isinstance(node, ast.Call) and len(node.args) == 2
            and ast.unparse(node.func).endswith("namedtuple")
            and isinstance(node.args[1], (ast.Tuple, ast.List)))


def namedtuple_fields(node):
    return [ast.AnnAssign(ast.Name(f.value), ast.Name("Incomplete"), None, simple=1)
            for f in node.args[1].elts]


def dedupe_imports(nodes):
    """Keep the last import of each name: the stub's own import rather than
    one added before it."""
    seen = set()
    for node in reversed(nodes):
        if isinstance(node, ast.ImportFrom) and node.names[0].name != "*":
            node.names = [a for a in node.names if (a.asname or a.name) not in seen]
            seen |= {a.asname or a.name for a in node.names}
    return [n for n in nodes if not isinstance(n, ast.ImportFrom) or n.names]


def order_assignments(body):
    """Move assignments after the definitions their values refer to; only
    annotations may refer forward in a stub."""
    position = {}
    for i, node in enumerate(body):
        for name in defined_names(ast.Module([node], [])):
            position[name] = i
    ordered, waiting = [], collections.defaultdict(list)
    for i, node in enumerate(body):
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
            later = [position[n.id] for n in ast.walk(node.value)
                     if isinstance(n, ast.Name) and position.get(n.id, -1) > i]
            if later:
                waiting[max(later)].append(node)
                continue
        ordered.append(node)
        ordered.extend(waiting.pop(i, []))
    return ordered


class Names(ast.NodeTransformer):
    """Spell the names an annotation uses so that the stub knows them."""

    def __init__(self, tidy):
        self.tidy = tidy

    def visit_Name(self, node):
        if node.id in self.tidy.known:
            return node
        if node.id in CYTHON_TYPES:
            stats["Cython types spelled"] += 1
            return ast.Name(CYTHON_TYPES[node.id])
        if qualified := self.tidy.lookup(node.id):
            stats["annotation names qualified"] += 1
            return ast.parse(qualified, mode="eval").body
        undefined[node.id] += 1
        return ast.Name("Incomplete")


class Tidy(ast.NodeTransformer):
    """Say what the stub can about attributes, using only names it knows.

    Module assignments become declarations typed from the runtime value:
    namedtuples become NamedTuple classes, type aliases stay, and the rest
    get the value's type or Incomplete; class constants likewise. Leaked
    Cython types get their Python spelling, classes other modules define get
    qualified, and other names an annotation does not know become
    Incomplete. Imports come first and once, assignments move after the
    names they alias, and `__enter__` and its kind return Self."""

    SELF_RETURNING = {"__enter__", "__aenter__", "__new__",
                      "__iter__", "__aiter__"}

    def __init__(self, module, known):
        self.module, self.known = module, known | set(dir(builtins))
        self.owner, self.owners, self.imports = None, [module], set()

    def lookup(self, name):
        """A class of that name in pyarrow or a submodule, qualified."""
        for prefix in ("", *(f"{sub}." for sub in SUBMODULES)):
            obj = pyarrow
            for part in f"{prefix}{name}".split("."):
                obj = getattr(obj, part, None)
            if isinstance(obj, type):
                if prefix:
                    self.imports.add(f"pyarrow.{prefix[:-1]}")
                return f"pyarrow.{prefix}{name}"
        return None

    def visit_Module(self, node):
        body = order_assignments([self.declare(n) for n in node.body])
        imports = [n for n in body if isinstance(n, (ast.Import, ast.ImportFrom))]
        node.body = dedupe_imports(imports) + [n for n in body if n not in imports]
        self.generic_visit(node)
        return node

    def declare(self, node):
        if not (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)):
            return node
        name = node.targets[0].id
        if name.startswith("__") and name.endswith("__"):
            return node
        owner = self.owners[-1]
        if owner is not self.module:  # class constants; members and aliases stay
            constant = isinstance(node.value, (ast.Constant, ast.BinOp, ast.UnaryOp))
            member = isinstance(owner, type) and issubclass(owner, enum.Enum)
            if not constant or member:
                return node
        value = getattr(owner, name, None)
        if is_namedtuple(node.value):
            stats["namedtuples declared"] += 1
            return ast.ClassDef(name=name, bases=[ast.Name("NamedTuple")], keywords=[],
                                body=namedtuple_fields(node.value) or [ast.Pass()],
                                decorator_list=[], type_params=[])
        if (isinstance(value, (type, types.UnionType, typing.TypeVar))
                or value is typing.Any or typing.get_origin(value) is not None):
            return node
        typed = from_default(node.value) or VALUE_TYPES.get(type(value))
        cls = type(value)
        if not typed and cls.__name__ in self.known \
                and getattr(self.module, cls.__name__, None) is cls:
            typed = cls.__name__
        stats["attributes typed from runtime" if typed
              else "attributes left Incomplete"] += 1
        return ast.AnnAssign(ast.Name(name), ast.parse(
            typed or "Incomplete", mode="eval").body, None, simple=1)

    def visit_ClassDef(self, node):
        for i, base in enumerate(node.bases):
            if is_namedtuple(base):
                node.bases[i] = ast.Name("NamedTuple")
                node.body[:0] = namedtuple_fields(base)
                stats["namedtuples declared"] += 1
        self.owners.append(getattr(self.owners[-1], node.name, None))
        node.body = order_assignments([self.declare(n) for n in node.body])
        self.owner, outer = node.name, self.owner
        self.generic_visit(node)
        self.owner = outer
        self.owners.pop()
        return node

    def visit_FunctionDef(self, node):
        self.generic_visit(node)
        node.returns = self.annotation(node.returns)
        returns = node.returns
        if (self.owner and node.name in self.SELF_RETURNING
                and isinstance(returns, ast.Name) and returns.id == self.owner):
            node.returns = ast.Name("Self")
            stats["returns changed to Self"] += 1
        return node

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_arg(self, node):
        node.annotation = self.annotation(node.annotation)
        return node

    def visit_AnnAssign(self, node):
        node.annotation = self.annotation(node.annotation)
        return node

    def annotation(self, node):
        return Names(self).visit(node) if node else None


class Annotator(ast.NodeTransformer):
    def __init__(self, module, local, sourced, traced):
        self.module, self.local = module, local
        self.sourced, self.traced = sourced, traced
        self.imports, self.owners = set(), [(module, None)]

    def visit_ClassDef(self, node):
        self.owners.append((getattr(self.owners[-1][0], node.name, None), node.name))
        self.generic_visit(node)
        self.owners.pop()
        return node

    def visit_FunctionDef(self, node):
        runtime, owner = self.owners[-1]
        try:
            doc = NumpyDocString(inspect.getdoc(
                getattr(runtime, node.name, None)) or "")
        except Exception:
            doc = {"Parameters": [], "Returns": []}
        params = {
            n.strip("* "): p.type for p in doc["Parameters"] for n in p.name.split(",")}
        traced_params, traced_return = self.traced.get((owner, node.name), ({}, None))
        positional = node.args.posonlyargs + node.args.args
        defaults = dict(zip(reversed(positional), reversed(node.args.defaults)))
        defaults.update((a, d) for a, d in zip(
            node.args.kwonlyargs, node.args.kw_defaults) if d)
        for arg in positional + node.args.kwonlyargs:
            if arg.arg in ("self", "cls"):
                continue
            stats["parameters"] += 1
            if arg.annotation:
                stats["parameters already typed"] += 1
                continue
            default = defaults.get(arg)
            typed = annotation(params[arg.arg], self.local,
                               self.imports) if params.get(arg.arg) else None
            source = "parameters typed from docstring"
            if not typed and from_default(default):
                typed, source = from_default(default), "parameters typed from default"
            # Traces only show what the tests passed; a default they never
            # saw, such as another class, means the observed type is too narrow.
            unknown_default = default is not None and not (
                isinstance(default, ast.Constant) and default.value is not ...)
            if not typed and arg.arg in traced_params and not unknown_default:
                typed, source = ast.unparse(
                    traced_params[arg.arg]), "parameters typed from traces"
            if typed:
                if isinstance(default, ast.Constant) and default.value is None:
                    if "None" not in typed:
                        typed += " | None"
                default_type = from_default(default)
                if default_type and default_type not in typed.split(" | "):
                    typed += " | " + default_type
                arg.annotation = ast.parse(typed, mode="eval").body
                stats[source] += 1
        if node.name != "__init__":
            stats["returns"] += 1
            if node.returns:
                stats["returns already typed"] += 1
            elif (typed := annotation(self.sourced.get((owner, node.name), ""),
                                      self.local, self.imports)):
                node.returns = ast.parse(typed, mode="eval").body
                stats["returns typed from Cython sources"] += 1
            elif len(doc["Returns"]) == 1 and (typed := annotation(
                    doc["Returns"][0].type or doc["Returns"][0].name,
                    self.local, self.imports)):
                node.returns = ast.parse(typed, mode="eval").body
                stats["returns typed from docstring"] += 1
            elif traced_return is not None:
                node.returns = traced_return
                stats["returns typed from traces"] += 1
        return node

    visit_AsyncFunctionDef = visit_FunctionDef


def defined_names(tree):
    names = {n.name for n in tree.body if isinstance(
        n, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))}
    names |= {a.asname or a.name for n in tree.body if isinstance(
        n, ast.ImportFrom) for a in n.names}
    names |= {a.asname or a.name.split(".")[0] for n in tree.body if isinstance(
        n, ast.Import) for a in n.names}
    names |= {t.id for n in tree.body if isinstance(
        n, ast.Assign) for t in n.targets if isinstance(t, ast.Name)}
    names |= {n.target.id for n in tree.body if isinstance(
        n, ast.AnnAssign) and isinstance(n.target, ast.Name)}
    return names


def order_classes(tree):
    """Define classes after their bases; pyright treats a base defined later
    in a stub as unknown. Everything else keeps its order."""
    classes = {n.name: n for n in tree.body if isinstance(n, ast.ClassDef)}
    ordered, done = [], set()

    def emit(node):
        if node.name in done:
            return
        done.add(node.name)
        for base in node.bases:
            if isinstance(base, ast.Name) and base.id in classes:
                emit(classes[base.id])
        ordered.append(node)

    for node in classes.values():
        emit(node)
    tree.body = [n for n in tree.body if not isinstance(n, ast.ClassDef)] + ordered
    return tree


def rewrite(path, module, sourced, traced):
    source = path.read_text()
    header = "".join(line for line in source.splitlines(
        keepends=True)[:20] if line.startswith("#"))
    tree = ast.parse(source)
    defined = defined_names(tree)
    # `from pyarrow.lib import *` skips private names such as the base class
    # of most Cython classes.
    missing = {n.id for n in ast.walk(tree)
               if isinstance(n, ast.Name) and n.id.startswith("_")
               and n.id not in defined and hasattr(pyarrow.lib, n.id)}
    local = {n for n in defined if isinstance(getattr(module, n, None), type)}
    annotator = Annotator(module, local, sourced, traced.get("functions", {}))
    tree = order_classes(annotator.visit(tree))
    imports = ["import os", "import re", "import pyarrow", TYPING_IMPORT,
               "from _typeshed import Incomplete"]
    imports += sorted(f"import {m}" for m in annotator.imports)
    imports += traced.get("imports", [])
    if missing and module is not pyarrow.lib:
        imports.append(f"from pyarrow.lib import {', '.join(sorted(missing))}")
    # Names the stub imports already stay with that import; ruff drops the
    # unused ones afterwards.
    for node in reversed([ast.parse(line).body[0] for line in imports]):
        node.names = [a for a in node.names
                      if (a.asname or a.name.split(".")[0]) not in defined]
        if node.names:
            tree.body.insert(0, node)
    tree = Cleanup(module).visit(tree)
    known = defined_names(tree)
    for node in tree.body:  # star imports bring in the runtime module's names
        if isinstance(node, ast.ImportFrom) and node.names[0].name == "*":
            known |= set(dir(importlib.import_module(
                "." * node.level + (node.module or ""), module.__name__)))
    tidy = Tidy(module, known)
    tree = tidy.visit(tree)
    tree.body[:0] = [ast.parse(f"import {m}").body[0] for m in sorted(tidy.imports)]
    path.write_text(header + "\n" + ast.unparse(tree) + "\n")


# --- MonkeyType traces -----------------------------------------------------

class Modernize(ast.NodeTransformer):
    """Spell the typing module forms MonkeyType renders the way the rest of
    the stubs do: `X | None`, `A | B` and `list[X]`, and the platform's path
    class as `Path`."""

    BUILTIN = {"List": "list", "Dict": "dict", "Tuple": "tuple", "Set": "set",
               "FrozenSet": "frozenset", "Type": "type"}
    PORTABLE = {"PosixPath": "Path", "WindowsPath": "Path"}

    def visit_Name(self, node):
        node.id = self.PORTABLE.get(node.id, node.id)
        return node

    def visit_Subscript(self, node):
        self.generic_visit(node)
        if not isinstance(node.value, ast.Name):
            return node
        if node.value.id == "Optional":
            return ast.BinOp(node.slice, ast.BitOr(), ast.Constant(None))
        if node.value.id == "Union":
            parts = node.slice.elts if isinstance(
                node.slice, ast.Tuple) else [node.slice]
            return functools.reduce(lambda a, b: ast.BinOp(a, ast.BitOr(), b), parts)
        node.value.id = self.BUILTIN.get(node.value.id, node.value.id)
        return node


def trace_tests(directory):
    """Run the shipped test suite in parallel under MonkeyType, recording
    into one database per worker in directory."""
    log.info("Tracing the test suite with MonkeyType")
    env = {**os.environ, "MT_DB_DIR": str(directory),
           "PYTHONPATH": str(pathlib.Path(__file__).parent)}
    run = subprocess.run([sys.executable, "-m", "pytest", "--pyargs", "pyarrow.tests",
                          "-q", "-p", "no:cacheprovider", "-p", "add_types_to_stubs",
                          "-n", "auto"],
                         cwd=directory, env=env, capture_output=True, text=True)
    if run.returncode > 1:  # failing tests are fine, not collecting is not
        log.warning("pytest exited with %d:\n%s", run.returncode,
                    "\n".join(run.stdout.splitlines()[-15:]))


def load_traces(directory):
    """Per module: traced annotations keyed by (class, function), plus the
    imports they need, keeping only names from TRACE_IMPORTS modules."""
    stores = []
    for db in sorted(directory.glob("*.sqlite3")):
        os.environ["MT_DB_PATH"] = str(db)
        stores.append(CONFIG.trace_store())
    result = {}
    for module in sorted({m for store in stores for m in store.list_modules()}):
        log.info("Reading traces of %s", module)
        traces = []
        # The default limit of 2000 drops traces of the big modules, and
        # which ones depends on the order the tests ran in.
        for thunk in (t for store in stores
                      for t in store.filter(module, limit=sys.maxsize)):
            try:
                traces.append(thunk.to_trace())
            except Exception:  # types local to a test function; the CLI skips too
                pass
        stub = build_module_stubs_from_traces(
            traces, CONFIG.max_typed_dict_size).get(module)
        if stub is None:
            continue
        tree = Modernize().visit(ast.parse(stub.render()))
        imports = [n for n in tree.body if isinstance(n, ast.ImportFrom)
                   and n.module.split(".")[0] in TRACE_IMPORTS]
        for node in imports:  # Python 3.13 defines Path in pathlib._local
            public = importlib.import_module(node.module.split("._")[0])
            for alias in node.names:
                alias.name = Modernize.PORTABLE.get(alias.name, alias.name)
            if all(hasattr(public, a.name) for a in node.names):
                node.module = public.__name__
        allowed = {a.asname or a.name for n in imports for a in n.names} | set(
            dir(builtins))

        def usable(node):
            names = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
            return names <= allowed and ast.unparse(node) not in ("Any", "None")

        functions = {}
        scopes = [(None, tree.body)]
        scopes += [(n.name, n.body) for n in tree.body if isinstance(n, ast.ClassDef)]
        for owner, body in scopes:
            for n in body:
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    args = n.args.posonlyargs + n.args.args + n.args.kwonlyargs
                    params = {a.arg: a.annotation for a in args
                              if a.annotation and usable(a.annotation)}
                    returns = n.returns if n.returns and usable(n.returns) else None
                    functions[(owner, n.name)] = (params, returns)
        result[module] = {"functions": functions,
                          "imports": [ast.unparse(n) for n in imports]}
    return result


# --- formatting and linting ------------------------------------------------

def lint():
    """Apply the fixes ruff has for the stubs (unused and duplicate imports,
    typing spellings, flake8-pyi conventions), format them, and report what
    it still finds. The pre-commit hooks run the same configuration."""
    log.info("Formatting and linting the stubs")
    ruff = pathlib.Path(sys.executable).parent / "ruff"
    subprocess.run([ruff, "check", "--quiet", "--fix", "--unsafe-fixes", STUBS],
                   stdout=subprocess.DEVNULL)
    subprocess.run([ruff, "format", "--quiet", STUBS])
    report = subprocess.run([ruff, "check", "--quiet", "--output-format", "concise",
                             STUBS], capture_output=True, text=True).stdout
    print(f"ruff findings: {len(report.splitlines())}")
    print(report, end="")


# --- completeness score ----------------------------------------------------

def verify_types():
    """Score the stubs with pyright as if they shipped inside the package."""
    log.info("Scoring type completeness with pyright")
    package = pathlib.Path(pyarrow.__file__).parent
    copied = [package / "py.typed"]
    copied[0].touch()
    for path in STUBS.rglob("*.pyi"):
        target = package / path.relative_to(STUBS / "pyarrow")
        if not target.exists():
            shutil.copy(path, target)
            copied.append(target)
    try:
        # pyright finds the package through `python`
        binaries = pathlib.Path(sys.executable).parent
        env = {**os.environ, "PATH": f"{binaries}{os.pathsep}{os.environ['PATH']}"}
        command = [binaries / "pyright", "--verifytypes", "pyarrow", "--ignoreexternal"]
        report = subprocess.run(command, capture_output=True, text=True, env=env).stdout
    finally:
        for path in copied:
            path.unlink()
    summary = r"Symbols exported|Other symbols|  With|Type completeness"
    print(*(line for line in report.splitlines() if re.match(summary, line)), sep="\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--no-traces", action="store_true",
                        help="skip tracing the test suite")
    args = parser.parse_args()
    logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)
    for sub in SUBMODULES:  # make them attributes of pyarrow for name resolution
        importlib.import_module(f"pyarrow.{sub}")
    traced = {}
    if not args.no_traces:
        with tempfile.TemporaryDirectory() as tmp:
            trace_tests(pathlib.Path(tmp))
            traced = load_traces(pathlib.Path(tmp))
    log.info("Reading return types from the Cython sources")
    sourced = cython_return_types.return_types(sorted(SOURCES.glob("*.pyx")))
    for path in sorted(STUBS.rglob("*.pyi")):
        name = ".".join(path.relative_to(STUBS).with_suffix(
            "").parts).removesuffix(".__init__")
        try:
            module = importlib.import_module(name)
        except ImportError:
            log.info("Skipping %s, not importable", name)
            continue
        log.info("Annotating %s", path)
        rewrite(path, module, sourced.get(name, {}), traced.get(name, {}))
    print("traced modules:", len(traced))
    for key, value in sorted(stats.items()):
        print(f"{key}: {value}")
    print("unmapped docstring types:", *
          (f"{n}x {t!r}" for t, n in unresolved.most_common(20)), sep="\n  ")
    print("undefined names made Incomplete:", *
          (f"{n}x {t}" for t, n in undefined.most_common()), sep="\n  ")
    lint()
    verify_types()


if __name__ == "__main__":
    main()
