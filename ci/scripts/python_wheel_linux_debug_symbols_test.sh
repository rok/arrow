#!/usr/bin/env bash
#
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

set -e
set -o pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <pyarrow-wheel> <debug-symbols.tar.gz>" >&2
  exit 1
fi

wheel=$(realpath "$1")
debug_symbols=$(realpath "$2")

for command in gdb grep tar; do
  if ! command -v "${command}" > /dev/null; then
    echo "Required command not found: ${command}" >&2
    exit 1
  fi
done

work_dir=$(mktemp -d)
trap 'rm -rf "${work_dir}"' EXIT
site_packages="${work_dir}/site-packages"
mkdir -p "${site_packages}"

python -m pip install --no-deps --target "${site_packages}" "${wheel}"
python -c "import numpy"

cat > "${work_dir}/backtrace_reproducer.py" <<'PYTHON'
import os

import pyarrow as pa
import pyarrow.compute as pc


def crash(ctx, value):
    os.abort()


pc.register_scalar_function(
    crash,
    "arrow_backtrace_crash",
    {
        "summary": "Crash for backtrace validation",
        "description": "Crash for backtrace validation",
    },
    {"value": pa.int64()},
    pa.int64(),
)
pc.call_function("arrow_backtrace_crash", [pa.array([1])])
PYTHON

run_gdb() {
  local output=$1
  PYTHONPATH="${site_packages}${PYTHONPATH:+:${PYTHONPATH}}" \
    gdb --batch --quiet \
      -ex run \
      -ex "thread apply all backtrace" \
      --args python "${work_dir}/backtrace_reproducer.py" \
      > "${output}" 2>&1 || true
}

# Verify that the main wheel is stripped of non-public function symbols.
run_gdb "${work_dir}/stripped-backtrace.txt"
if grep -F "ScalarExecutor::Execute" "${work_dir}/stripped-backtrace.txt"; then
  echo "The wheel still contains non-public function symbols" >&2
  exit 1
fi

# GDB searches an adjacent .debug directory for GNU debuglink targets.
tar -xzf "${debug_symbols}" -C "${site_packages}"
run_gdb "${work_dir}/symbolized-backtrace.txt"

for symbol in \
  "PythonUdfExec" \
  "ScalarExecutor::Execute" \
  "FunctionExecutorImpl::Execute" \
  "ExecuteInternal"; do
  if ! grep -F "${symbol}" "${work_dir}/symbolized-backtrace.txt"; then
    echo "Missing expected symbol in GDB backtrace: ${symbol}" >&2
    cat "${work_dir}/symbolized-backtrace.txt" >&2
    exit 1
  fi
done

echo "The stripped wheel and separate debug-symbol bundle produced the expected backtrace."
