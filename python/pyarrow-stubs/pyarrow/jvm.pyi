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

import os
import re
import pyarrow
from typing import Any, Callable, IO, Iterable, Iterator, Literal, Mapping, NamedTuple, Self, Sequence
from _typeshed import Incomplete

def jvm_buffer(jvm_buf) -> pyarrow.Buffer:
    ...

def _from_jvm_int_type(jvm_type) -> pyarrow.DataType:
    ...

def _from_jvm_float_type(jvm_type):
    ...

def _from_jvm_time_type(jvm_type):
    ...

def _from_jvm_timestamp_type(jvm_type):
    ...

def _from_jvm_date_type(jvm_type):
    ...

def field(jvm_field) -> pyarrow.Field:
    ...

def schema(jvm_schema) -> pyarrow.Schema:
    ...

def array(jvm_array) -> pyarrow.Array:
    ...

def record_batch(jvm_vector_schema_root):
    ...

class _JvmBufferNanny:
    ref_manager: Incomplete

    def __init__(self, jvm_buf) -> None:
        ...

    def __del__(self) -> None:
        ...
