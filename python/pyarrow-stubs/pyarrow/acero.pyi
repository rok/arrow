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

from pyarrow._acero import AggregateNodeOptions as AggregateNodeOptions, AsofJoinNodeOptions as AsofJoinNodeOptions, Declaration as Declaration, ExecNodeOptions as ExecNodeOptions, FilterNodeOptions as FilterNodeOptions, HashJoinNodeOptions as HashJoinNodeOptions, OrderByNodeOptions as OrderByNodeOptions, ProjectNodeOptions as ProjectNodeOptions, RecordBatchReaderSourceNodeOptions as RecordBatchReaderSourceNodeOptions, TableSourceNodeOptions as TableSourceNodeOptions
from pyarrow._dataset import ScanNodeOptions as ScanNodeOptions
from pyarrow.compute import Expression as Expression, field as field
from pyarrow.lib import RecordBatch as RecordBatch, Table as Table, array as array

class DatasetModuleStub:
    class Dataset: ...
    class InMemoryDataset: ...
ds = DatasetModuleStub

def _dataset_to_decl(dataset, use_threads: bool = True, implicit_ordering: bool = False): ...
def _perform_join(join_type, left_operand, left_keys, right_operand, right_keys, left_suffix=None, right_suffix=None, use_threads: bool = True, coalesce_keys: bool = False, output_type=..., filter_expression=None): ...
def _perform_join_asof(left_operand, left_on, left_by, right_operand, right_on, right_by, tolerance, use_threads: bool = True, output_type=...): ...
def _filter_table(table, expression): ...
def _sort_source(table_or_dataset, sort_keys, output_type=..., **kwargs): ...
def _group_by(table, aggregates, keys, use_threads: bool = True): ...
