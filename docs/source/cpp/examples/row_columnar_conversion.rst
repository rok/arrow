.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. default-domain:: cpp
.. highlight:: cpp

Row to columnar conversion
==========================

Fixed Schemas
-------------

The following example converts an array of structs to a :class:`arrow::Table`
instance, and then converts it back to the original array of structs.

.. literalinclude:: ../../../../cpp/examples/arrow/row_wise_conversion_example.cc


Dynamic Schemas
---------------

In many cases, we need to convert to and from row data that does not have a
schema known at compile time. To help implement these conversions, this library
provides several utilities:

* :class:`arrow::RecordBatchBuilder`: creates and manages array builders for
  a full record batch.
* :func:`arrow::VisitTypeInline`: dispatch to functions specialized for the given
  array type.
* :ref:`type-traits` (such as ``arrow::enable_if_primitive_ctype``): narrow template
  functions to specific Arrow types, useful in conjunction with
  the :ref:`cpp-visitor-pattern`.
* :class:`arrow::TableBatchReader`: read a table in a batch at a time, with each
  batch being a zero-copy slice.
