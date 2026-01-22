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

# ARROW_SIMDJSON_HEADER_ONLY is set in the parent CMakeLists.txt
if(ARROW_SIMDJSON_HEADER_ONLY)
  set(ARROW_JSON_TEST_DEFINITIONS SIMDJSON_HEADER_ONLY)
else()
  set(ARROW_JSON_TEST_DEFINITIONS "")
endif()

add_arrow_test(test
               SOURCES
               chunked_builder_test.cc
               chunker_test.cc
               converter_test.cc
               from_string_test.cc
               parser_test.cc
               reader_test.cc
               PREFIX
               "arrow-json"
               EXTRA_LINK_LIBS
               simdjson::simdjson
               DEFINITIONS
               ${ARROW_JSON_TEST_DEFINITIONS})

add_arrow_benchmark(parser_benchmark
                    PREFIX
                    "arrow-json"
                    EXTRA_LINK_LIBS
                    simdjson::simdjson)
# Add SIMDJSON_HEADER_ONLY to benchmark if needed (add_benchmark doesn't support DEFINITIONS)
# Only add if the target was actually created (benchmarks may be disabled)
if(ARROW_SIMDJSON_HEADER_ONLY AND TARGET arrow-json-parser-benchmark)
  target_compile_definitions(arrow-json-parser-benchmark PRIVATE SIMDJSON_HEADER_ONLY)
endif()
arrow_install_all_headers("arrow/json")

# pkg-config support
arrow_add_pkg_config("arrow-json")
