// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This file provides simdjson's implementation using the "single compilation unit"
// model. When simdjson is used as a header-only library (no prebuilt .lib/.a),
// this file compiles all of simdjson's implementation.
//
// Other source files include simdjson.h normally and get extern declarations.
// This file defines SIMDJSON_IMPLEMENTATION to provide the actual definitions.

// On Windows, when building a DLL, we need to export simdjson's global symbols.
// ARROW_EXPORTING is defined by CMake for shared library builds.
#if defined(_WIN32) && defined(ARROW_EXPORTING)
#define SIMDJSON_BUILDING_WINDOWS_DYNAMIC_LIBRARY 1
#endif

// Enable the implementation - this causes simdjson.h to include all .cpp files
#define SIMDJSON_IMPLEMENTATION 1

#include <simdjson.h>
