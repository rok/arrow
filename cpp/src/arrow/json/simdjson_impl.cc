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

// This file provides simdjson's implementation when the library is header-only.
// CMake only includes this file in the build when header-only mode is detected.
//
// SIMDJSON_IMPLEMENTATION must be defined before including simdjson.h to
// instantiate the implementation. On Windows DLL builds, we also need
// SIMDJSON_BUILDING_WINDOWS_DYNAMIC_LIBRARY to export global data symbols.

// Enable the implementation - this file is only compiled in header-only mode
#define SIMDJSON_IMPLEMENTATION 1

// On Windows, when building a DLL, we need to export simdjson's global symbols.
// ARROW_EXPORTING is defined by CMake for shared library builds.
#if defined(_WIN32) && defined(ARROW_EXPORTING)
#define SIMDJSON_BUILDING_WINDOWS_DYNAMIC_LIBRARY 1
#endif

#include <simdjson.h>
