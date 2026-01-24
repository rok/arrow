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

// This file provides simdjson's implementation when needed.
//
// There are two modes:
// 1. If SIMDJSON_HEADER_ONLY is defined (from simdjson's interface or CMake),
//    all implementations are inline and this file does nothing.
// 2. If SIMDJSON_HEADER_ONLY is NOT defined, we compile the implementation here
//    using SIMDJSON_IMPLEMENTATION.
//
// This file is always included in the build to handle both cases correctly.

// Debug: Output compilation mode at compile time
#ifdef SIMDJSON_HEADER_ONLY
#pragma message("simdjson_impl.cc: SIMDJSON_HEADER_ONLY is DEFINED - skipping implementation")
#else
#pragma message("simdjson_impl.cc: SIMDJSON_HEADER_ONLY is NOT defined - compiling implementation")
#endif

// Check if simdjson is configured for header-only mode (set by simdjson's
// CMake target or our CMakeLists.txt). If so, all code is inline and we
// don't need to compile anything here.
#ifndef SIMDJSON_HEADER_ONLY

// On Windows, when building a DLL, we need to export simdjson's global symbols.
// ARROW_EXPORTING is defined by CMake for shared library builds.
#if defined(_WIN32) && defined(ARROW_EXPORTING)
#define SIMDJSON_BUILDING_WINDOWS_DYNAMIC_LIBRARY 1
#pragma message("simdjson_impl.cc: SIMDJSON_BUILDING_WINDOWS_DYNAMIC_LIBRARY is DEFINED")
#endif

// Enable the implementation - this causes simdjson.h to include all .cpp files
#define SIMDJSON_IMPLEMENTATION 1

#include <simdjson.h>

// Verify symbols are being compiled by taking address of them
namespace arrow::json::simdjson_verify {
[[maybe_unused]] static const void* verify_error_codes = &simdjson::internal::error_codes;
}

#endif  // SIMDJSON_HEADER_ONLY
