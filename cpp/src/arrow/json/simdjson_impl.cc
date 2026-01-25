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
#if defined(SIMDJSON_USING_LIBRARY)
#pragma message("simdjson_impl.cc: SIMDJSON_USING_LIBRARY - linking against simdjson library")
#elif defined(SIMDJSON_HEADER_ONLY)
#pragma message("simdjson_impl.cc: SIMDJSON_HEADER_ONLY - all code inline")
#else
#pragma message("simdjson_impl.cc: Compiling simdjson implementation")
#endif

// When using simdjson as a library (SIMDJSON_USING_LIBRARY) or in header-only
// mode (SIMDJSON_HEADER_ONLY), this file is a no-op.
// Only compile the implementation when neither is defined.
#if !defined(SIMDJSON_HEADER_ONLY) && !defined(SIMDJSON_USING_LIBRARY)

// On Windows, when building a DLL, we need to export simdjson's global symbols.
// ARROW_EXPORTING is defined by CMake for shared library builds.
#if defined(_WIN32) && defined(ARROW_EXPORTING)
#define SIMDJSON_BUILDING_WINDOWS_DYNAMIC_LIBRARY 1
#pragma message("simdjson_impl.cc: SIMDJSON_BUILDING_WINDOWS_DYNAMIC_LIBRARY is DEFINED")
#endif

// Enable the implementation - this causes simdjson.h to include all .cpp files
#define SIMDJSON_IMPLEMENTATION 1

#include <simdjson.h>

#endif  // !SIMDJSON_HEADER_ONLY && !SIMDJSON_USING_LIBRARY
