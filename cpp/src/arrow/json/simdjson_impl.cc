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
// SIMDJSON_IMPLEMENTATION must be defined before including simdjson.h to
// instantiate the implementation. On Windows DLL builds, we also need
// SIMDJSON_BUILDING_WINDOWS_DYNAMIC_LIBRARY to export global data symbols.
//
// SIMDJSON_HEADER_ONLY is set by CMake (via target_compile_definitions or
// simdjson's INTERFACE_COMPILE_DEFINITIONS) when simdjson is used in header-only mode.

// Only compile the implementation in header-only mode
#if defined(SIMDJSON_HEADER_ONLY)

// Enable the implementation
#ifndef SIMDJSON_IMPLEMENTATION
#define SIMDJSON_IMPLEMENTATION 1
#endif

// On Windows, when building a DLL, we need to export simdjson's global symbols
#if defined(_WIN32) && defined(ARROW_EXPORTING)
#ifndef SIMDJSON_BUILDING_WINDOWS_DYNAMIC_LIBRARY
#define SIMDJSON_BUILDING_WINDOWS_DYNAMIC_LIBRARY 1
#endif
#endif

#include <simdjson.h>

#else
// Not in header-only mode - simdjson library provides the implementation
// This file is still compiled but produces no code
#endif  // SIMDJSON_HEADER_ONLY
