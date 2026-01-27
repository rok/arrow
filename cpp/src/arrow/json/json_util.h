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

#pragma once

// Ensure simdjson is used in header-only mode
#ifndef SIMDJSON_HEADER_ONLY
#define SIMDJSON_HEADER_ONLY
#endif
#include <simdjson.h>

#include <cstdint>
#include <string>
#include <string_view>

#include "arrow/util/visibility.h"

namespace arrow {
namespace json {

/// \brief Escape a string for JSON output and return it with surrounding quotes.
///
/// This function properly escapes special characters like quotes, backslashes,
/// and control characters according to the JSON specification.
/// Uses simdjson's SIMD-accelerated string escaping.
///
/// \param s The string to escape
/// \return The escaped string surrounded by double quotes
inline std::string EscapeJsonString(std::string_view s) {
  simdjson::builder::string_builder builder;
  builder.escape_and_append_with_quotes(s);
  return static_cast<std::string>(builder);
}

/// \brief JSON writer with automatic comma insertion between elements.
/// Uses simdjson's builder API for SIMD-accelerated serialization.
///
/// Provides both runtime and compile-time key methods. Use compile-time
/// key methods (e.g., SetString<"key">(value)) for better performance
/// when keys are string literals.
class JsonWriter {
 public:
  JsonWriter() = default;

  void StartObject() {
    MaybeComma();
    builder_.start_object();
    needs_comma_ = false;
  }

  void EndObject() {
    builder_.end_object();
    needs_comma_ = true;
  }

  void StartArray() {
    MaybeComma();
    builder_.start_array();
    needs_comma_ = false;
  }

  void EndArray() {
    builder_.end_array();
    needs_comma_ = true;
  }

  void Key(std::string_view key) {
    MaybeComma();
    builder_.escape_and_append_with_quotes(key);
    builder_.append_colon();
    needs_comma_ = false;
  }

  void String(std::string_view value) {
    MaybeComma();
    builder_.escape_and_append_with_quotes(value);
    needs_comma_ = true;
  }

  void Bool(bool value) {
    MaybeComma();
    builder_.append(value);
    needs_comma_ = true;
  }

  void Int(int32_t value) {
    MaybeComma();
    builder_.append(value);
    needs_comma_ = true;
  }

  void Int64(int64_t value) {
    MaybeComma();
    builder_.append(value);
    needs_comma_ = true;
  }

  void Uint(uint32_t value) {
    MaybeComma();
    builder_.append(value);
    needs_comma_ = true;
  }

  void Uint64(uint64_t value) {
    MaybeComma();
    builder_.append(value);
    needs_comma_ = true;
  }

  void Double(double value) {
    MaybeComma();
    builder_.append(value);
    needs_comma_ = true;
  }

  void Null() {
    MaybeComma();
    builder_.append_null();
    needs_comma_ = true;
  }

  /// \brief Serialize a container (vector, array, etc.) directly as a JSON array.
  ///
  /// Uses simdjson's builder for efficient serialization of STL containers.
  /// Supported types include std::vector, std::array, and other iterable containers
  /// with numeric, boolean, or string element types.
  ///
  /// Example:
  ///   std::vector<int64_t> shape = {2, 3, 4};
  ///   writer.Key("shape");
  ///   writer.Value(shape);  // writes [2,3,4]
  template <typename Container>
  void Value(const Container& container) {
    MaybeComma();
    builder_.append(container);
    needs_comma_ = true;
  }

  /// @name Compile-time key methods
  /// These use simdjson's compile-time string optimization for better performance
  /// when keys are string literals.
  ///
  /// Example:
  ///   writer.SetString<"name">(name_value);
  ///   writer.SetBool<"enabled">(true);
  /// @{

  template <simdjson::constevalutil::fixed_string Key>
  void SetString(std::string_view value) {
    MaybeComma();
    builder_.append_key_value<Key>(value);
    needs_comma_ = true;
  }

  template <simdjson::constevalutil::fixed_string Key>
  void SetBool(bool value) {
    MaybeComma();
    builder_.append_key_value<Key>(value);
    needs_comma_ = true;
  }

  template <simdjson::constevalutil::fixed_string Key>
  void SetInt(int32_t value) {
    MaybeComma();
    builder_.append_key_value<Key>(value);
    needs_comma_ = true;
  }

  template <simdjson::constevalutil::fixed_string Key>
  void SetInt64(int64_t value) {
    MaybeComma();
    builder_.append_key_value<Key>(value);
    needs_comma_ = true;
  }

  template <simdjson::constevalutil::fixed_string Key>
  void SetUint(uint32_t value) {
    MaybeComma();
    builder_.append_key_value<Key>(value);
    needs_comma_ = true;
  }

  template <simdjson::constevalutil::fixed_string Key>
  void SetUint64(uint64_t value) {
    MaybeComma();
    builder_.append_key_value<Key>(value);
    needs_comma_ = true;
  }

  template <simdjson::constevalutil::fixed_string Key>
  void SetDouble(double value) {
    MaybeComma();
    builder_.append_key_value<Key>(value);
    needs_comma_ = true;
  }

  template <simdjson::constevalutil::fixed_string Key>
  void SetNull() {
    MaybeComma();
    builder_.append_key_value<Key>(nullptr);
    needs_comma_ = true;
  }

  template <simdjson::constevalutil::fixed_string Key, typename Container>
  void SetValue(const Container& container) {
    MaybeComma();
    builder_.append_key_value<Key>(container);
    needs_comma_ = true;
  }

  /// @}

  /// @name Runtime key methods
  /// These accept runtime string keys. Use compile-time methods when keys
  /// are string literals for better performance.
  /// @{

  void SetString(std::string_view key, std::string_view value) {
    Key(key);
    String(value);
  }

  void SetBool(std::string_view key, bool value) {
    Key(key);
    Bool(value);
  }

  void SetInt(std::string_view key, int32_t value) {
    Key(key);
    Int(value);
  }

  void SetInt64(std::string_view key, int64_t value) {
    Key(key);
    Int64(value);
  }

  void SetUint(std::string_view key, uint32_t value) {
    Key(key);
    Uint(value);
  }

  void SetUint64(std::string_view key, uint64_t value) {
    Key(key);
    Uint64(value);
  }

  void SetDouble(std::string_view key, double value) {
    Key(key);
    Double(value);
  }

  void SetNull(std::string_view key) {
    Key(key);
    Null();
  }

  template <typename Container>
  void SetValue(std::string_view key, const Container& container) {
    Key(key);
    Value(container);
  }

  /// @}

  /// Write a raw character (for unquoted values like decimal numbers)
  void RawChar(char c) { builder_.append(c); }

  /// Mark that a raw value was written (for comma tracking)
  void MarkValueWritten() { needs_comma_ = true; }

  /// Direct access to underlying builder for advanced use cases
  simdjson::builder::string_builder& builder() { return builder_; }

  std::string_view GetString() const { return builder_.view().value(); }

  void Clear() {
    builder_.clear();
    needs_comma_ = false;
  }

 private:
  void MaybeComma() {
    if (needs_comma_) {
      builder_.append_comma();
    }
  }

  simdjson::builder::string_builder builder_;
  bool needs_comma_ = false;
};

/// \brief Convert a JSON string to a pretty-printed (human-readable) format.
///
/// Uses simdjson's prettify for formatting. Returns the original string
/// if parsing fails.
///
/// \param json_str The JSON string to pretty-print
/// \return The pretty-printed JSON string
inline std::string PrettyPrintJson(std::string_view json_str) {
  simdjson::dom::parser parser;
  simdjson::padded_string padded(json_str);
  auto result = parser.parse(padded);
  if (result.error()) {
    return std::string(json_str);
  }
  return simdjson::prettify(result.value());
}

}  // namespace json
}  // namespace arrow
