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

#include <cstdint>
#include <cstdio>
#include <sstream>
#include <string>
#include <string_view>

#include "arrow/util/visibility.h"

namespace arrow {
namespace json {

/// \brief Escape a string for JSON output and return it with surrounding quotes.
///
/// This function properly escapes special characters like quotes, backslashes,
/// and control characters according to the JSON specification.
///
/// \param s The string to escape
/// \return The escaped string surrounded by double quotes
inline std::string EscapeJsonString(std::string_view s) {
  std::ostringstream ss;
  ss << '"';
  for (char c : s) {
    switch (c) {
      case '"':
        ss << "\\\"";
        break;
      case '\\':
        ss << "\\\\";
        break;
      case '\b':
        ss << "\\b";
        break;
      case '\f':
        ss << "\\f";
        break;
      case '\n':
        ss << "\\n";
        break;
      case '\r':
        ss << "\\r";
        break;
      case '\t':
        ss << "\\t";
        break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          // Control character - escape as \u00XX
          char buf[8];
          std::snprintf(buf, sizeof(buf), "\\u%04X", static_cast<unsigned char>(c));
          ss << buf;
        } else {
          ss << c;
        }
    }
  }
  ss << '"';
  return ss.str();
}

/// \brief JSON writer with automatic comma insertion between elements.
class JsonWriter {
 public:
  JsonWriter() = default;

  void StartObject() {
    MaybeComma();
    buffer_ += '{';
    needs_comma_ = false;
  }

  void EndObject() {
    buffer_ += '}';
    needs_comma_ = true;
  }

  void StartArray() {
    MaybeComma();
    buffer_ += '[';
    needs_comma_ = false;
  }

  void EndArray() {
    buffer_ += ']';
    needs_comma_ = true;
  }

  void Key(std::string_view key) {
    MaybeComma();
    buffer_ += EscapeJsonString(key);
    buffer_ += ':';
    needs_comma_ = false;
  }

  void String(std::string_view value) {
    MaybeComma();
    buffer_ += EscapeJsonString(value);
    needs_comma_ = true;
  }

  void Bool(bool value) {
    MaybeComma();
    buffer_ += value ? "true" : "false";
    needs_comma_ = true;
  }

  void Int(int32_t value) {
    MaybeComma();
    buffer_ += std::to_string(value);
    needs_comma_ = true;
  }

  void Int64(int64_t value) {
    MaybeComma();
    buffer_ += std::to_string(value);
    needs_comma_ = true;
  }

  void Uint(uint32_t value) {
    MaybeComma();
    buffer_ += std::to_string(value);
    needs_comma_ = true;
  }

  void Uint64(uint64_t value) {
    MaybeComma();
    buffer_ += std::to_string(value);
    needs_comma_ = true;
  }

  void Double(double value) {
    MaybeComma();
    char buf[32];
    int len = std::snprintf(buf, sizeof(buf), "%.17g", value);
    buffer_.append(buf, len);
    needs_comma_ = true;
  }

  void Null() {
    MaybeComma();
    buffer_ += "null";
    needs_comma_ = true;
  }

  /// Write a raw character (for unquoted values like decimal numbers)
  void RawChar(char c) { buffer_ += c; }

  /// Mark that a raw value was written (for comma tracking)
  void MarkValueWritten() { needs_comma_ = true; }

  std::string_view GetString() const { return buffer_; }

  void Clear() {
    buffer_.clear();
    needs_comma_ = false;
  }

 private:
  void MaybeComma() {
    if (needs_comma_) {
      buffer_ += ',';
    }
  }

  std::string buffer_;
  bool needs_comma_ = false;
};

}  // namespace json
}  // namespace arrow
