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

#include <cctype>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <sstream>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array/array_dict.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_time.h"
#include "arrow/array/builder_union.h"
#include "arrow/chunked_array.h"
#include "arrow/json/from_string.h"
#include "arrow/scalar.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/float16.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/value_parsing.h"

#include <simdjson.h>

namespace arrow {

using internal::ParseValue;
using util::Float16;

namespace json {

using ::arrow::internal::checked_cast;
using ::arrow::internal::checked_pointer_cast;

namespace {

// Preprocess JSON string to handle non-standard NaN/Inf literals.
// simdjson strictly follows the JSON spec which doesn't allow these literals.
// This function replaces NaN, Inf, -Inf with quoted strings that we handle later.
std::string PreprocessNanInf(std::string_view json_string) {
  std::string result;
  result.reserve(json_string.size() + 32);  // Reserve extra space for quotes

  bool in_string = false;
  bool escape_next = false;

  for (size_t i = 0; i < json_string.size(); ++i) {
    char c = json_string[i];

    if (escape_next) {
      result.push_back(c);
      escape_next = false;
      continue;
    }

    if (c == '\\' && in_string) {
      result.push_back(c);
      escape_next = true;
      continue;
    }

    if (c == '"') {
      result.push_back(c);
      in_string = !in_string;
      continue;
    }

    if (!in_string) {
      // Check for NaN
      if (i + 3 <= json_string.size() &&
          (json_string[i] == 'N' && json_string[i + 1] == 'a' &&
           json_string[i + 2] == 'N')) {
        // Make sure it's not part of a larger word
        bool before_ok =
            (i == 0 || !std::isalnum(static_cast<unsigned char>(json_string[i - 1])));
        bool after_ok = (i + 3 >= json_string.size() ||
                         !std::isalnum(static_cast<unsigned char>(json_string[i + 3])));
        if (before_ok && after_ok) {
          result += "\"NaN\"";
          i += 2;
          continue;
        }
      }
      // Check for Inf (not -Inf, that's handled separately)
      if (i + 3 <= json_string.size() &&
          (json_string[i] == 'I' && json_string[i + 1] == 'n' &&
           json_string[i + 2] == 'f')) {
        bool before_ok =
            (i == 0 || !std::isalnum(static_cast<unsigned char>(json_string[i - 1])));
        bool after_ok = (i + 3 >= json_string.size() ||
                         !std::isalnum(static_cast<unsigned char>(json_string[i + 3])));
        if (before_ok && after_ok) {
          result += "\"Inf\"";
          i += 2;
          continue;
        }
      }
      // Check for -Inf
      if (i + 4 <= json_string.size() &&
          (json_string[i] == '-' && json_string[i + 1] == 'I' &&
           json_string[i + 2] == 'n' && json_string[i + 3] == 'f')) {
        bool before_ok =
            (i == 0 || !std::isalnum(static_cast<unsigned char>(json_string[i - 1])));
        bool after_ok = (i + 4 >= json_string.size() ||
                         !std::isalnum(static_cast<unsigned char>(json_string[i + 4])));
        if (before_ok && after_ok) {
          result += "\"-Inf\"";
          i += 3;
          continue;
        }
      }
      // Check for -NaN
      if (i + 4 <= json_string.size() &&
          (json_string[i] == '-' && json_string[i + 1] == 'N' &&
           json_string[i + 2] == 'a' && json_string[i + 3] == 'N')) {
        bool before_ok =
            (i == 0 || !std::isalnum(static_cast<unsigned char>(json_string[i - 1])));
        bool after_ok = (i + 4 >= json_string.size() ||
                         !std::isalnum(static_cast<unsigned char>(json_string[i + 4])));
        if (before_ok && after_ok) {
          result += "\"-NaN\"";
          i += 3;
          continue;
        }
      }
    }

    result.push_back(c);
  }

  return result;
}

bool IsStringLikeType(const std::shared_ptr<DataType>& type) {
  switch (type->id()) {
    case Type::STRING:
    case Type::BINARY:
    case Type::LARGE_STRING:
    case Type::LARGE_BINARY:
    case Type::STRING_VIEW:
    case Type::BINARY_VIEW:
    case Type::FIXED_SIZE_BINARY:
      return true;
    default:
      return false;
  }
}

bool IsWhitespace(char c) {
  return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

Status AppendCodepointAsUtf8(uint32_t codepoint, std::string* out) {
  if (codepoint > 0x10FFFF) {
    return Status::Invalid("Invalid Unicode code point in JSON string");
  }
  if (codepoint <= 0x7F) {
    out->push_back(static_cast<char>(codepoint));
    return Status::OK();
  }
  if (codepoint <= 0x7FF) {
    out->push_back(static_cast<char>(0xC0 | (codepoint >> 6)));
    out->push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    return Status::OK();
  }
  if (codepoint <= 0xFFFF) {
    out->push_back(static_cast<char>(0xE0 | (codepoint >> 12)));
    out->push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
    out->push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    return Status::OK();
  }
  out->push_back(static_cast<char>(0xF0 | (codepoint >> 18)));
  out->push_back(static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F)));
  out->push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
  out->push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
  return Status::OK();
}

bool IsHexDigit(char c) {
  return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
         (c >= 'A' && c <= 'F');
}

Result<uint32_t> ParseHex4(std::string_view input, size_t pos) {
  if (pos + 4 > input.size()) {
    return Status::Invalid("Truncated \\u escape in JSON string");
  }
  uint32_t value = 0;
  for (size_t i = 0; i < 4; ++i) {
    char c = input[pos + i];
    if (!IsHexDigit(c)) {
      return Status::Invalid("Invalid hex digit in \\u escape");
    }
    value <<= 4;
    if (c >= '0' && c <= '9') {
      value |= static_cast<uint32_t>(c - '0');
    } else if (c >= 'a' && c <= 'f') {
      value |= static_cast<uint32_t>(c - 'a' + 10);
    } else {
      value |= static_cast<uint32_t>(c - 'A' + 10);
    }
  }
  return value;
}

Status SkipWhitespace(std::string_view input, size_t* pos) {
  while (*pos < input.size() && IsWhitespace(input[*pos])) {
    ++(*pos);
  }
  return Status::OK();
}

Status ParseJsonStringValue(std::string_view input, size_t* pos, std::string* out) {
  if (*pos >= input.size() || input[*pos] != '"') {
    return Status::Invalid("Expected JSON string");
  }
  ++(*pos);
  out->clear();
  while (*pos < input.size()) {
    char c = input[*pos];
    if (c == '"') {
      ++(*pos);
      return Status::OK();
    }
    if (static_cast<unsigned char>(c) < 0x20) {
      return Status::Invalid("Control character in JSON string");
    }
    if (c == '\\') {
      ++(*pos);
      if (*pos >= input.size()) {
        return Status::Invalid("Truncated escape in JSON string");
      }
      char esc = input[*pos];
      switch (esc) {
        case '"':
        case '\\':
        case '/':
          out->push_back(esc);
          ++(*pos);
          break;
        case 'b':
          out->push_back('\b');
          ++(*pos);
          break;
        case 'f':
          out->push_back('\f');
          ++(*pos);
          break;
        case 'n':
          out->push_back('\n');
          ++(*pos);
          break;
        case 'r':
          out->push_back('\r');
          ++(*pos);
          break;
        case 't':
          out->push_back('\t');
          ++(*pos);
          break;
        case 'u': {
          ++(*pos);
          ARROW_ASSIGN_OR_RAISE(uint32_t codepoint, ParseHex4(input, *pos));
          *pos += 4;
          if (codepoint >= 0xD800 && codepoint <= 0xDBFF) {
            if (*pos + 6 > input.size() || input[*pos] != '\\' ||
                input[*pos + 1] != 'u') {
              return Status::Invalid("Missing low surrogate in JSON string");
            }
            *pos += 2;
            ARROW_ASSIGN_OR_RAISE(uint32_t low, ParseHex4(input, *pos));
            *pos += 4;
            if (low < 0xDC00 || low > 0xDFFF) {
              return Status::Invalid("Invalid low surrogate in JSON string");
            }
            codepoint = 0x10000 + ((codepoint - 0xD800) << 10) + (low - 0xDC00);
          } else if (codepoint >= 0xDC00 && codepoint <= 0xDFFF) {
            return Status::Invalid("Unexpected low surrogate in JSON string");
          }
          RETURN_NOT_OK(AppendCodepointAsUtf8(codepoint, out));
          break;
        }
        default:
          return Status::Invalid("Invalid escape in JSON string");
      }
      continue;
    }
    out->push_back(c);
    ++(*pos);
  }
  return Status::Invalid("Unterminated JSON string");
}

Status ParseJsonNull(std::string_view input, size_t* pos) {
  if (*pos + 4 <= input.size() && input.substr(*pos, 4) == "null") {
    *pos += 4;
    return Status::OK();
  }
  return Status::Invalid("Expected null");
}

Result<std::vector<std::optional<std::string>>> ParseJsonStringArrayUnsafe(
    std::string_view input) {
  size_t pos = 0;
  RETURN_NOT_OK(SkipWhitespace(input, &pos));
  if (pos >= input.size() || input[pos] != '[') {
    return Status::Invalid("Expected JSON array");
  }
  ++pos;

  std::vector<std::optional<std::string>> values;
  RETURN_NOT_OK(SkipWhitespace(input, &pos));
  if (pos < input.size() && input[pos] == ']') {
    ++pos;
    RETURN_NOT_OK(SkipWhitespace(input, &pos));
    if (pos != input.size()) {
      return Status::Invalid("Trailing content after JSON array");
    }
    return values;
  }

  while (pos < input.size()) {
    RETURN_NOT_OK(SkipWhitespace(input, &pos));
    if (pos >= input.size()) {
      return Status::Invalid("Unexpected end of JSON array");
    }
    if (input[pos] == '"') {
      std::string value;
      RETURN_NOT_OK(ParseJsonStringValue(input, &pos, &value));
      values.emplace_back(std::move(value));
    } else {
      RETURN_NOT_OK(ParseJsonNull(input, &pos));
      values.emplace_back(std::nullopt);
    }
    RETURN_NOT_OK(SkipWhitespace(input, &pos));
    if (pos >= input.size()) {
      return Status::Invalid("Unexpected end of JSON array");
    }
    if (input[pos] == ',') {
      ++pos;
      continue;
    }
    if (input[pos] == ']') {
      ++pos;
      RETURN_NOT_OK(SkipWhitespace(input, &pos));
      if (pos != input.size()) {
        return Status::Invalid("Trailing content after JSON array");
      }
      return values;
    }
    return Status::Invalid("Expected ',' or ']' in JSON array");
  }
  return Status::Invalid("Unterminated JSON array");
}

Result<std::optional<std::string>> ParseJsonStringScalarUnsafe(std::string_view input) {
  size_t pos = 0;
  RETURN_NOT_OK(SkipWhitespace(input, &pos));
  if (pos >= input.size()) {
    return Status::Invalid("Empty JSON input");
  }
  if (input[pos] == '"') {
    std::string value;
    RETURN_NOT_OK(ParseJsonStringValue(input, &pos, &value));
    RETURN_NOT_OK(SkipWhitespace(input, &pos));
    if (pos != input.size()) {
      return Status::Invalid("Trailing content after JSON scalar");
    }
    return value;
  }
  RETURN_NOT_OK(ParseJsonNull(input, &pos));
  RETURN_NOT_OK(SkipWhitespace(input, &pos));
  if (pos != input.size()) {
    return Status::Invalid("Trailing content after JSON scalar");
  }
  return std::nullopt;
}

Result<std::shared_ptr<Array>> BuildStringLikeArray(
    const std::shared_ptr<DataType>& type,
    const std::vector<std::optional<std::string>>& values) {
  switch (type->id()) {
    case Type::STRING: {
      StringBuilder builder;
      for (const auto& value : values) {
        if (value.has_value()) {
          RETURN_NOT_OK(builder.Append(*value));
        } else {
          RETURN_NOT_OK(builder.AppendNull());
        }
      }
      return builder.Finish();
    }
    case Type::BINARY: {
      BinaryBuilder builder;
      for (const auto& value : values) {
        if (value.has_value()) {
          RETURN_NOT_OK(builder.Append(*value));
        } else {
          RETURN_NOT_OK(builder.AppendNull());
        }
      }
      return builder.Finish();
    }
    case Type::LARGE_STRING: {
      LargeStringBuilder builder;
      for (const auto& value : values) {
        if (value.has_value()) {
          RETURN_NOT_OK(builder.Append(*value));
        } else {
          RETURN_NOT_OK(builder.AppendNull());
        }
      }
      return builder.Finish();
    }
    case Type::LARGE_BINARY: {
      LargeBinaryBuilder builder;
      for (const auto& value : values) {
        if (value.has_value()) {
          RETURN_NOT_OK(builder.Append(*value));
        } else {
          RETURN_NOT_OK(builder.AppendNull());
        }
      }
      return builder.Finish();
    }
    case Type::STRING_VIEW: {
      StringViewBuilder builder;
      for (const auto& value : values) {
        if (value.has_value()) {
          RETURN_NOT_OK(builder.Append(*value));
        } else {
          RETURN_NOT_OK(builder.AppendNull());
        }
      }
      return builder.Finish();
    }
    case Type::BINARY_VIEW: {
      BinaryViewBuilder builder;
      for (const auto& value : values) {
        if (value.has_value()) {
          RETURN_NOT_OK(builder.Append(*value));
        } else {
          RETURN_NOT_OK(builder.AppendNull());
        }
      }
      return builder.Finish();
    }
    case Type::FIXED_SIZE_BINARY: {
      auto fixed_type = checked_pointer_cast<FixedSizeBinaryType>(type);
      FixedSizeBinaryBuilder builder(fixed_type);
      for (const auto& value : values) {
        if (value.has_value()) {
          if (value->size() != static_cast<size_t>(fixed_type->byte_width())) {
            return Status::Invalid("Invalid string length ", value->size(),
                                   " in JSON input for ", type->ToString());
          }
          RETURN_NOT_OK(builder.Append(*value));
        } else {
          RETURN_NOT_OK(builder.AppendNull());
        }
      }
      return builder.Finish();
    }
    default:
      return Status::TypeError("Unsupported type for string parsing: ", type->ToString());
  }
}

const char* JsonTypeName(simdjson::dom::element_type json_type) {
  switch (json_type) {
    case simdjson::dom::element_type::NULL_VALUE:
      return "null";
    case simdjson::dom::element_type::BOOL:
      return "boolean";
    case simdjson::dom::element_type::OBJECT:
      return "object";
    case simdjson::dom::element_type::ARRAY:
      return "array";
    case simdjson::dom::element_type::STRING:
      return "string";
    case simdjson::dom::element_type::INT64:
      return "int64";
    case simdjson::dom::element_type::UINT64:
      return "uint64";
    case simdjson::dom::element_type::DOUBLE:
      return "double";
    default:
      return "unknown";
  }
}

Status JSONTypeError(const char* expected_type, simdjson::dom::element_type json_type) {
  return Status::Invalid("Expected ", expected_type, " or null, got JSON type ",
                         JsonTypeName(json_type));
}

class JSONConverter {
 public:
  virtual ~JSONConverter() = default;

  virtual Status Init() { return Status::OK(); }

  virtual Status AppendValue(simdjson::dom::element json_obj) = 0;

  Status AppendNull() { return this->builder()->AppendNull(); }

  virtual Status AppendValues(simdjson::dom::element json_array) = 0;

  virtual std::shared_ptr<ArrayBuilder> builder() = 0;

  virtual Status Finish(std::shared_ptr<Array>* out) {
    auto builder = this->builder();
    if (builder->length() == 0) {
      // Make sure the builder was initialized
      RETURN_NOT_OK(builder->Resize(1));
    }
    return builder->Finish(out);
  }

 protected:
  std::shared_ptr<DataType> type_;
};

Status GetConverter(const std::shared_ptr<DataType>&,
                    std::shared_ptr<JSONConverter>* out);

// CRTP
template <class Derived>
class ConcreteConverter : public JSONConverter {
 public:
  Result<int64_t> SizeOfJSONArray(simdjson::dom::element json_obj) {
    if (!json_obj.is_array()) {
      return JSONTypeError("array", json_obj.type());
    }
    return static_cast<int64_t>(json_obj.get_array().value().size());
  }

  Status AppendValues(simdjson::dom::element json_array) final {
    auto self = static_cast<Derived*>(this);
    ARROW_ASSIGN_OR_RAISE(auto size, SizeOfJSONArray(json_array));
    auto arr = json_array.get_array().value();
    int64_t i = 0;
    for (auto elem : arr) {
      if (i >= size) break;
      RETURN_NOT_OK(self->AppendValue(elem));
      ++i;
    }
    return Status::OK();
  }

  const std::shared_ptr<DataType>& value_type() {
    if (type_->id() != Type::DICTIONARY) {
      return type_;
    }
    return checked_cast<const DictionaryType&>(*type_).value_type();
  }

  template <typename BuilderType>
  Status MakeConcreteBuilder(std::shared_ptr<BuilderType>* out) {
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(default_memory_pool(), this->type_, &builder));
    *out = checked_pointer_cast<BuilderType>(std::move(builder));
    DCHECK(*out);
    return Status::OK();
  }
};

// ------------------------------------------------------------------------
// Converter for null arrays

class NullConverter final : public ConcreteConverter<NullConverter> {
 public:
  explicit NullConverter(const std::shared_ptr<DataType>& type) {
    type_ = type;
    builder_ = std::make_shared<NullBuilder>();
  }

  Status AppendValue(simdjson::dom::element json_obj) override {
    if (json_obj.is_null()) {
      return AppendNull();
    }
    return JSONTypeError("null", json_obj.type());
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<NullBuilder> builder_;
};

// ------------------------------------------------------------------------
// Converter for boolean arrays

class BooleanConverter final : public ConcreteConverter<BooleanConverter> {
 public:
  explicit BooleanConverter(const std::shared_ptr<DataType>& type) {
    type_ = type;
    builder_ = std::make_shared<BooleanBuilder>();
  }

  Status AppendValue(simdjson::dom::element json_obj) override {
    if (json_obj.is_null()) {
      return AppendNull();
    }
    if (json_obj.is_bool()) {
      return builder_->Append(json_obj.get_bool().value());
    }
    if (json_obj.is_int64()) {
      return builder_->Append(json_obj.get_int64().value() != 0);
    }
    if (json_obj.is_uint64()) {
      return builder_->Append(json_obj.get_uint64().value() != 0);
    }
    return JSONTypeError("boolean", json_obj.type());
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BooleanBuilder> builder_;
};

// ------------------------------------------------------------------------
// Helpers for numeric converters

// Convert single signed integer value (also {Date,Time}{32,64} and Timestamp)
template <typename T>
enable_if_physical_signed_integer<T, Status> ConvertNumber(
    simdjson::dom::element json_obj, const DataType& type, typename T::c_type* out) {
  if (json_obj.is_int64()) {
    int64_t v64 = json_obj.get_int64().value();
    *out = static_cast<typename T::c_type>(v64);
    if (*out == v64) {
      return Status::OK();
    } else {
      return Status::Invalid("Value ", v64, " out of bounds for ", type);
    }
  } else if (json_obj.is_uint64()) {
    // simdjson may parse positive integers as uint64
    uint64_t v64 = json_obj.get_uint64().value();
    if (v64 <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      int64_t v64s = static_cast<int64_t>(v64);
      *out = static_cast<typename T::c_type>(v64s);
      if (*out == v64s) {
        return Status::OK();
      } else {
        return Status::Invalid("Value ", v64, " out of bounds for ", type);
      }
    }
    return Status::Invalid("Value ", v64, " out of bounds for ", type);
  } else {
    *out = static_cast<typename T::c_type>(0);
    return JSONTypeError("signed int", json_obj.type());
  }
}

// Convert single unsigned integer value
template <typename T>
enable_if_unsigned_integer<T, Status> ConvertNumber(simdjson::dom::element json_obj,
                                                    const DataType& type,
                                                    typename T::c_type* out) {
  if (json_obj.is_uint64()) {
    uint64_t v64 = json_obj.get_uint64().value();
    *out = static_cast<typename T::c_type>(v64);
    if (*out == v64) {
      return Status::OK();
    } else {
      return Status::Invalid("Value ", v64, " out of bounds for ", type);
    }
  } else if (json_obj.is_int64()) {
    // simdjson may parse small positive integers as int64
    int64_t v64 = json_obj.get_int64().value();
    if (v64 >= 0) {
      uint64_t v64u = static_cast<uint64_t>(v64);
      *out = static_cast<typename T::c_type>(v64u);
      if (*out == v64u) {
        return Status::OK();
      } else {
        return Status::Invalid("Value ", v64, " out of bounds for ", type);
      }
    }
    return Status::Invalid("Value ", v64, " out of bounds for ", type);
  } else {
    *out = static_cast<typename T::c_type>(0);
    return JSONTypeError("unsigned int", json_obj.type());
  }
}

// Convert float16/HalfFloatType
template <typename T>
enable_if_half_float<T, Status> ConvertNumber(simdjson::dom::element json_obj,
                                              const DataType& type, uint16_t* out) {
  if (json_obj.is_double()) {
    double f64 = json_obj.get_double().value();
    *out = Float16(f64).bits();
    return Status::OK();
  } else if (json_obj.is_uint64()) {
    uint64_t u64 = json_obj.get_uint64().value();
    double f64 = static_cast<double>(u64);
    *out = Float16(f64).bits();
    return Status::OK();
  } else if (json_obj.is_int64()) {
    int64_t i64 = json_obj.get_int64().value();
    double f64 = static_cast<double>(i64);
    *out = Float16(f64).bits();
    return Status::OK();
  } else if (json_obj.is_string()) {
    // Handle NaN/Inf that were preprocessed to strings
    std::string_view str = json_obj.get_string().value();
    if (str == "NaN" || str == "-NaN") {
      *out = Float16(std::nan("")).bits();
      return Status::OK();
    } else if (str == "Inf") {
      *out = Float16(INFINITY).bits();
      return Status::OK();
    } else if (str == "-Inf") {
      *out = Float16(-INFINITY).bits();
      return Status::OK();
    }
    *out = static_cast<uint16_t>(0);
    return JSONTypeError("number", json_obj.type());
  } else {
    *out = static_cast<uint16_t>(0);
    return JSONTypeError("number", json_obj.type());
  }
}

// Convert single floating point value
template <typename T>
enable_if_physical_floating_point<T, Status> ConvertNumber(
    simdjson::dom::element json_obj, const DataType& type, typename T::c_type* out) {
  // simdjson doesn't have is_number(), so check all numeric types
  if (json_obj.is_double()) {
    *out = static_cast<typename T::c_type>(json_obj.get_double().value());
    return Status::OK();
  } else if (json_obj.is_int64()) {
    *out = static_cast<typename T::c_type>(json_obj.get_int64().value());
    return Status::OK();
  } else if (json_obj.is_uint64()) {
    *out = static_cast<typename T::c_type>(json_obj.get_uint64().value());
    return Status::OK();
  } else if (json_obj.is_string()) {
    // Handle NaN/Inf that were preprocessed to strings
    std::string_view str = json_obj.get_string().value();
    if (str == "NaN" || str == "-NaN") {
      *out = static_cast<typename T::c_type>(std::nan(""));
      return Status::OK();
    } else if (str == "Inf") {
      *out = static_cast<typename T::c_type>(INFINITY);
      return Status::OK();
    } else if (str == "-Inf") {
      *out = static_cast<typename T::c_type>(-INFINITY);
      return Status::OK();
    }
    *out = static_cast<typename T::c_type>(0);
    return JSONTypeError("number", json_obj.type());
  } else {
    *out = static_cast<typename T::c_type>(0);
    return JSONTypeError("number", json_obj.type());
  }
}

// ------------------------------------------------------------------------
// Converter for int arrays

template <typename Type, typename BuilderType = typename TypeTraits<Type>::BuilderType>
class IntegerConverter final
    : public ConcreteConverter<IntegerConverter<Type, BuilderType>> {
  using c_type = typename Type::c_type;

  static constexpr auto is_signed = std::is_signed<c_type>::value;

 public:
  explicit IntegerConverter(const std::shared_ptr<DataType>& type) { this->type_ = type; }

  Status Init() override { return this->MakeConcreteBuilder(&builder_); }

  Status AppendValue(simdjson::dom::element json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    c_type value;
    RETURN_NOT_OK(ConvertNumber<Type>(json_obj, *this->type_, &value));
    return builder_->Append(value);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BuilderType> builder_;
};

// ------------------------------------------------------------------------
// Converter for float arrays

template <typename Type, typename BuilderType = typename TypeTraits<Type>::BuilderType>
class FloatConverter final : public ConcreteConverter<FloatConverter<Type, BuilderType>> {
  using c_type = typename Type::c_type;

 public:
  explicit FloatConverter(const std::shared_ptr<DataType>& type) { this->type_ = type; }

  Status Init() override { return this->MakeConcreteBuilder(&builder_); }

  Status AppendValue(simdjson::dom::element json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    c_type value;
    RETURN_NOT_OK(ConvertNumber<Type>(json_obj, *this->type_, &value));
    return builder_->Append(value);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BuilderType> builder_;
};

// ------------------------------------------------------------------------
// Converter for decimal arrays

template <typename DecimalSubtype, typename DecimalValue, typename BuilderType>
class DecimalConverter final
    : public ConcreteConverter<
          DecimalConverter<DecimalSubtype, DecimalValue, BuilderType>> {
 public:
  explicit DecimalConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
    decimal_type_ = &checked_cast<const DecimalSubtype&>(*this->value_type());
  }

  Status Init() override { return this->MakeConcreteBuilder(&builder_); }

  Status AppendValue(simdjson::dom::element json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    if (json_obj.is_string()) {
      int32_t precision, scale;
      DecimalValue d;
      std::string_view view = json_obj.get_string().value();
      RETURN_NOT_OK(DecimalValue::FromString(view, &d, &precision, &scale));
      if (scale != decimal_type_->scale()) {
        return Status::Invalid("Invalid scale for decimal: expected ",
                               decimal_type_->scale(), ", got ", scale);
      }
      return builder_->Append(d);
    }
    return JSONTypeError("decimal string", json_obj.type());
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BuilderType> builder_;
  const DecimalSubtype* decimal_type_;
};

template <typename BuilderType = typename TypeTraits<Decimal32Type>::BuilderType>
using Decimal32Converter = DecimalConverter<Decimal32Type, Decimal32, BuilderType>;
template <typename BuilderType = typename TypeTraits<Decimal64Type>::BuilderType>
using Decimal64Converter = DecimalConverter<Decimal64Type, Decimal64, BuilderType>;
template <typename BuilderType = typename TypeTraits<Decimal128Type>::BuilderType>
using Decimal128Converter = DecimalConverter<Decimal128Type, Decimal128, BuilderType>;
template <typename BuilderType = typename TypeTraits<Decimal256Type>::BuilderType>
using Decimal256Converter = DecimalConverter<Decimal256Type, Decimal256, BuilderType>;

// ------------------------------------------------------------------------
// Converter for timestamp arrays

class TimestampConverter final : public ConcreteConverter<TimestampConverter> {
 public:
  explicit TimestampConverter(const std::shared_ptr<DataType>& type)
      : timestamp_type_{checked_cast<const TimestampType*>(type.get())} {
    this->type_ = type;
    builder_ = std::make_shared<TimestampBuilder>(type, default_memory_pool());
  }

  Status AppendValue(simdjson::dom::element json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    int64_t value;
    if (json_obj.is_int64() || json_obj.is_uint64() || json_obj.is_double()) {
      RETURN_NOT_OK(ConvertNumber<Int64Type>(json_obj, *this->type_, &value));
    } else if (json_obj.is_string()) {
      std::string_view view = json_obj.get_string().value();
      if (!ParseValue(*timestamp_type_, view.data(), view.size(), &value)) {
        return Status::Invalid("couldn't parse timestamp from ", view);
      }
    } else {
      return JSONTypeError("timestamp", json_obj.type());
    }
    return builder_->Append(value);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  const TimestampType* timestamp_type_;
  std::shared_ptr<TimestampBuilder> builder_;
};

// ------------------------------------------------------------------------
// Converter for day-time interval arrays

class DayTimeIntervalConverter final
    : public ConcreteConverter<DayTimeIntervalConverter> {
 public:
  explicit DayTimeIntervalConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
    builder_ = std::make_shared<DayTimeIntervalBuilder>(default_memory_pool());
  }

  Status AppendValue(simdjson::dom::element json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    DayTimeIntervalType::DayMilliseconds value;
    if (!json_obj.is_array()) {
      return JSONTypeError("array", json_obj.type());
    }
    auto arr = json_obj.get_array().value();
    if (arr.size() != 2) {
      return Status::Invalid(
          "day time interval pair must have exactly two elements, had ", arr.size());
    }
    RETURN_NOT_OK(ConvertNumber<Int32Type>(arr.at(0).value(), *this->type_, &value.days));
    RETURN_NOT_OK(
        ConvertNumber<Int32Type>(arr.at(1).value(), *this->type_, &value.milliseconds));
    return builder_->Append(value);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<DayTimeIntervalBuilder> builder_;
};

class MonthDayNanoIntervalConverter final
    : public ConcreteConverter<MonthDayNanoIntervalConverter> {
 public:
  explicit MonthDayNanoIntervalConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
    builder_ = std::make_shared<MonthDayNanoIntervalBuilder>(default_memory_pool());
  }

  Status AppendValue(simdjson::dom::element json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    MonthDayNanoIntervalType::MonthDayNanos value;
    if (!json_obj.is_array()) {
      return JSONTypeError("array", json_obj.type());
    }
    auto arr = json_obj.get_array().value();
    if (arr.size() != 3) {
      return Status::Invalid(
          "month_day_nano_interval  must have exactly 3 elements, had ", arr.size());
    }
    RETURN_NOT_OK(
        ConvertNumber<Int32Type>(arr.at(0).value(), *this->type_, &value.months));
    RETURN_NOT_OK(ConvertNumber<Int32Type>(arr.at(1).value(), *this->type_, &value.days));
    RETURN_NOT_OK(
        ConvertNumber<Int64Type>(arr.at(2).value(), *this->type_, &value.nanoseconds));

    return builder_->Append(value);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<MonthDayNanoIntervalBuilder> builder_;
};

// ------------------------------------------------------------------------
// Converter for binary and string arrays

template <typename Type, typename BuilderType = typename TypeTraits<Type>::BuilderType>
class StringConverter final
    : public ConcreteConverter<StringConverter<Type, BuilderType>> {
 public:
  explicit StringConverter(const std::shared_ptr<DataType>& type) { this->type_ = type; }

  Status Init() override { return this->MakeConcreteBuilder(&builder_); }

  Status AppendValue(simdjson::dom::element json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    if (json_obj.is_string()) {
      std::string_view view = json_obj.get_string().value();
      return builder_->Append(view);
    } else {
      return JSONTypeError("string", json_obj.type());
    }
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BuilderType> builder_;
};

// ------------------------------------------------------------------------
// Converter for fixed-size binary arrays

template <typename BuilderType = typename TypeTraits<FixedSizeBinaryType>::BuilderType>
class FixedSizeBinaryConverter final
    : public ConcreteConverter<FixedSizeBinaryConverter<BuilderType>> {
 public:
  explicit FixedSizeBinaryConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
  }

  Status Init() override { return this->MakeConcreteBuilder(&builder_); }

  Status AppendValue(simdjson::dom::element json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    if (json_obj.is_string()) {
      std::string_view view = json_obj.get_string().value();
      if (view.length() != static_cast<size_t>(builder_->byte_width())) {
        std::stringstream ss;
        ss << "Invalid string length " << view.length() << " in JSON input for "
           << this->type_->ToString();
        return Status::Invalid(ss.str());
      }
      return builder_->Append(view);
    } else {
      return JSONTypeError("string", json_obj.type());
    }
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BuilderType> builder_;
};

// ------------------------------------------------------------------------
// Converter for list arrays

template <typename TYPE>
class VarLengthListLikeConverter final
    : public ConcreteConverter<VarLengthListLikeConverter<TYPE>> {
 public:
  using BuilderType = typename TypeTraits<TYPE>::BuilderType;

  explicit VarLengthListLikeConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
  }

  Status Init() override {
    const auto& var_length_list_like_type = checked_cast<const TYPE&>(*this->type_);
    RETURN_NOT_OK(
        GetConverter(var_length_list_like_type.value_type(), &child_converter_));
    auto child_builder = child_converter_->builder();
    builder_ =
        std::make_shared<BuilderType>(default_memory_pool(), child_builder, this->type_);
    return Status::OK();
  }

  Status AppendValue(simdjson::dom::element json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    // Extend the child converter with this JSON array
    ARROW_ASSIGN_OR_RAISE(auto size, this->SizeOfJSONArray(json_obj));
    RETURN_NOT_OK(builder_->Append(true, size));
    return child_converter_->AppendValues(json_obj);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BuilderType> builder_;
  std::shared_ptr<JSONConverter> child_converter_;
};

// ------------------------------------------------------------------------
// Converter for map arrays

class MapConverter final : public ConcreteConverter<MapConverter> {
 public:
  explicit MapConverter(const std::shared_ptr<DataType>& type) { type_ = type; }

  Status Init() override {
    const auto& map_type = checked_cast<const MapType&>(*type_);
    RETURN_NOT_OK(GetConverter(map_type.key_type(), &key_converter_));
    RETURN_NOT_OK(GetConverter(map_type.item_type(), &item_converter_));
    auto key_builder = key_converter_->builder();
    auto item_builder = item_converter_->builder();
    builder_ = std::make_shared<MapBuilder>(default_memory_pool(), key_builder,
                                            item_builder, type_);
    return Status::OK();
  }

  Status AppendValue(simdjson::dom::element json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    RETURN_NOT_OK(builder_->Append());
    if (!json_obj.is_array()) {
      return JSONTypeError("array", json_obj.type());
    }
    auto arr = json_obj.get_array().value();
    for (auto json_pair : arr) {
      if (!json_pair.is_array()) {
        return JSONTypeError("array", json_pair.type());
      }
      auto pair_arr = json_pair.get_array().value();
      if (pair_arr.size() != 2) {
        return Status::Invalid("key item pair must have exactly two elements, had ",
                               pair_arr.size());
      }
      auto key_elem = pair_arr.at(0).value();
      auto item_elem = pair_arr.at(1).value();
      if (key_elem.is_null()) {
        return Status::Invalid("null key is invalid");
      }
      RETURN_NOT_OK(key_converter_->AppendValue(key_elem));
      RETURN_NOT_OK(item_converter_->AppendValue(item_elem));
    }
    return Status::OK();
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<MapBuilder> builder_;
  std::shared_ptr<JSONConverter> key_converter_, item_converter_;
};

// ------------------------------------------------------------------------
// Converter for fixed size list arrays

class FixedSizeListConverter final : public ConcreteConverter<FixedSizeListConverter> {
 public:
  explicit FixedSizeListConverter(const std::shared_ptr<DataType>& type) { type_ = type; }

  Status Init() override {
    const auto& list_type = checked_cast<const FixedSizeListType&>(*type_);
    list_size_ = list_type.list_size();
    RETURN_NOT_OK(GetConverter(list_type.value_type(), &child_converter_));
    auto child_builder = child_converter_->builder();
    builder_ = std::make_shared<FixedSizeListBuilder>(default_memory_pool(),
                                                      child_builder, type_);
    return Status::OK();
  }

  Status AppendValue(simdjson::dom::element json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    RETURN_NOT_OK(builder_->Append());
    // Extend the child converter with this JSON array
    RETURN_NOT_OK(child_converter_->AppendValues(json_obj));
    auto arr = json_obj.get_array().value();
    if (arr.size() != static_cast<size_t>(list_size_)) {
      return Status::Invalid("incorrect list size ", arr.size());
    }
    return Status::OK();
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  int32_t list_size_;
  std::shared_ptr<FixedSizeListBuilder> builder_;
  std::shared_ptr<JSONConverter> child_converter_;
};

// ------------------------------------------------------------------------
// Converter for struct arrays

class StructConverter final : public ConcreteConverter<StructConverter> {
 public:
  explicit StructConverter(const std::shared_ptr<DataType>& type) { type_ = type; }

  Status Init() override {
    std::vector<std::shared_ptr<ArrayBuilder>> child_builders;
    for (const auto& field : type_->fields()) {
      std::shared_ptr<JSONConverter> child_converter;
      RETURN_NOT_OK(GetConverter(field->type(), &child_converter));
      child_converters_.push_back(child_converter);
      child_builders.push_back(child_converter->builder());
    }
    builder_ = std::make_shared<StructBuilder>(type_, default_memory_pool(),
                                               std::move(child_builders));
    return Status::OK();
  }

  // Append a JSON value that is either an array of N elements in order
  // or an object mapping struct names to values (omitted struct members
  // are mapped to null).
  Status AppendValue(simdjson::dom::element json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    if (json_obj.is_array()) {
      auto arr = json_obj.get_array().value();
      auto size = arr.size();
      auto expected_size = static_cast<size_t>(type_->num_fields());
      if (size != expected_size) {
        return Status::Invalid("Expected array of size ", expected_size,
                               ", got array of size ", size);
      }
      size_t i = 0;
      for (auto elem : arr) {
        RETURN_NOT_OK(child_converters_[i]->AppendValue(elem));
        ++i;
      }
      return builder_->Append();
    }
    if (json_obj.is_object()) {
      auto obj = json_obj.get_object().value();
      size_t remaining = obj.size();
      auto num_children = type_->num_fields();
      for (int32_t i = 0; i < num_children; ++i) {
        const auto& field = type_->field(i);
        auto found = obj[field->name()];
        if (!found.error()) {
          --remaining;
          RETURN_NOT_OK(child_converters_[i]->AppendValue(found.value()));
        } else {
          RETURN_NOT_OK(child_converters_[i]->AppendNull());
        }
      }
      if (remaining > 0) {
        // Serialize the object for error message
        std::ostringstream ss;
        ss << json_obj;
        return Status::Invalid("Unexpected members in JSON object for type ",
                               type_->ToString(), " Object: ", ss.str());
      }
      return builder_->Append();
    }
    return JSONTypeError("array or object", json_obj.type());
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<StructBuilder> builder_;
  std::vector<std::shared_ptr<JSONConverter>> child_converters_;
};

// ------------------------------------------------------------------------
// Converter for union arrays

class UnionConverter final : public ConcreteConverter<UnionConverter> {
 public:
  explicit UnionConverter(const std::shared_ptr<DataType>& type) { type_ = type; }

  Status Init() override {
    auto union_type = checked_cast<const UnionType*>(type_.get());
    mode_ = union_type->mode();
    type_id_to_child_num_.clear();
    type_id_to_child_num_.resize(union_type->max_type_code() + 1, -1);
    int child_i = 0;
    for (auto type_id : union_type->type_codes()) {
      type_id_to_child_num_[type_id] = child_i++;
    }
    std::vector<std::shared_ptr<ArrayBuilder>> child_builders;
    for (const auto& field : type_->fields()) {
      std::shared_ptr<JSONConverter> child_converter;
      RETURN_NOT_OK(GetConverter(field->type(), &child_converter));
      child_converters_.push_back(child_converter);
      child_builders.push_back(child_converter->builder());
    }
    if (mode_ == UnionMode::DENSE) {
      builder_ = std::make_shared<DenseUnionBuilder>(default_memory_pool(),
                                                     std::move(child_builders), type_);
    } else {
      builder_ = std::make_shared<SparseUnionBuilder>(default_memory_pool(),
                                                      std::move(child_builders), type_);
    }
    return Status::OK();
  }

  // Append a JSON value that must be a 2-long array, containing the type_id
  // and value of the UnionArray's slot.
  Status AppendValue(simdjson::dom::element json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    if (!json_obj.is_array()) {
      return JSONTypeError("array", json_obj.type());
    }
    auto arr = json_obj.get_array().value();
    if (arr.size() != 2) {
      return Status::Invalid("Expected [type_id, value] pair, got array of size ",
                             arr.size());
    }
    auto id_obj = arr.at(0).value();
    if (!id_obj.is_int64() && !id_obj.is_uint64()) {
      return JSONTypeError("int", id_obj.type());
    }

    int64_t id_val = id_obj.is_int64()
                         ? id_obj.get_int64().value()
                         : static_cast<int64_t>(id_obj.get_uint64().value());
    auto id = static_cast<int8_t>(id_val);
    auto child_num = type_id_to_child_num_[id];
    if (child_num == -1) {
      return Status::Invalid("type_id ", id, " not found in ", *type_);
    }

    auto child_converter = child_converters_[child_num];
    if (mode_ == UnionMode::SPARSE) {
      RETURN_NOT_OK(checked_cast<SparseUnionBuilder&>(*builder_).Append(id));
      for (auto&& other_converter : child_converters_) {
        if (other_converter != child_converter) {
          RETURN_NOT_OK(other_converter->AppendNull());
        }
      }
    } else {
      RETURN_NOT_OK(checked_cast<DenseUnionBuilder&>(*builder_).Append(id));
    }
    return child_converter->AppendValue(arr.at(1).value());
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  UnionMode::type mode_;
  std::shared_ptr<ArrayBuilder> builder_;
  std::vector<std::shared_ptr<JSONConverter>> child_converters_;
  std::vector<int8_t> type_id_to_child_num_;
};

// ------------------------------------------------------------------------
// General conversion functions

Status ConversionNotImplemented(const std::shared_ptr<DataType>& type) {
  return Status::NotImplemented("JSON conversion to ", type->ToString(),
                                " not implemented");
}

Status GetDictConverter(const std::shared_ptr<DataType>& type,
                        std::shared_ptr<JSONConverter>* out) {
  std::shared_ptr<JSONConverter> res;

  const auto value_type = checked_cast<const DictionaryType&>(*type).value_type();

#define SIMPLE_CONVERTER_CASE(ID, CLASS, TYPE)                    \
  case ID:                                                        \
    res = std::make_shared<CLASS<DictionaryBuilder<TYPE>>>(type); \
    break;

#define PARAM_CONVERTER_CASE(ID, CLASS, TYPE)                           \
  case ID:                                                              \
    res = std::make_shared<CLASS<TYPE, DictionaryBuilder<TYPE>>>(type); \
    break;

  switch (value_type->id()) {
    PARAM_CONVERTER_CASE(Type::INT8, IntegerConverter, Int8Type)
    PARAM_CONVERTER_CASE(Type::INT16, IntegerConverter, Int16Type)
    PARAM_CONVERTER_CASE(Type::INT32, IntegerConverter, Int32Type)
    PARAM_CONVERTER_CASE(Type::INT64, IntegerConverter, Int64Type)
    PARAM_CONVERTER_CASE(Type::UINT8, IntegerConverter, UInt8Type)
    PARAM_CONVERTER_CASE(Type::UINT16, IntegerConverter, UInt16Type)
    PARAM_CONVERTER_CASE(Type::UINT32, IntegerConverter, UInt32Type)
    PARAM_CONVERTER_CASE(Type::UINT64, IntegerConverter, UInt64Type)
    PARAM_CONVERTER_CASE(Type::FLOAT, FloatConverter, FloatType)
    PARAM_CONVERTER_CASE(Type::DOUBLE, FloatConverter, DoubleType)
    PARAM_CONVERTER_CASE(Type::STRING, StringConverter, StringType)
    PARAM_CONVERTER_CASE(Type::BINARY, StringConverter, BinaryType)
    PARAM_CONVERTER_CASE(Type::LARGE_STRING, StringConverter, LargeStringType)
    PARAM_CONVERTER_CASE(Type::LARGE_BINARY, StringConverter, LargeBinaryType)
    PARAM_CONVERTER_CASE(Type::STRING_VIEW, StringConverter, StringViewType)
    PARAM_CONVERTER_CASE(Type::BINARY_VIEW, StringConverter, BinaryViewType)
    SIMPLE_CONVERTER_CASE(Type::FIXED_SIZE_BINARY, FixedSizeBinaryConverter,
                          FixedSizeBinaryType)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL32, Decimal32Converter, Decimal32Type)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL64, Decimal64Converter, Decimal64Type)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL128, Decimal128Converter, Decimal128Type)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL256, Decimal256Converter, Decimal256Type)
    default:
      return ConversionNotImplemented(type);
  }

#undef SIMPLE_CONVERTER_CASE
#undef PARAM_CONVERTER_CASE

  RETURN_NOT_OK(res->Init());
  *out = res;
  return Status::OK();
}

Status GetConverter(const std::shared_ptr<DataType>& type,
                    std::shared_ptr<JSONConverter>* out) {
  if (type->id() == Type::DICTIONARY) {
    return GetDictConverter(type, out);
  }

  std::shared_ptr<JSONConverter> res;

#define SIMPLE_CONVERTER_CASE(ID, CLASS) \
  case ID:                               \
    res = std::make_shared<CLASS>(type); \
    break;

  switch (type->id()) {
    SIMPLE_CONVERTER_CASE(Type::INT8, IntegerConverter<Int8Type>)
    SIMPLE_CONVERTER_CASE(Type::INT16, IntegerConverter<Int16Type>)
    SIMPLE_CONVERTER_CASE(Type::INT32, IntegerConverter<Int32Type>)
    SIMPLE_CONVERTER_CASE(Type::INT64, IntegerConverter<Int64Type>)
    SIMPLE_CONVERTER_CASE(Type::UINT8, IntegerConverter<UInt8Type>)
    SIMPLE_CONVERTER_CASE(Type::UINT16, IntegerConverter<UInt16Type>)
    SIMPLE_CONVERTER_CASE(Type::UINT32, IntegerConverter<UInt32Type>)
    SIMPLE_CONVERTER_CASE(Type::UINT64, IntegerConverter<UInt64Type>)
    SIMPLE_CONVERTER_CASE(Type::TIMESTAMP, TimestampConverter)
    SIMPLE_CONVERTER_CASE(Type::DATE32, IntegerConverter<Date32Type>)
    SIMPLE_CONVERTER_CASE(Type::DATE64, IntegerConverter<Date64Type>)
    SIMPLE_CONVERTER_CASE(Type::TIME32, IntegerConverter<Time32Type>)
    SIMPLE_CONVERTER_CASE(Type::TIME64, IntegerConverter<Time64Type>)
    SIMPLE_CONVERTER_CASE(Type::DURATION, IntegerConverter<DurationType>)
    SIMPLE_CONVERTER_CASE(Type::NA, NullConverter)
    SIMPLE_CONVERTER_CASE(Type::BOOL, BooleanConverter)
    SIMPLE_CONVERTER_CASE(Type::HALF_FLOAT, IntegerConverter<HalfFloatType>)
    SIMPLE_CONVERTER_CASE(Type::FLOAT, FloatConverter<FloatType>)
    SIMPLE_CONVERTER_CASE(Type::DOUBLE, FloatConverter<DoubleType>)
    SIMPLE_CONVERTER_CASE(Type::LIST, VarLengthListLikeConverter<ListType>)
    SIMPLE_CONVERTER_CASE(Type::LARGE_LIST, VarLengthListLikeConverter<LargeListType>)
    SIMPLE_CONVERTER_CASE(Type::LIST_VIEW, VarLengthListLikeConverter<ListViewType>)
    SIMPLE_CONVERTER_CASE(Type::LARGE_LIST_VIEW,
                          VarLengthListLikeConverter<LargeListViewType>)
    SIMPLE_CONVERTER_CASE(Type::MAP, MapConverter)
    SIMPLE_CONVERTER_CASE(Type::FIXED_SIZE_LIST, FixedSizeListConverter)
    SIMPLE_CONVERTER_CASE(Type::STRUCT, StructConverter)
    SIMPLE_CONVERTER_CASE(Type::STRING, StringConverter<StringType>)
    SIMPLE_CONVERTER_CASE(Type::BINARY, StringConverter<BinaryType>)
    SIMPLE_CONVERTER_CASE(Type::LARGE_STRING, StringConverter<LargeStringType>)
    SIMPLE_CONVERTER_CASE(Type::LARGE_BINARY, StringConverter<LargeBinaryType>)
    SIMPLE_CONVERTER_CASE(Type::STRING_VIEW, StringConverter<StringViewType>)
    SIMPLE_CONVERTER_CASE(Type::BINARY_VIEW, StringConverter<BinaryViewType>)
    SIMPLE_CONVERTER_CASE(Type::FIXED_SIZE_BINARY, FixedSizeBinaryConverter<>)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL32, Decimal32Converter<>)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL64, Decimal64Converter<>)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL128, Decimal128Converter<>)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL256, Decimal256Converter<>)
    SIMPLE_CONVERTER_CASE(Type::SPARSE_UNION, UnionConverter)
    SIMPLE_CONVERTER_CASE(Type::DENSE_UNION, UnionConverter)
    SIMPLE_CONVERTER_CASE(Type::INTERVAL_MONTHS, IntegerConverter<MonthIntervalType>)
    SIMPLE_CONVERTER_CASE(Type::INTERVAL_DAY_TIME, DayTimeIntervalConverter)
    SIMPLE_CONVERTER_CASE(Type::INTERVAL_MONTH_DAY_NANO, MonthDayNanoIntervalConverter)
    default:
      return ConversionNotImplemented(type);
  }

#undef SIMPLE_CONVERTER_CASE

  RETURN_NOT_OK(res->Init());
  *out = res;
  return Status::OK();
}

}  // namespace

Result<std::shared_ptr<Array>> ArrayFromJSONString(const std::shared_ptr<DataType>& type,
                                                   std::string_view json_string) {
  std::shared_ptr<JSONConverter> converter;
  RETURN_NOT_OK(GetConverter(type, &converter));

  // Preprocess to handle NaN/Inf literals (not standard JSON)
  std::string preprocessed = PreprocessNanInf(json_string);

  simdjson::dom::parser parser;
  simdjson::padded_string padded_json(preprocessed);
  auto result = parser.parse(padded_json);
  if (result.error()) {
    // Preserve RapidJSON's permissive UTF-8 behavior for string-like types.
    if (IsStringLikeType(type)) {
      ARROW_ASSIGN_OR_RAISE(auto values, ParseJsonStringArrayUnsafe(preprocessed));
      return BuildStringLikeArray(type, values);
    }
    return Status::Invalid("JSON parse error: ", simdjson::error_message(result.error()));
  }
  auto json_doc = result.value();

  // The JSON document should be an array, append it
  RETURN_NOT_OK(converter->AppendValues(json_doc));
  std::shared_ptr<Array> out;
  RETURN_NOT_OK(converter->Finish(&out));
  return out;
}

Result<std::shared_ptr<Array>> ArrayFromJSONString(const std::shared_ptr<DataType>& type,
                                                   const std::string& json_string) {
  return ArrayFromJSONString(type, std::string_view(json_string));
}

Result<std::shared_ptr<Array>> ArrayFromJSONString(const std::shared_ptr<DataType>& type,
                                                   const char* json_string) {
  return ArrayFromJSONString(type, std::string_view(json_string));
}

Result<std::shared_ptr<ChunkedArray>> ChunkedArrayFromJSONString(
    const std::shared_ptr<DataType>& type, const std::vector<std::string>& json_strings) {
  ArrayVector out_chunks;
  out_chunks.reserve(json_strings.size());
  for (const std::string& chunk_json : json_strings) {
    out_chunks.emplace_back();
    ARROW_ASSIGN_OR_RAISE(out_chunks.back(), ArrayFromJSONString(type, chunk_json));
  }
  return std::make_shared<ChunkedArray>(std::move(out_chunks), type);
}

Result<std::shared_ptr<Array>> DictArrayFromJSONString(
    const std::shared_ptr<DataType>& type, std::string_view indices_json,
    std::string_view dictionary_json) {
  if (type->id() != Type::DICTIONARY) {
    return Status::TypeError("DictArrayFromJSON requires dictionary type, got ", *type);
  }

  const auto& dictionary_type = checked_cast<const DictionaryType&>(*type);

  ARROW_ASSIGN_OR_RAISE(auto indices,
                        ArrayFromJSONString(dictionary_type.index_type(), indices_json));
  ARROW_ASSIGN_OR_RAISE(auto dictionary, ArrayFromJSONString(dictionary_type.value_type(),
                                                             dictionary_json));
  return DictionaryArray::FromArrays(type, std::move(indices), std::move(dictionary));
}

Result<std::shared_ptr<Scalar>> ScalarFromJSONString(
    const std::shared_ptr<DataType>& type, std::string_view json_string) {
  std::shared_ptr<JSONConverter> converter;
  RETURN_NOT_OK(GetConverter(type, &converter));

  // Preprocess to handle NaN/Inf literals (not standard JSON)
  std::string preprocessed = PreprocessNanInf(json_string);

  simdjson::dom::parser parser;
  simdjson::padded_string padded_json(preprocessed);
  auto result = parser.parse(padded_json);
  if (result.error()) {
    // Preserve RapidJSON's permissive UTF-8 behavior for string-like types.
    if (IsStringLikeType(type)) {
      ARROW_ASSIGN_OR_RAISE(auto value, ParseJsonStringScalarUnsafe(preprocessed));
      ARROW_ASSIGN_OR_RAISE(auto array,
                            BuildStringLikeArray(type, {std::move(value)}));
      DCHECK_EQ(array->length(), 1);
      return array->GetScalar(0);
    }
    return Status::Invalid("JSON parse error: ", simdjson::error_message(result.error()));
  }
  auto json_doc = result.value();

  std::shared_ptr<Array> array;
  RETURN_NOT_OK(converter->AppendValue(json_doc));
  RETURN_NOT_OK(converter->Finish(&array));
  DCHECK_EQ(array->length(), 1);
  return array->GetScalar(0);
}

Result<std::shared_ptr<Scalar>> DictScalarFromJSONString(
    const std::shared_ptr<DataType>& type, std::string_view index_json,
    std::string_view dictionary_json) {
  if (type->id() != Type::DICTIONARY) {
    return Status::TypeError("DictScalarFromJSONString requires dictionary type, got ",
                             *type);
  }

  const auto& dictionary_type = checked_cast<const DictionaryType&>(*type);

  std::shared_ptr<Array> dictionary;
  ARROW_ASSIGN_OR_RAISE(auto index,
                        ScalarFromJSONString(dictionary_type.index_type(), index_json));
  ARROW_ASSIGN_OR_RAISE(
      dictionary, ArrayFromJSONString(dictionary_type.value_type(), dictionary_json));

  return DictionaryScalar::Make(std::move(index), std::move(dictionary));
}

}  // namespace json
}  // namespace arrow
