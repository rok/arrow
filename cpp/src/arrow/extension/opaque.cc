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

#include "arrow/extension/opaque.h"

#include <sstream>

// Ensure simdjson is used in header-only mode
#ifndef SIMDJSON_HEADER_ONLY
#define SIMDJSON_HEADER_ONLY
#endif
#include <simdjson.h>

#include "arrow/json/json_util.h"
#include "arrow/util/logging_internal.h"

namespace arrow::extension {

std::string OpaqueType::ToString(bool show_metadata) const {
  std::stringstream ss;
  ss << "extension<" << this->extension_name()
     << "[storage_type=" << storage_type_->ToString(show_metadata)
     << ", type_name=" << type_name_ << ", vendor_name=" << vendor_name_ << "]>";
  return ss.str();
}

bool OpaqueType::ExtensionEquals(const ExtensionType& other) const {
  if (extension_name() != other.extension_name()) {
    return false;
  }
  const auto& opaque = internal::checked_cast<const OpaqueType&>(other);
  return storage_type()->Equals(*opaque.storage_type()) &&
         type_name() == opaque.type_name() && vendor_name() == opaque.vendor_name();
}

std::string OpaqueType::Serialize() const {
  json::JsonWriter writer;
  writer.StartObject();
  writer.SetString<"type_name">(type_name_);
  writer.SetString<"vendor_name">(vendor_name_);
  writer.EndObject();
  return std::string(writer.GetString());
}

Result<std::shared_ptr<DataType>> OpaqueType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized_data) const {
  simdjson::dom::parser parser;
  simdjson::padded_string padded_json(serialized_data);
  auto result = parser.parse(padded_json);
  if (result.error()) {
    return Status::Invalid("Invalid serialized JSON data for OpaqueType: ",
                           simdjson::error_message(result.error()), ": ",
                           serialized_data);
  }
  auto document = result.value();
  if (!document.is_object()) {
    return Status::Invalid("Invalid serialized JSON data for OpaqueType: not an object");
  }

  auto obj = document.get_object().value();
  auto type_name_field = obj["type_name"];
  auto vendor_name_field = obj["vendor_name"];

  if (type_name_field.error()) {
    return Status::Invalid(
        "Invalid serialized JSON data for OpaqueType: missing type_name");
  }
  if (vendor_name_field.error()) {
    return Status::Invalid(
        "Invalid serialized JSON data for OpaqueType: missing vendor_name");
  }

  if (!type_name_field.is_string()) {
    return Status::Invalid(
        "Invalid serialized JSON data for OpaqueType: type_name is not a string");
  }
  if (!vendor_name_field.is_string()) {
    return Status::Invalid(
        "Invalid serialized JSON data for OpaqueType: vendor_name is not a string");
  }

  return opaque(std::move(storage_type),
                std::string(type_name_field.get_string().value()),
                std::string(vendor_name_field.get_string().value()));
}

std::shared_ptr<Array> OpaqueType::MakeArray(std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ("arrow.opaque",
            internal::checked_cast<const ExtensionType&>(*data->type).extension_name());
  return std::make_shared<OpaqueArray>(data);
}

std::shared_ptr<DataType> opaque(std::shared_ptr<DataType> storage_type,
                                 std::string type_name, std::string vendor_name) {
  return std::make_shared<OpaqueType>(std::move(storage_type), std::move(type_name),
                                      std::move(vendor_name));
}

}  // namespace arrow::extension
