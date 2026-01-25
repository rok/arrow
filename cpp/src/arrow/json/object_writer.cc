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

#include "arrow/json/object_writer.h"

#include <simdjson.h>

namespace arrow {
namespace json {
namespace internal {

class ObjectWriter::Impl {
 public:
  void SetString(std::string_view key, std::string_view value) {
    if (!first_) {
      builder_.append_comma();
    }
    first_ = false;
    builder_.append_key_value(key, value);
  }

  void SetBool(std::string_view key, bool value) {
    if (!first_) {
      builder_.append_comma();
    }
    first_ = false;
    builder_.append_key_value(key, value);
  }

  std::string Serialize() {
    simdjson::builder::string_builder result;
    result.start_object();
    result.append_raw(builder_.view().value());
    result.end_object();
    return static_cast<std::string>(result);
  }

 private:
  simdjson::builder::string_builder builder_;
  bool first_ = true;
};

ObjectWriter::ObjectWriter() : impl_(new ObjectWriter::Impl()) {}

ObjectWriter::~ObjectWriter() = default;

void ObjectWriter::SetString(std::string_view key, std::string_view value) {
  impl_->SetString(key, value);
}

void ObjectWriter::SetBool(std::string_view key, bool value) {
  impl_->SetBool(key, value);
}

std::string ObjectWriter::Serialize() { return impl_->Serialize(); }

}  // namespace internal
}  // namespace json
}  // namespace arrow
