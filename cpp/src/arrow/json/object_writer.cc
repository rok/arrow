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

#include <sstream>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/json/json_util.h"

namespace arrow {
namespace json {
namespace internal {

class ObjectWriter::Impl {
 public:
  using Value = std::variant<std::string, bool>;

  void SetString(std::string_view key, std::string_view value) {
    members_.emplace_back(std::string(key), std::string(value));
  }

  void SetBool(std::string_view key, bool value) {
    members_.emplace_back(std::string(key), value);
  }

  std::string Serialize() {
    std::ostringstream out;
    out << '{';

    bool first = true;
    for (const auto& [key, value] : members_) {
      if (!first) {
        out << ',';
      }
      first = false;

      out << ::arrow::json::EscapeJsonString(key);
      out << ':';

      if (std::holds_alternative<std::string>(value)) {
        out << ::arrow::json::EscapeJsonString(std::get<std::string>(value));
      } else {
        out << (std::get<bool>(value) ? "true" : "false");
      }
    }

    out << '}';
    return out.str();
  }

 private:
  std::vector<std::pair<std::string, Value>> members_;
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
