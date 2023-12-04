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

#include <set>
#include <sstream>

#include "arrow/extension/tensor_internal.h"
#include "arrow/extension/variable_shape_tensor.h"

#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/json/rapidjson_defs.h"  // IWYU pragma: keep
#include "arrow/scalar.h"
#include "arrow/tensor.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/logging.h"
#include "arrow/util/sort.h"

#include <rapidjson/document.h>
#include <rapidjson/writer.h>

namespace rj = arrow::rapidjson;

namespace arrow {
namespace extension {

const Result<std::shared_ptr<Tensor>> VariableShapeTensorArray::GetTensor(
    const int64_t i) const {
  auto ext_arr = internal::checked_pointer_cast<StructArray>(this->storage());
  const auto ext_type =
      internal::checked_pointer_cast<VariableShapeTensorType>(this->type());
  ARROW_ASSIGN_OR_RAISE(const auto tensor_scalar, this->GetScalar(i));
  return ext_type->GetTensor(
      internal::checked_pointer_cast<ExtensionScalar>(tensor_scalar));
}

bool VariableShapeTensorType::ExtensionEquals(const ExtensionType& other) const {
  if (extension_name() != other.extension_name()) {
    return false;
  }
  const auto& other_ext = static_cast<const VariableShapeTensorType&>(other);
  if (this->ndim() != other_ext.ndim()) {
    return false;
  }

  auto is_permutation_trivial = [](const std::vector<int64_t>& permutation) {
    for (size_t i = 1; i < permutation.size(); ++i) {
      if (permutation[i - 1] + 1 != permutation[i]) {
        return false;
      }
    }
    return true;
  };
  const bool permutation_equivalent =
      ((permutation_ == other_ext.permutation()) ||
       (permutation_.empty() && is_permutation_trivial(other_ext.permutation())) ||
       (is_permutation_trivial(permutation_) && other_ext.permutation().empty()));

  return (storage_type()->Equals(other_ext.storage_type())) &&
         (dim_names_ == other_ext.dim_names()) &&
         (uniform_shape_ == other_ext.uniform_shape()) && permutation_equivalent;
}

std::string VariableShapeTensorType::Serialize() const {
  rj::Document document;
  document.SetObject();
  rj::Document::AllocatorType& allocator = document.GetAllocator();

  if (!permutation_.empty()) {
    rj::Value permutation(rj::kArrayType);
    for (auto v : permutation_) {
      permutation.PushBack(v, allocator);
    }
    document.AddMember(rj::Value("permutation", allocator), permutation, allocator);
  }

  if (!dim_names_.empty()) {
    rj::Value dim_names(rj::kArrayType);
    for (std::string v : dim_names_) {
      dim_names.PushBack(rj::Value{}.SetString(v.c_str(), allocator), allocator);
    }
    document.AddMember(rj::Value("dim_names", allocator), dim_names, allocator);
  }

  if (!uniform_shape_.empty()) {
    rj::Value uniform_shape(rj::kArrayType);
    for (auto v : uniform_shape_) {
      if (v.has_value()) {
        uniform_shape.PushBack(v.value(), allocator);
      } else {
        uniform_shape.PushBack(rj::Value{}.SetNull(), allocator);
      }
    }
    document.AddMember(rj::Value("uniform_shape", allocator), uniform_shape, allocator);
  }

  rj::StringBuffer buffer;
  rj::Writer<rj::StringBuffer> writer(buffer);
  document.Accept(writer);
  return buffer.GetString();
}

Result<std::shared_ptr<DataType>> VariableShapeTensorType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized_data) const {
  if (storage_type->id() != Type::STRUCT) {
    return Status::Invalid("Expected Struct storage type, got ",
                           storage_type->ToString());
  }

  if (storage_type->num_fields() != 2) {
    return Status::Invalid("Expected Struct storage type with 2 fields, got ",
                           storage_type->num_fields());
  }
  if (storage_type->field(0)->type()->id() != Type::FIXED_SIZE_LIST) {
    return Status::Invalid("Expected FixedSizeList storage type, got ",
                           storage_type->field(0)->type()->ToString());
  }
  if (storage_type->field(1)->type()->id() != Type::LIST) {
    return Status::Invalid("Expected List storage type, got ",
                           storage_type->field(1)->type()->ToString());
  }
  if (std::static_pointer_cast<FixedSizeListType>(storage_type->field(0)->type())
          ->value_type() != int32()) {
    return Status::Invalid("Expected FixedSizeList value type int32, got ",
                           storage_type->field(0)->type()->ToString());
  }

  const auto value_type = storage_type->field(1)->type()->field(0)->type();
  const size_t ndim =
      std::static_pointer_cast<FixedSizeListType>(storage_type->field(0)->type())
          ->list_size();

  rj::Document document;
  if (document.Parse(serialized_data.data(), serialized_data.length()).HasParseError()) {
    return Status::Invalid("Invalid serialized JSON data: ", serialized_data);
  }

  std::vector<int64_t> permutation;
  if (document.HasMember("permutation")) {
    for (auto& x : document["permutation"].GetArray()) {
      permutation.emplace_back(x.GetInt64());
    }
    if (permutation.size() != ndim) {
      return Status::Invalid("Invalid permutation");
    }
  }
  std::vector<std::string> dim_names;
  if (document.HasMember("dim_names")) {
    for (auto& x : document["dim_names"].GetArray()) {
      dim_names.emplace_back(x.GetString());
    }
    if (dim_names.size() != ndim) {
      return Status::Invalid("Invalid dim_names");
    }
  }

  std::vector<std::optional<int64_t>> uniform_shape;
  if (document.HasMember("uniform_shape")) {
    for (auto& x : document["uniform_shape"].GetArray()) {
      if (x.IsNull()) {
        uniform_shape.emplace_back(std::nullopt);
      } else {
        uniform_shape.emplace_back(x.GetInt64());
      }
    }
    if (uniform_shape.size() != ndim) {
      return Status::Invalid("uniform_shape size must match ndim. Expected: ", ndim,
                             " Got: ", uniform_shape.size());
    }
  }

  return variable_shape_tensor(value_type, static_cast<int32_t>(ndim), permutation,
                               dim_names, uniform_shape);
}

std::shared_ptr<Array> VariableShapeTensorType::MakeArray(
    std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ("arrow.variable_shape_tensor",
            internal::checked_cast<const ExtensionType&>(*data->type).extension_name());
  return std::make_shared<ExtensionArray>(data);
}

Result<std::shared_ptr<Tensor>> VariableShapeTensorType::GetTensor(
    const std::shared_ptr<ExtensionScalar>& scalar) const {
  const auto& tensor_scalar = internal::checked_cast<const StructScalar&>(*scalar->value);

  ARROW_ASSIGN_OR_RAISE(const auto shape_scalar, tensor_scalar.field(0));
  ARROW_ASSIGN_OR_RAISE(const auto data, tensor_scalar.field(1));
  const auto& shape_array = internal::checked_cast<const Int32Array&>(
      *internal::checked_cast<const FixedSizeListScalar&>(*shape_scalar).value);

  auto permutation = this->permutation();
  if (permutation.empty()) {
    for (int64_t j = 0; j < static_cast<int64_t>(this->ndim()); ++j) {
      permutation.emplace_back(j);
    }
  }

  std::vector<int64_t> shape;
  std::vector<int64_t> permuted_shape;
  for (int64_t j = 0; j < static_cast<int64_t>(this->ndim()); ++j) {
    ARROW_ASSIGN_OR_RAISE(const auto size, shape_array.GetScalar(j));
    const auto size_value = internal::checked_pointer_cast<Int32Scalar>(size)->value;
    if (size_value < 0) {
      return Status::Invalid("shape must have non-negative values");
    }
    ARROW_ASSIGN_OR_RAISE(const auto permuted_size,
                          shape_array.GetScalar(permutation[j]));
    const auto permuted_size_value =
        internal::checked_pointer_cast<Int32Scalar>(permuted_size)->value;
    shape.push_back(size_value);
    permuted_shape.push_back(permuted_size_value);
  }

  std::vector<std::string> dim_names;
  if (!this->dim_names().empty()) {
    for (auto j : permutation) {
      dim_names.emplace_back(this->dim_names()[j]);
    }
  } else {
    dim_names = {};
  }

  std::vector<int64_t> strides;
  ARROW_CHECK_OK(internal::ComputeStrides(this->value_type(), permuted_shape, permutation,
                                          &strides));

  const auto& array = internal::checked_cast<const BaseListScalar&>(*data).value;
  const auto byte_width = this->value_type()->byte_width();
  const auto start_position = array->offset() * byte_width;
  const auto size = std::accumulate(shape.begin(), shape.end(), static_cast<int64_t>(1),
                                    std::multiplies<>());

  // Create a slice of the buffer
  const std::shared_ptr<arrow::Buffer> buffer =
      SliceBuffer(array->data()->buffers[1], start_position, size * byte_width);

  return Tensor::Make(this->value_type(), buffer, shape, strides, this->dim_names());
}

Result<std::shared_ptr<DataType>> VariableShapeTensorType::Make(
    const std::shared_ptr<DataType>& value_type, const int32_t ndim,
    const std::vector<int64_t> permutation, const std::vector<std::string> dim_names,
    const std::vector<std::optional<int64_t>> uniform_shape) {
  if (!permutation.empty()) {
    if (permutation.size() != static_cast<size_t>(ndim)) {
      return Status::Invalid("permutation size must match ndim. Expected: ", ndim,
                             " Got: ", permutation.size());
    }
    const std::set<int64_t> permutation_set(permutation.begin(), permutation.end());
    if (permutation_set.size() != permutation.size()) {
      return Status::Invalid("permutation must be a valid permutation vector");
    }
    for (auto p : permutation) {
      if (p < 0 || ndim <= p) {
        return Status::Invalid("permutation must be a valid permutation vector");
      }
    }
  }

  if (!dim_names.empty() && dim_names.size() != static_cast<size_t>(ndim)) {
    return Status::Invalid("dim_names size must match ndim. Expected: ", ndim,
                           " Got: ", dim_names.size());
  }
  if (!uniform_shape.empty() && uniform_shape.size() != static_cast<size_t>(ndim)) {
    return Status::Invalid("uniform_shape size must match ndim. Expected: ", ndim,
                           " Got: ", uniform_shape.size());
  }
  if (!uniform_shape.empty()) {
    for (const auto& v : uniform_shape) {
      if (v.has_value() && v.value() < 0) {
        return Status::Invalid("uniform_shape must have non-negative values");
      }
    }
  }
  return std::make_shared<VariableShapeTensorType>(
      value_type, std::move(ndim), std::move(permutation), std::move(dim_names),
      std::move(uniform_shape));
}

std::shared_ptr<DataType> variable_shape_tensor(
    const std::shared_ptr<DataType>& value_type, const int32_t ndim,
    const std::vector<int64_t> permutation, const std::vector<std::string> dim_names,
    const std::vector<std::optional<int64_t>> uniform_shape) {
  auto maybe_type =
      VariableShapeTensorType::Make(value_type, std::move(ndim), std::move(permutation),
                                    std::move(dim_names), std::move(uniform_shape));
  ARROW_CHECK_OK(maybe_type.status());
  return maybe_type.MoveValueUnsafe();
}

}  // namespace extension
}  // namespace arrow
