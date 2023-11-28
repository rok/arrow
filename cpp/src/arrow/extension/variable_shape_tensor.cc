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

#include <sstream>

#include "arrow/extension/fixed_shape_tensor.h"
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
  auto ext_type = internal::checked_pointer_cast<VariableShapeTensorType>(this->type());
  auto value_type = ext_type->value_type();
  auto ndim = ext_type->ndim();
  auto dim_names = ext_type->dim_names();
  auto shapes =
      std::static_pointer_cast<FixedSizeListArray>(ext_arr->field(0))->value_slice(i);

  std::vector<int64_t> shape;
  for (int64_t j = 0; j < ndim; ++j) {
    ARROW_ASSIGN_OR_RAISE(auto size, shapes->GetScalar(j));
    shape.push_back(
        static_cast<int64_t>(std::static_pointer_cast<UInt32Scalar>(size)->value));
  }

  std::vector<int64_t> strides;
  // TODO: optimize ComputeStrides for non-uniform tensors
  ARROW_CHECK_OK(
      internal::ComputeStrides(value_type, shape, ext_type->permutation(), &strides));

  auto list_arr =
      std::static_pointer_cast<ListArray>(ext_arr->field(1))->value_slice(i)->data();
  auto byte_width = value_type->byte_width();
  auto buffer = SliceBuffer(list_arr->buffers[1], list_arr->offset * byte_width,
                            list_arr->length * byte_width);

  return Tensor::Make(value_type, buffer, shape, strides, dim_names);
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
  auto value_type = storage_type->field(1)->type()->field(0)->type();
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
    if (uniform_shape.size() > ndim) {
      return Status::Invalid("Invalid uniform_shape");
    }
  }

  return variable_shape_tensor(value_type, static_cast<uint32_t>(ndim), permutation,
                               dim_names, uniform_shape);
}

std::shared_ptr<Array> VariableShapeTensorType::MakeArray(
    std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ("arrow.variable_shape_tensor",
            static_cast<const ExtensionType&>(*data->type).extension_name());
  return std::make_shared<ExtensionArray>(data);
}

Result<std::shared_ptr<Tensor>> VariableShapeTensorType::GetTensor(
    const std::shared_ptr<ExtensionScalar>& scalar) const {
  const auto tensor_scalar = internal::checked_pointer_cast<StructScalar>(scalar->value);
  const auto fields = this->storage_type()->fields();

  ARROW_ASSIGN_OR_RAISE(const auto shape_scalar, tensor_scalar->field(0));
  ARROW_ASSIGN_OR_RAISE(const auto data, tensor_scalar->field(1));
  const auto shape_array =
      std::static_pointer_cast<FixedSizeListScalar>(shape_scalar)->value;

  std::vector<int64_t> shape;
  for (uint32_t j = 0; j < this->ndim(); ++j) {
    ARROW_ASSIGN_OR_RAISE(auto size, shape_array->GetScalar(j));
    shape.push_back(
        static_cast<int64_t>(std::static_pointer_cast<UInt32Scalar>(size)->value));
  }

  // TODO: optimize ComputeStrides for non-uniform tensors
  std::vector<int64_t> strides;
  ARROW_CHECK_OK(
      internal::ComputeStrides(this->value_type(), shape, this->permutation(), &strides));

  const auto array = std::static_pointer_cast<FixedSizeListScalar>(data)->value;
  const auto byte_width = this->value_type()->byte_width();
  const auto start_position = array->offset() * byte_width;
  const auto size = std::accumulate(shape.begin(), shape.end(), static_cast<int64_t>(1),
                                    std::multiplies<>());

  // Create a slice of the buffer
  std::shared_ptr<arrow::Buffer> buffer =
      arrow::SliceBuffer(array->data()->buffers[1], start_position, size * byte_width);

  return Tensor::Make(this->value_type(), buffer, shape, strides, this->dim_names());
}

Result<std::shared_ptr<DataType>> VariableShapeTensorType::Make(
    const std::shared_ptr<DataType>& value_type, const uint32_t& ndim,
    const std::vector<int64_t>& permutation, const std::vector<std::string>& dim_names,
    const std::vector<std::optional<int64_t>>& uniform_shape) {
  if (!permutation.empty() && permutation.size() != ndim) {
    return Status::Invalid("permutation size must match ndim. Expected: ", ndim,
                           " Got: ", permutation.size());
  }
  if (!dim_names.empty() && dim_names.size() != ndim) {
    return Status::Invalid("dim_names size must match ndim. Expected: ", ndim,
                           " Got: ", dim_names.size());
  }
  if (uniform_shape.size() > ndim) {
    return Status::Invalid("uniform_shape size must be less or equal ndim.");
  }
  return std::make_shared<VariableShapeTensorType>(value_type, ndim, permutation,
                                                   dim_names, uniform_shape);
}

std::shared_ptr<DataType> variable_shape_tensor(
    const std::shared_ptr<DataType>& value_type, const uint32_t& ndim,
    const std::vector<int64_t>& permutation, const std::vector<std::string>& dim_names,
    const std::vector<std::optional<int64_t>>& uniform_shape) {
  auto maybe_type = VariableShapeTensorType::Make(value_type, ndim, permutation,
                                                  dim_names, uniform_shape);
  ARROW_DCHECK_OK(maybe_type.status());
  return maybe_type.MoveValueUnsafe();
}

}  // namespace extension
}  // namespace arrow
