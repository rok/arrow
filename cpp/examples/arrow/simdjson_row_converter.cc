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

#include <arrow/api.h>
#include <arrow/json/json_util.h>
#include <arrow/result.h>
#include <arrow/table_builder.h>
#include <arrow/type_traits.h>
#include <arrow/util/iterator.h>
#include <arrow/util/logging.h>
#include <arrow/visit_array_inline.h>

#include <simdjson.h>

#include <cassert>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

// Transforming dynamic row data into Arrow data
// When building connectors to other data systems, it's common to receive data in
// row-based structures. While the row_wise_conversion_example.cc shows how to
// handle this conversion for fixed schemas, this example demonstrates how to
// writer converters for arbitrary schemas.
//
// As an example, this conversion is between Arrow and simdjson::dom::elements.
//
// We use the following helpers and patterns here:
//  * arrow::VisitArrayInline and arrow::VisitTypeInline for implementing a visitor
//    pattern with Arrow to handle different array types
//  * arrow::enable_if_primitive_ctype to create a template method that handles
//    conversion for Arrow types that have corresponding C types (bool, integer,
//    float).

using Row = simdjson::dom::object;

namespace {

arrow::Status AppendScalarValue(const std::shared_ptr<arrow::Scalar>& scalar,
                                arrow::json::JsonWriter* writer) {
  if (!scalar || !scalar->is_valid) {
    writer->Null();
    return arrow::Status::OK();
  }

  switch (scalar->type->id()) {
    case arrow::Type::BOOL: {
      auto value = std::static_pointer_cast<arrow::BooleanScalar>(scalar);
      writer->Bool(value->value);
      return arrow::Status::OK();
    }
    case arrow::Type::INT8:
    case arrow::Type::INT16:
    case arrow::Type::INT32:
    case arrow::Type::INT64: {
      ARROW_ASSIGN_OR_RAISE(auto casted, scalar->CastTo(arrow::int64()));
      auto value = std::static_pointer_cast<arrow::Int64Scalar>(casted);
      writer->Int64(value->value);
      return arrow::Status::OK();
    }
    case arrow::Type::UINT8:
    case arrow::Type::UINT16:
    case arrow::Type::UINT32:
    case arrow::Type::UINT64: {
      ARROW_ASSIGN_OR_RAISE(auto casted, scalar->CastTo(arrow::uint64()));
      auto value = std::static_pointer_cast<arrow::UInt64Scalar>(casted);
      writer->Uint64(value->value);
      return arrow::Status::OK();
    }
    case arrow::Type::FLOAT:
    case arrow::Type::DOUBLE: {
      ARROW_ASSIGN_OR_RAISE(auto casted, scalar->CastTo(arrow::float64()));
      auto value = std::static_pointer_cast<arrow::DoubleScalar>(casted);
      writer->Double(value->value);
      return arrow::Status::OK();
    }
    case arrow::Type::STRING:
    case arrow::Type::BINARY:
    case arrow::Type::LARGE_STRING:
    case arrow::Type::LARGE_BINARY:
    case arrow::Type::STRING_VIEW:
    case arrow::Type::BINARY_VIEW: {
      auto value = std::static_pointer_cast<arrow::BaseBinaryScalar>(scalar);
      writer->String(value->value->ToString());
      return arrow::Status::OK();
    }
    case arrow::Type::STRUCT: {
      auto value = std::static_pointer_cast<arrow::StructScalar>(scalar);
      writer->StartObject();
      for (int i = 0; i < value->type->num_fields(); ++i) {
        writer->Key(value->type->field(i)->name());
        ARROW_RETURN_NOT_OK(AppendScalarValue(value->value[i], writer));
      }
      writer->EndObject();
      return arrow::Status::OK();
    }
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST: {
      auto value = std::static_pointer_cast<arrow::BaseListScalar>(scalar);
      writer->StartArray();
      if (value->value) {
        for (int64_t i = 0; i < value->value->length(); ++i) {
          ARROW_ASSIGN_OR_RAISE(auto item, value->value->GetScalar(i));
          ARROW_RETURN_NOT_OK(AppendScalarValue(item, writer));
        }
      }
      writer->EndArray();
      return arrow::Status::OK();
    }
    default:
      return arrow::Status::NotImplemented(
          "Arrow to JSON conversion not implemented for type ", scalar->type->ToString());
  }
}

arrow::Status AppendElementValue(const simdjson::dom::element& element,
                                 const std::shared_ptr<arrow::DataType>& type,
                                 arrow::ArrayBuilder* builder);

class ScalarConverter {
 public:
  ScalarConverter(simdjson::dom::element element, arrow::ArrayBuilder* builder)
      : element_(element), builder_(builder) {}

  arrow::Status Convert(const std::shared_ptr<arrow::DataType>& type) {
    return AppendElementValue(element_, type, builder_);
  }

 private:
  simdjson::dom::element element_;
  arrow::ArrayBuilder* builder_;
};

arrow::Status AppendElementValue(const simdjson::dom::element& element,
                                 const std::shared_ptr<arrow::DataType>& type,
                                 arrow::ArrayBuilder* builder) {
  if (element.is_null()) {
    return builder->AppendNull();
  }

  switch (type->id()) {
    case arrow::Type::BOOL: {
      auto* b = static_cast<arrow::BooleanBuilder*>(builder);
      return b->Append(element.get_bool().value());
    }
    case arrow::Type::INT64: {
      auto* b = static_cast<arrow::Int64Builder*>(builder);
      return b->Append(element.get_int64().value());
    }
    case arrow::Type::UINT64: {
      auto* b = static_cast<arrow::UInt64Builder*>(builder);
      return b->Append(element.get_uint64().value());
    }
    case arrow::Type::DOUBLE: {
      auto* b = static_cast<arrow::DoubleBuilder*>(builder);
      return b->Append(element.get_double().value());
    }
    case arrow::Type::STRING: {
      auto* b = static_cast<arrow::StringBuilder*>(builder);
      return b->Append(element.get_string().value());
    }
    case arrow::Type::STRUCT: {
      auto* b = static_cast<arrow::StructBuilder*>(builder);
      auto obj = element.get_object().value();
      ARROW_RETURN_NOT_OK(b->Append());
      for (int i = 0; i < b->num_fields(); ++i) {
        auto field = type->field(i);
        auto child = obj[field->name()];
        if (child.error() || child.is_null()) {
          ARROW_RETURN_NOT_OK(b->field_builder(i)->AppendNull());
        } else {
          ARROW_RETURN_NOT_OK(
              AppendElementValue(child.value(), field->type(), b->field_builder(i)));
        }
      }
      return arrow::Status::OK();
    }
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST: {
      auto* b = static_cast<arrow::BaseListBuilder*>(builder);
      auto arr = element.get_array().value();
      ARROW_RETURN_NOT_OK(b->Append());
      for (auto item : arr) {
        ARROW_RETURN_NOT_OK(
            AppendElementValue(item.value(), type->field(0)->type(), b->value_builder()));
      }
      return arrow::Status::OK();
    }
    default:
      return arrow::Status::NotImplemented("JSON conversion not implemented for ",
                                           type->ToString());
  }
}

}  // namespace

/// \brief Builder that holds state for a single conversion.
///
/// Implements Visit() methods for each type of Arrow Array that set the values
/// of the corresponding fields in each row.
class RowBatchBuilder {
 public:
  explicit RowBatchBuilder(int64_t num_rows) : field_(nullptr) {
    writers_.resize(static_cast<size_t>(num_rows));
    for (auto& writer : writers_) {
      writer.StartObject();
    }
  }

  /// \brief Set which field to convert.
  void SetField(const arrow::Field* field) { field_ = field; }

  /// \brief Retrieve converted rows from builder.
  std::vector<std::string> Rows() && {
    std::vector<std::string> rows;
    rows.reserve(writers_.size());
    for (auto& writer : writers_) {
      writer.EndObject();
      rows.emplace_back(std::string(writer.GetString()));
    }
    return rows;
  }

  // Default implementation
  arrow::Status Visit(const arrow::Array& array) {
    return arrow::Status::NotImplemented("Cannot convert to JSON for array of type ",
                                         array.type()->ToString());
  }

  arrow::Status Visit(const arrow::BooleanArray& array) {
    assert(static_cast<int64_t>(writers_.size()) == array.length());
    for (int64_t i = 0; i < array.length(); ++i) {
      if (!array.IsNull(i)) {
        writers_[static_cast<size_t>(i)].Key(field_->name());
        writers_[static_cast<size_t>(i)].Bool(array.Value(i));
      }
    }
    return arrow::Status::OK();
  }

  // Handles integers and floats
  template <typename ArrayType, typename DataClass = typename ArrayType::TypeClass>
  arrow::enable_if_primitive_ctype<DataClass, arrow::Status> Visit(
      const ArrayType& array) {
    assert(static_cast<int64_t>(writers_.size()) == array.length());
    for (int64_t i = 0; i < array.length(); ++i) {
      if (!array.IsNull(i)) {
        writers_[static_cast<size_t>(i)].Key(field_->name());
        writers_[static_cast<size_t>(i)].Double(array.Value(i));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringArray& array) {
    assert(static_cast<int64_t>(writers_.size()) == array.length());
    for (int64_t i = 0; i < array.length(); ++i) {
      if (!array.IsNull(i)) {
        writers_[static_cast<size_t>(i)].Key(field_->name());
        writers_[static_cast<size_t>(i)].String(array.GetView(i));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StructArray& array) {
    assert(static_cast<int64_t>(writers_.size()) == array.length());
    for (int64_t i = 0; i < array.length(); ++i) {
      if (!array.IsNull(i)) {
        writers_[static_cast<size_t>(i)].Key(field_->name());
        ARROW_ASSIGN_OR_RAISE(auto scalar, array.GetScalar(i));
        ARROW_RETURN_NOT_OK(AppendScalarValue(scalar, &writers_[static_cast<size_t>(i)]));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::ListArray& array) {
    assert(static_cast<int64_t>(writers_.size()) == array.length());
    for (int64_t i = 0; i < array.length(); ++i) {
      if (!array.IsNull(i)) {
        writers_[static_cast<size_t>(i)].Key(field_->name());
        ARROW_ASSIGN_OR_RAISE(auto scalar, array.GetScalar(i));
        ARROW_RETURN_NOT_OK(AppendScalarValue(scalar, &writers_[static_cast<size_t>(i)]));
      }
    }
    return arrow::Status::OK();
  }

 private:
  const arrow::Field* field_;
  std::vector<arrow::json::JsonWriter> writers_;
};

/// \brief Holds state to convert simdjson rows to Arrow arrays.
class JsonValueConverter {
 public:
  JsonValueConverter(const std::vector<Row>& rows, const arrow::Field* field,
                     arrow::ArrayBuilder* builder)
      : rows_(rows), field_(field), builder_(builder) {}

  arrow::Status Convert() { return arrow::VisitTypeInline(*field_->type(), this); }

  arrow::Status Visit(const arrow::BooleanType&) {
    auto* b = static_cast<arrow::BooleanBuilder*>(builder_);
    for (const auto& row : rows_) {
      auto value = row[field_->name()];
      if (value.error() || value.is_null()) {
        ARROW_RETURN_NOT_OK(b->AppendNull());
      } else {
        ARROW_RETURN_NOT_OK(b->Append(value.get_bool().value()));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int64Type&) {
    auto* b = static_cast<arrow::Int64Builder*>(builder_);
    for (const auto& row : rows_) {
      auto value = row[field_->name()];
      if (value.error() || value.is_null()) {
        ARROW_RETURN_NOT_OK(b->AppendNull());
      } else {
        ARROW_RETURN_NOT_OK(b->Append(value.get_int64().value()));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::UInt64Type&) {
    auto* b = static_cast<arrow::UInt64Builder*>(builder_);
    for (const auto& row : rows_) {
      auto value = row[field_->name()];
      if (value.error() || value.is_null()) {
        ARROW_RETURN_NOT_OK(b->AppendNull());
      } else {
        ARROW_RETURN_NOT_OK(b->Append(value.get_uint64().value()));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleType&) {
    auto* b = static_cast<arrow::DoubleBuilder*>(builder_);
    for (const auto& row : rows_) {
      auto value = row[field_->name()];
      if (value.error() || value.is_null()) {
        ARROW_RETURN_NOT_OK(b->AppendNull());
      } else {
        ARROW_RETURN_NOT_OK(b->Append(value.get_double().value()));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringType&) {
    auto* b = static_cast<arrow::StringBuilder*>(builder_);
    for (const auto& row : rows_) {
      auto value = row[field_->name()];
      if (value.error() || value.is_null()) {
        ARROW_RETURN_NOT_OK(b->AppendNull());
      } else {
        ARROW_RETURN_NOT_OK(b->Append(value.get_string().value()));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StructType& type) {
    auto* b = static_cast<arrow::StructBuilder*>(builder_);
    for (const auto& row : rows_) {
      auto value = row[field_->name()];
      if (value.error() || value.is_null()) {
        ARROW_RETURN_NOT_OK(b->AppendNull());
        continue;
      }
      ARROW_RETURN_NOT_OK(AppendElementValue(value.value(), field_->type(), b));
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::ListType&) {
    auto* b = static_cast<arrow::ListBuilder*>(builder_);
    for (const auto& row : rows_) {
      auto value = row[field_->name()];
      if (value.error() || value.is_null()) {
        ARROW_RETURN_NOT_OK(b->AppendNull());
        continue;
      }
      ARROW_RETURN_NOT_OK(AppendElementValue(value.value(), field_->type(), b));
    }
    return arrow::Status::OK();
  }

 private:
  const std::vector<Row>& rows_;
  const arrow::Field* field_;
  arrow::ArrayBuilder* builder_;
};

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ConvertToRecordBatch(
    const std::vector<Row>& rows, const std::shared_ptr<arrow::Schema>& schema) {
  auto batch_builder =
      arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool());
  ARROW_RETURN_NOT_OK(batch_builder.status());
  std::unique_ptr<arrow::RecordBatchBuilder> builder =
      std::move(batch_builder).ValueOrDie();

  for (int i = 0; i < schema->num_fields(); ++i) {
    const arrow::Field* field = schema->field(i).get();
    std::shared_ptr<arrow::ArrayBuilder> array_builder = builder->GetField(i);
    JsonValueConverter converter(rows, field, array_builder.get());
    ARROW_RETURN_NOT_OK(converter.Convert());
  }

  std::shared_ptr<arrow::RecordBatch> batch;
  ARROW_RETURN_NOT_OK(builder->Flush(&batch));
  ARROW_RETURN_NOT_OK(batch->ValidateFull());
  return batch;
}  // ConvertToRecordBatch

/// \brief Converter from Arrow to JSON rows (strings).
class ArrowToDocumentConverter {
 public:
  explicit ArrowToDocumentConverter(const std::shared_ptr<arrow::Schema>& schema)
      : schema_(schema) {}

  arrow::Result<std::vector<std::string>> ConvertToVector(
      const std::shared_ptr<arrow::RecordBatch>& batch) {
    RowBatchBuilder builder(batch->num_rows());
    for (int i = 0; i < batch->num_columns(); ++i) {
      builder.SetField(batch->schema()->field(i).get());
      ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*batch->column(i), &builder));
    }
    return std::move(builder).Rows();
  }

  arrow::Result<std::vector<std::string>> ConvertToVector(
      const std::shared_ptr<arrow::Table>& table) {
    arrow::TableBatchReader reader(*table);
    std::vector<std::string> out;
    for (auto maybe_batch : reader) {
      ARROW_ASSIGN_OR_RAISE(auto batch, maybe_batch);
      ARROW_ASSIGN_OR_RAISE(auto rows, ConvertToVector(batch));
      out.insert(out.end(), rows.begin(), rows.end());
    }
    return out;
  }

 private:
  std::shared_ptr<arrow::Schema> schema_;
};  // ArrowToDocumentConverter
