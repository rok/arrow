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

#include "arrow/integration/json_integration.h"

// Ensure simdjson is used in header-only mode
#ifndef SIMDJSON_HEADER_ONLY
#define SIMDJSON_HEADER_ONLY
#endif
#include <simdjson.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/integration/json_internal.h"
#include "arrow/io/file.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/logging_internal.h"

using arrow::ipc::DictionaryFieldMapper;
using arrow::ipc::DictionaryMemo;

namespace arrow::internal::integration {

// ----------------------------------------------------------------------
// Writer implementation

class IntegrationJsonWriter::Impl {
 public:
  explicit Impl(const std::shared_ptr<Schema>& schema)
      : schema_(schema), mapper_(*schema), first_batch_written_(false) {}

  Status Start() {
    writer_.StartObject();
    RETURN_NOT_OK(json::WriteSchema(*schema_, mapper_, &writer_));
    return Status::OK();
  }

  Status FirstRecordBatch(const RecordBatch& batch) {
    ARROW_ASSIGN_OR_RAISE(const auto dictionaries, CollectDictionaries(batch, mapper_));

    // Write dictionaries, if any
    if (!dictionaries.empty()) {
      writer_.Key("dictionaries");
      writer_.StartArray();
      for (const auto& entry : dictionaries) {
        RETURN_NOT_OK(json::WriteDictionary(entry.first, entry.second, &writer_));
      }
      writer_.EndArray();
    }

    // Record batches
    writer_.Key("batches");
    writer_.StartArray();
    first_batch_written_ = true;
    return Status::OK();
  }

  Result<std::string> Finish() {
    writer_.EndArray();  // Record batches
    writer_.EndObject();

    return std::string(writer_.GetString());
  }

  Status WriteRecordBatch(const RecordBatch& batch) {
    DCHECK_EQ(batch.num_columns(), schema_->num_fields());

    if (!first_batch_written_) {
      RETURN_NOT_OK(FirstRecordBatch(batch));
    }
    return json::WriteRecordBatch(batch, &writer_);
  }

 private:
  std::shared_ptr<Schema> schema_;
  DictionaryFieldMapper mapper_;

  bool first_batch_written_;

  json::JsonWriter writer_;
};

IntegrationJsonWriter::IntegrationJsonWriter(const std::shared_ptr<Schema>& schema) {
  impl_.reset(new Impl(schema));
}

IntegrationJsonWriter::~IntegrationJsonWriter() {}

Result<std::unique_ptr<IntegrationJsonWriter>> IntegrationJsonWriter::Open(
    const std::shared_ptr<Schema>& schema) {
  auto writer = std::unique_ptr<IntegrationJsonWriter>(new IntegrationJsonWriter(schema));
  RETURN_NOT_OK(writer->impl_->Start());
  return writer;
}

Result<std::string> IntegrationJsonWriter::Finish() { return impl_->Finish(); }

Status IntegrationJsonWriter::WriteRecordBatch(const RecordBatch& batch) {
  return impl_->WriteRecordBatch(batch);
}

// ----------------------------------------------------------------------
// Reader implementation

class IntegrationJsonReader::Impl {
 public:
  Impl(MemoryPool* pool, const std::shared_ptr<Buffer>& data)
      : pool_(pool), data_(data) {}

  Status ParseAndReadSchema() {
    // Create padded string for simdjson
    simdjson::padded_string padded(reinterpret_cast<const char*>(data_->data()),
                                   static_cast<size_t>(data_->size()));
    auto result = parser_.parse(padded);
    if (result.error()) {
      return Status::IOError("JSON parsing failed: ",
                             simdjson::error_message(result.error()));
    }
    doc_ = result.value();

    ARROW_ASSIGN_OR_RAISE(schema_, json::ReadSchema(doc_, pool_, &dictionary_memo_));

    auto batches_result = doc_["batches"];
    if (batches_result.error()) {
      return Status::Invalid("'batches' field not found");
    }
    auto arr_result = batches_result.get_array();
    if (arr_result.error()) {
      return Status::Invalid("'batches' was not an array");
    }
    record_batches_ = arr_result.value();

    // Count batches
    num_batches_ = 0;
    for (auto _ : record_batches_) {
      (void)_;
      ++num_batches_;
    }

    return Status::OK();
  }

  Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(int i) {
    if (i < 0 || i >= num_batches_) {
      return Status::IndexError("record batch index ", i, " out of bounds");
    }

    // Iterate to the i-th batch
    int idx = 0;
    for (auto batch : record_batches_) {
      if (idx == i) {
        return json::ReadRecordBatch(batch, schema_, &dictionary_memo_, pool_);
      }
      ++idx;
    }

    return Status::IndexError("record batch index ", i, " not found");
  }

  std::shared_ptr<Schema> schema() const { return schema_; }

  int num_record_batches() const { return num_batches_; }

 private:
  MemoryPool* pool_;
  std::shared_ptr<Buffer> data_;
  simdjson::dom::parser parser_;
  simdjson::dom::element doc_;

  simdjson::dom::array record_batches_;
  int num_batches_{0};
  std::shared_ptr<Schema> schema_;
  DictionaryMemo dictionary_memo_;
};

IntegrationJsonReader::IntegrationJsonReader(MemoryPool* pool,
                                             std::shared_ptr<Buffer> data) {
  impl_.reset(new Impl(pool, std::move(data)));
}

IntegrationJsonReader::~IntegrationJsonReader() {}

Result<std::unique_ptr<IntegrationJsonReader>> IntegrationJsonReader::Open(
    MemoryPool* pool, std::shared_ptr<Buffer> data) {
  auto reader = std::unique_ptr<IntegrationJsonReader>(
      new IntegrationJsonReader(pool, std::move(data)));
  RETURN_NOT_OK(reader->impl_->ParseAndReadSchema());
  return reader;
}

Result<std::unique_ptr<IntegrationJsonReader>> IntegrationJsonReader::Open(
    std::shared_ptr<Buffer> data) {
  return Open(default_memory_pool(), std::move(data));
}

Result<std::unique_ptr<IntegrationJsonReader>> IntegrationJsonReader::Open(
    MemoryPool* pool, const std::shared_ptr<io::ReadableFile>& in_file) {
  ARROW_ASSIGN_OR_RAISE(int64_t file_size, in_file->GetSize());
  ARROW_ASSIGN_OR_RAISE(auto json_buffer, in_file->Read(file_size));
  return Open(pool, std::move(json_buffer));
}

std::shared_ptr<Schema> IntegrationJsonReader::schema() const { return impl_->schema(); }

int IntegrationJsonReader::num_record_batches() const {
  return impl_->num_record_batches();
}

Result<std::shared_ptr<RecordBatch>> IntegrationJsonReader::ReadRecordBatch(int i) const {
  return impl_->ReadRecordBatch(i);
}

}  // namespace arrow::internal::integration
