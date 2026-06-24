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

#include "benchmark/benchmark.h"

#include <array>
#include <iostream>
#include <random>
#include <type_traits>

#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/platform.h"
#include "parquet/properties.h"

#include "arrow/array.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/data.h"
#include "arrow/buffer.h"
#include "arrow/compute/cast.h"
#include "arrow/io/memory.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/logging.h"

using arrow::Array;
using arrow::ArrayVector;
using arrow::BooleanBuilder;
using arrow::FieldVector;
using arrow::NumericBuilder;
using arrow::Table;

#define EXIT_NOT_OK(s)                                        \
  do {                                                        \
    ::arrow::Status _s = (s);                                 \
    if (ARROW_PREDICT_FALSE(!_s.ok())) {                      \
      std::cout << "Exiting: " << _s.ToString() << std::endl; \
      exit(EXIT_FAILURE);                                     \
    }                                                         \
  } while (0)

namespace parquet {

using arrow::FileReader;
using arrow::WriteTable;
using schema::PrimitiveNode;

namespace benchmarks {

// This should result in multiple pages for most primitive types
constexpr int64_t BENCHMARK_SIZE = 10 * 1024 * 1024;

template <typename ParquetType>
struct benchmark_traits {};

template <>
struct benchmark_traits<Int32Type> {
  using arrow_type = ::arrow::Int32Type;
};

template <>
struct benchmark_traits<Int64Type> {
  using arrow_type = ::arrow::Int64Type;
};

template <>
struct benchmark_traits<DoubleType> {
  using arrow_type = ::arrow::DoubleType;
};

template <>
struct benchmark_traits<BooleanType> {
  using arrow_type = ::arrow::BooleanType;
};

template <>
struct benchmark_traits<Float16LogicalType> {
  using arrow_type = ::arrow::HalfFloatType;
};

template <typename ParquetType>
using ArrowType = typename benchmark_traits<ParquetType>::arrow_type;

template <typename ParquetType>
std::shared_ptr<ColumnDescriptor> MakeSchema(Repetition::type repetition) {
  auto node = PrimitiveNode::Make("int64", repetition, ParquetType::type_num);
  return std::make_shared<ColumnDescriptor>(node, repetition != Repetition::REQUIRED,
                                            repetition == Repetition::REPEATED);
}

template <typename ParquetType>
int64_t BytesForItems(int64_t num_items) {
  static_assert(!std::is_same_v<ParquetType, FLBAType>,
                "BytesForItems unsupported for FLBAType");
  return num_items * sizeof(typename ParquetType::c_type);
}

template <>
int64_t BytesForItems<BooleanType>(int64_t num_items) {
  return ::arrow::bit_util::BytesForBits(num_items);
}

template <>
int64_t BytesForItems<Float16LogicalType>(int64_t num_items) {
  return num_items * sizeof(uint16_t);
}

template <typename ParquetType>
void SetBytesProcessed(::benchmark::State& state, int64_t num_values = BENCHMARK_SIZE) {
  const int64_t items_processed = state.iterations() * num_values;
  state.SetItemsProcessed(items_processed);
  state.SetBytesProcessed(BytesForItems<ParquetType>(items_processed));
}

constexpr int64_t kAlternatingOrNa = -1;

template <typename T>
std::vector<T> RandomVector(int64_t true_percentage, int64_t vector_size,
                            const std::array<T, 2>& sample_values, int seed = 500) {
  std::vector<T> values(vector_size, {});
  if (true_percentage == kAlternatingOrNa) {
    int n = {0};
    std::generate(values.begin(), values.end(), [&n] { return n++ % 2; });
  } else {
    std::default_random_engine rng(seed);
    double true_probability = static_cast<double>(true_percentage) / 100.0;
    std::bernoulli_distribution dist(true_probability);
    std::generate(values.begin(), values.end(), [&] { return sample_values[dist(rng)]; });
  }
  return values;
}

template <typename ParquetType, typename ArrowType = ArrowType<ParquetType>>
std::shared_ptr<Table> TableFromVector(const std::vector<typename ArrowType::c_type>& vec,
                                       bool nullable,
                                       int64_t null_percentage = kAlternatingOrNa) {
  if (!nullable) {
    ARROW_CHECK_EQ(null_percentage, kAlternatingOrNa);
  }
  std::shared_ptr<::arrow::DataType> type = std::make_shared<ArrowType>();
  NumericBuilder<ArrowType> builder;
  if (nullable) {
    // Note true values select index 1 of sample_values
    auto valid_bytes = RandomVector<uint8_t>(/*true_percentage=*/null_percentage,
                                             vec.size(), /*sample_values=*/{1, 0});
    EXIT_NOT_OK(builder.AppendValues(vec.data(), vec.size(), valid_bytes.data()));
  } else {
    EXIT_NOT_OK(builder.AppendValues(vec.data(), vec.size(), nullptr));
  }
  std::shared_ptr<::arrow::Array> array;
  EXIT_NOT_OK(builder.Finish(&array));

  auto field = ::arrow::field("column", type, nullable);
  auto schema = ::arrow::schema({field});
  return Table::Make(schema, {array});
}

template <>
std::shared_ptr<Table> TableFromVector<BooleanType, ::arrow::BooleanType>(
    const std::vector<bool>& vec, bool nullable, int64_t null_percentage) {
  BooleanBuilder builder;
  if (nullable) {
    auto valid_bytes = RandomVector<bool>(/*true_percentage=*/null_percentage, vec.size(),
                                          {true, false});
    EXIT_NOT_OK(builder.AppendValues(vec, valid_bytes));
  } else {
    EXIT_NOT_OK(builder.AppendValues(vec));
  }
  std::shared_ptr<::arrow::Array> array;
  EXIT_NOT_OK(builder.Finish(&array));

  auto field = ::arrow::field("column", ::arrow::boolean(), nullable);
  auto schema = std::make_shared<::arrow::Schema>(
      std::vector<std::shared_ptr<::arrow::Field>>({field}));
  return Table::Make(schema, {array});
}

template <bool nullable, typename ParquetType>
static void BM_WriteColumn(::benchmark::State& state) {
  using T = typename ParquetType::c_type;
  std::vector<T> values(BENCHMARK_SIZE, static_cast<T>(128));
  std::shared_ptr<Table> table = TableFromVector<ParquetType>(values, nullable);

  while (state.KeepRunning()) {
    auto output = CreateOutputStream();
    EXIT_NOT_OK(
        WriteTable(*table, ::arrow::default_memory_pool(), output, BENCHMARK_SIZE));
  }
  SetBytesProcessed<ParquetType>(state);
}

BENCHMARK_TEMPLATE2(BM_WriteColumn, false, Int32Type);
BENCHMARK_TEMPLATE2(BM_WriteColumn, true, Int32Type);

BENCHMARK_TEMPLATE2(BM_WriteColumn, false, Int64Type);
BENCHMARK_TEMPLATE2(BM_WriteColumn, true, Int64Type);

BENCHMARK_TEMPLATE2(BM_WriteColumn, false, DoubleType);
BENCHMARK_TEMPLATE2(BM_WriteColumn, true, DoubleType);

BENCHMARK_TEMPLATE2(BM_WriteColumn, false, BooleanType);
BENCHMARK_TEMPLATE2(BM_WriteColumn, true, BooleanType);

int32_t kInfiniteUniqueValues = -1;

std::shared_ptr<Table> RandomStringTable(std::shared_ptr<::arrow::DataType> type,
                                         int64_t length, int64_t unique_values,
                                         int64_t null_percentage) {
  std::shared_ptr<::arrow::Array> arr;
  ::arrow::random::RandomArrayGenerator generator(/*seed=*/500);
  double null_probability = static_cast<double>(null_percentage) / 100.0;
  if (unique_values == kInfiniteUniqueValues) {
    arr = generator.String(length, /*min_length=*/3, /*max_length=*/32,
                           /*null_probability=*/null_probability);
  } else {
    arr = generator.StringWithRepeats(length, /*unique=*/unique_values,
                                      /*min_length=*/3, /*max_length=*/32,
                                      /*null_probability=*/null_probability);
  }
  arr = *::arrow::compute::Cast(*arr, type);
  return Table::Make(
      ::arrow::schema({::arrow::field("column", type, null_percentage > 0)}), {arr});
}

static void BM_WriteBinaryColumn(::benchmark::State& state) {
  std::shared_ptr<Table> table =
      RandomStringTable(::arrow::utf8(), BENCHMARK_SIZE, state.range(1), state.range(0));

  while (state.KeepRunning()) {
    auto output = CreateOutputStream();
    EXIT_NOT_OK(
        WriteTable(*table, ::arrow::default_memory_pool(), output, BENCHMARK_SIZE));
  }

  // Offsets + data
  int64_t total_bytes = table->column(0)->chunk(0)->data()->buffers[1]->size() +
                        table->column(0)->chunk(0)->data()->buffers[2]->size();
  state.SetItemsProcessed(BENCHMARK_SIZE * state.iterations());
  state.SetBytesProcessed(total_bytes * state.iterations());
}

BENCHMARK(BM_WriteBinaryColumn)
    ->ArgNames({"null_probability", "unique_values"})
    // We vary unique values to trigger the dictionary-encoded (for low-cardinality)
    // and plain (for high-cardinality) code paths.
    ->Args({0, 32})
    ->Args({0, kInfiniteUniqueValues})
    ->Args({1, 32})
    ->Args({50, 32})
    ->Args({99, 32})
    ->Args({1, kInfiniteUniqueValues})
    ->Args({50, kInfiniteUniqueValues})
    ->Args({99, kInfiniteUniqueValues});

template <typename T>
struct Examples {
  static constexpr std::array<T, 2> values() { return {127, 128}; }
};

template <>
struct Examples<bool> {
  static constexpr std::array<bool, 2> values() { return {false, true}; }
};

::arrow::Result<std::shared_ptr<Buffer>> WriteReadBenchmarkBuffer(
    const Table& table, const std::shared_ptr<WriterProperties>& properties,
    const std::shared_ptr<ArrowWriterProperties>& arrow_properties) {
  auto output = CreateOutputStream();
  RETURN_NOT_OK(WriteTable(table, ::arrow::default_memory_pool(), output,
                           /*chunk_size=*/table.num_rows(), properties,
                           arrow_properties));
  return output->Finish();
}

void SetParquetSizeCounters(::benchmark::State& state, int64_t parquet_size,
                            int64_t total_bytes) {
  state.counters["parquet_size_bytes"] = static_cast<double>(parquet_size);
  if (total_bytes > 0) {
    state.counters["parquet_to_raw_ratio"] =
        static_cast<double>(parquet_size) / static_cast<double>(total_bytes);
  }
}

static void BenchmarkWriteTable(::benchmark::State& state, const Table& table,
                                std::shared_ptr<WriterProperties> properties,
                                std::shared_ptr<ArrowWriterProperties> arrow_properties,
                                int64_t num_values = -1, int64_t total_bytes = -1) {
  PARQUET_ASSIGN_OR_THROW(auto sample_buffer,
                          WriteReadBenchmarkBuffer(table, properties, arrow_properties));

  for (auto _ : state) {
    auto output = CreateOutputStream();
    EXIT_NOT_OK(WriteTable(table, ::arrow::default_memory_pool(), output,
                           /*chunk_size=*/table.num_rows(), properties,
                           arrow_properties));
    auto finished = output->Finish();
    EXIT_NOT_OK(finished.status());
  }

  if (num_values == -1) {
    num_values = table.num_rows();
  }
  state.SetItemsProcessed(num_values * state.iterations());
  if (total_bytes != -1) {
    state.SetBytesProcessed(total_bytes * state.iterations());
    SetParquetSizeCounters(state, sample_buffer->size(), total_bytes);
  }
}

static void BenchmarkRoundtripTable(
    ::benchmark::State& state, const Table& table,
    std::shared_ptr<WriterProperties> properties,
    std::shared_ptr<ArrowWriterProperties> arrow_properties, int64_t num_values = -1,
    int64_t total_bytes = -1) {
  PARQUET_ASSIGN_OR_THROW(auto sample_buffer,
                          WriteReadBenchmarkBuffer(table, properties, arrow_properties));

  for (auto _ : state) {
    auto output = CreateOutputStream();
    EXIT_NOT_OK(WriteTable(table, ::arrow::default_memory_pool(), output,
                           /*chunk_size=*/table.num_rows(), properties,
                           arrow_properties));
    PARQUET_ASSIGN_OR_THROW(auto buffer, output->Finish());

    auto reader =
        ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
    auto arrow_reader_result =
        FileReader::Make(::arrow::default_memory_pool(), std::move(reader));
    EXIT_NOT_OK(arrow_reader_result.status());
    auto arrow_reader = std::move(*arrow_reader_result);

    auto table_result = arrow_reader->ReadTable();
    EXIT_NOT_OK(table_result.status());
  }

  if (num_values == -1) {
    num_values = table.num_rows();
  }
  state.SetItemsProcessed(num_values * state.iterations());
  if (total_bytes != -1) {
    state.SetBytesProcessed(total_bytes * state.iterations());
    SetParquetSizeCounters(state, sample_buffer->size(), total_bytes);
  }
}

static void BenchmarkReadTable(::benchmark::State& state, const Table& table,
                               std::shared_ptr<WriterProperties> properties,
                               std::shared_ptr<ArrowWriterProperties> arrow_properties,
                               int64_t num_values = -1, int64_t total_bytes = -1) {
  PARQUET_ASSIGN_OR_THROW(auto buffer,
                          WriteReadBenchmarkBuffer(table, properties, arrow_properties));

  for (auto _ : state) {
    auto reader =
        ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
    auto arrow_reader_result =
        FileReader::Make(::arrow::default_memory_pool(), std::move(reader));
    EXIT_NOT_OK(arrow_reader_result.status());
    auto arrow_reader = std::move(*arrow_reader_result);

    auto table_result = arrow_reader->ReadTable();
    EXIT_NOT_OK(table_result.status());
  }

  if (num_values == -1) {
    num_values = table.num_rows();
  }
  state.SetItemsProcessed(num_values * state.iterations());
  if (total_bytes != -1) {
    state.SetBytesProcessed(total_bytes * state.iterations());
    SetParquetSizeCounters(state, buffer->size(), total_bytes);
  }
}

static void BenchmarkReadTable(::benchmark::State& state, const Table& table,
                               std::shared_ptr<WriterProperties> properties,
                               int64_t num_values = -1, int64_t total_bytes = -1) {
  auto arrow_properties = ArrowWriterProperties::Builder().store_schema()->build();
  BenchmarkReadTable(state, table, std::move(properties), std::move(arrow_properties),
                     num_values, total_bytes);
}

static void BenchmarkReadTable(::benchmark::State& state, const Table& table,
                               int64_t num_values = -1, int64_t total_bytes = -1) {
  BenchmarkReadTable(state, table, default_writer_properties(), num_values, total_bytes);
}

static void BenchmarkReadArray(::benchmark::State& state,
                               const std::shared_ptr<Array>& array, bool nullable,
                               std::shared_ptr<WriterProperties> properties,
                               int64_t num_values = -1, int64_t total_bytes = -1) {
  auto schema = ::arrow::schema({field("s", array->type(), nullable)});
  auto table = Table::Make(schema, {array}, array->length());

  EXIT_NOT_OK(table->Validate());

  BenchmarkReadTable(state, *table, num_values, total_bytes);
}

static void BenchmarkReadArray(::benchmark::State& state,
                               const std::shared_ptr<Array>& array, bool nullable,
                               int64_t num_values = -1, int64_t total_bytes = -1) {
  BenchmarkReadArray(state, array, nullable, default_writer_properties(), num_values,
                     total_bytes);
}

//
// Benchmark reading a dict-encoded primitive column
//

template <bool nullable, typename ParquetType>
static void BM_ReadColumn(::benchmark::State& state) {
  using T = typename ParquetType::c_type;

  auto values = RandomVector<T>(/*percentage=*/state.range(1), BENCHMARK_SIZE,
                                Examples<T>::values());

  std::shared_ptr<Table> table =
      TableFromVector<ParquetType>(values, nullable, state.range(0));

  auto properties = WriterProperties::Builder().disable_dictionary()->build();

  BenchmarkReadTable(state, *table, properties, table->num_rows(),
                     BytesForItems<ParquetType>(table->num_rows()));
}

// There are two parameters here that cover different data distributions.
// null_percentage governs distribution and therefore runs of null values.
// first_value_percentage governs distribution of values (we select from 1 of 2)
// so when 0 or 100 RLE is triggered all the time.  When a value in the range (0, 100)
// there will be some percentage of RLE-encoded dictionary indices and some
// percentage of literal encoded dictionary indices
// (RLE is much less likely with percentages close to 50).
BENCHMARK_TEMPLATE2(BM_ReadColumn, false, Int32Type)
    ->Args({/*null_percentage=*/kAlternatingOrNa, 1})
    ->Args({/*null_percentage=*/kAlternatingOrNa, 10})
    ->Args({/*null_percentage=*/kAlternatingOrNa, 50});

BENCHMARK_TEMPLATE2(BM_ReadColumn, true, Int32Type)
    ->Args({/*null_percentage=*/kAlternatingOrNa, /*first_value_percentage=*/0})
    ->Args({/*null_percentage=*/0, /*first_value_percentage=*/1})
    ->Args({/*null_percentage=*/1, /*first_value_percentage=*/1})
    ->Args({/*null_percentage=*/10, /*first_value_percentage=*/10})
    ->Args({/*null_percentage=*/25, /*first_value_percentage=*/5})
    ->Args({/*null_percentage=*/50, /*first_value_percentage=*/50})
    ->Args({/*null_percentage=*/50, /*first_value_percentage=*/0})
    ->Args({/*null_percentage=*/99, /*first_value_percentage=*/50})
    ->Args({/*null_percentage=*/99, /*first_value_percentage=*/0});

BENCHMARK_TEMPLATE2(BM_ReadColumn, false, Int64Type)
    ->Args({/*null_percentage=*/kAlternatingOrNa, 1})
    ->Args({/*null_percentage=*/kAlternatingOrNa, 10})
    ->Args({/*null_percentage=*/kAlternatingOrNa, 50});
BENCHMARK_TEMPLATE2(BM_ReadColumn, true, Int64Type)
    ->Args({/*null_percentage=*/kAlternatingOrNa, /*first_value_percentage=*/0})
    ->Args({/*null_percentage=*/1, /*first_value_percentage=*/1})
    ->Args({/*null_percentage=*/5, /*first_value_percentage=*/5})
    ->Args({/*null_percentage=*/10, /*first_value_percentage=*/5})
    ->Args({/*null_percentage=*/25, /*first_value_percentage=*/10})
    ->Args({/*null_percentage=*/30, /*first_value_percentage=*/10})
    ->Args({/*null_percentage=*/35, /*first_value_percentage=*/10})
    ->Args({/*null_percentage=*/45, /*first_value_percentage=*/25})
    ->Args({/*null_percentage=*/50, /*first_value_percentage=*/50})
    ->Args({/*null_percentage=*/50, /*first_value_percentage=*/1})
    ->Args({/*null_percentage=*/75, /*first_value_percentage=*/1})
    ->Args({/*null_percentage=*/99, /*first_value_percentage=*/50})
    ->Args({/*null_percentage=*/99, /*first_value_percentage=*/0});

BENCHMARK_TEMPLATE2(BM_ReadColumn, false, DoubleType)
    ->Args({kAlternatingOrNa, 0})
    ->Args({kAlternatingOrNa, 20});
// Less coverage because int64_t should be pretty good representation for nullability and
// repeating values.
BENCHMARK_TEMPLATE2(BM_ReadColumn, true, DoubleType)
    ->Args({/*null_percentage=*/kAlternatingOrNa, /*first_value_percentage=*/0})
    ->Args({/*null_percentage=*/10, /*first_value_percentage=*/50})
    ->Args({/*null_percentage=*/25, /*first_value_percentage=*/25});

BENCHMARK_TEMPLATE2(BM_ReadColumn, false, BooleanType)
    ->Args({kAlternatingOrNa, 0})
    ->Args({1, 20});
BENCHMARK_TEMPLATE2(BM_ReadColumn, true, BooleanType)
    ->Args({kAlternatingOrNa, 1})
    ->Args({5, 10});

//
// Benchmark reading a non-dict-encoded primitive column
//

template <typename ParquetType>
static void BenchmarkReadNonDictColumn(::benchmark::State& state, bool nullable,
                                       Encoding::type encoding) {
  using c_type = typename ArrowType<ParquetType>::c_type;

  const std::vector<c_type> values(BENCHMARK_SIZE, static_cast<c_type>(42));
  std::shared_ptr<Table> table =
      TableFromVector<ParquetType>(values, /*nullable=*/nullable, state.range(0));

  auto properties =
      WriterProperties::Builder().disable_dictionary()->encoding(encoding)->build();
  BenchmarkReadTable(state, *table, properties, table->num_rows(),
                     BytesForItems<ParquetType>(table->num_rows()));
}

template <bool nullable, typename ParquetType>
static void BM_ReadColumnPlain(::benchmark::State& state) {
  BenchmarkReadNonDictColumn<ParquetType>(state, nullable, Encoding::PLAIN);
}

template <bool nullable, typename ParquetType>
static void BM_ReadColumnByteStreamSplit(::benchmark::State& state) {
  BenchmarkReadNonDictColumn<ParquetType>(state, nullable, Encoding::BYTE_STREAM_SPLIT);
}

BENCHMARK_TEMPLATE2(BM_ReadColumnPlain, false, Int32Type)
    ->ArgNames({"null_probability"})
    ->Args({kAlternatingOrNa});
BENCHMARK_TEMPLATE2(BM_ReadColumnPlain, true, Int32Type)
    ->ArgNames({"null_probability"})
    ->Args({0})
    ->Args({1})
    ->Args({50})
    ->Args({99})
    ->Args({100});

BENCHMARK_TEMPLATE2(BM_ReadColumnPlain, false, Float16LogicalType)
    ->ArgNames({"null_probability"})
    ->Args({kAlternatingOrNa});
BENCHMARK_TEMPLATE2(BM_ReadColumnPlain, true, Float16LogicalType)
    ->ArgNames({"null_probability"})
    ->Args({0})
    ->Args({1})
    ->Args({50})
    ->Args({99})
    ->Args({100});

BENCHMARK_TEMPLATE2(BM_ReadColumnByteStreamSplit, false, Float16LogicalType)
    ->ArgNames({"null_probability"})
    ->Args({kAlternatingOrNa});
BENCHMARK_TEMPLATE2(BM_ReadColumnByteStreamSplit, true, Float16LogicalType)
    ->ArgNames({"null_probability"})
    ->Args({0})
    ->Args({1})
    ->Args({50})
    ->Args({99})
    ->Args({100});

//
// Benchmark reading binary column
//

static void BenchmarkReadBinaryColumn(::benchmark::State& state,
                                      const std::shared_ptr<::arrow::DataType>& type,
                                      Encoding::type encoding) {
  std::shared_ptr<Table> table =
      RandomStringTable(type, BENCHMARK_SIZE, state.range(1), state.range(0));

  // Offsets / views + data
  int64_t total_bytes = 0;
  const ::arrow::ArrayData& column = *table->column(0)->chunk(0)->data();
  for (size_t i = 1; i < column.buffers.size(); ++i) {
    total_bytes += column.buffers[i]->size();
  }

  auto properties = WriterProperties::Builder().encoding(encoding)->build();
  BenchmarkReadTable(state, *table, properties, table->num_rows(), total_bytes);
}

static void SetReadBinaryColumnArgs(benchmark::internal::Benchmark* b) {
  b->ArgNames({"null_probability", "unique_values"})
      // We vary unique values to trigger the dictionary-encoded (for low-cardinality)
      // and plain (for high-cardinality) code paths.
      ->Args({0, 32})
      ->Args({0, kInfiniteUniqueValues})
      ->Args({1, 32})
      ->Args({50, 32})
      ->Args({99, 32})
      ->Args({1, kInfiniteUniqueValues})
      ->Args({50, kInfiniteUniqueValues})
      ->Args({99, kInfiniteUniqueValues});
}

static void SetReadBinaryColumnArgsWithoutDictEncoding(
    benchmark::internal::Benchmark* b) {
  b->ArgNames({"null_probability", "unique_values"})
      // Dict-encoding is already tested in the PLAIN benchmarks, so only exercise
      // non-dict-encoding using high cardinality.
      ->Args({0, kInfiniteUniqueValues})
      ->Args({1, kInfiniteUniqueValues})
      ->Args({50, kInfiniteUniqueValues})
      ->Args({99, kInfiniteUniqueValues});
}

static void BM_ReadBinaryColumn(::benchmark::State& state) {
  BenchmarkReadBinaryColumn(state, ::arrow::utf8(), Encoding::PLAIN);
}

static void BM_ReadBinaryViewColumn(::benchmark::State& state) {
  BenchmarkReadBinaryColumn(state, ::arrow::large_utf8(), Encoding::PLAIN);
}

static void BM_ReadBinaryColumnDeltaByteArray(::benchmark::State& state) {
  BenchmarkReadBinaryColumn(state, ::arrow::utf8(), Encoding::DELTA_BYTE_ARRAY);
}

static void BM_ReadBinaryViewColumnDeltaByteArray(::benchmark::State& state) {
  BenchmarkReadBinaryColumn(state, ::arrow::large_utf8(), Encoding::DELTA_BYTE_ARRAY);
}

BENCHMARK(BM_ReadBinaryColumn)->Apply(SetReadBinaryColumnArgs);
BENCHMARK(BM_ReadBinaryViewColumn)->Apply(SetReadBinaryColumnArgs);

BENCHMARK(BM_ReadBinaryColumnDeltaByteArray)
    ->Apply(SetReadBinaryColumnArgsWithoutDictEncoding);
BENCHMARK(BM_ReadBinaryViewColumnDeltaByteArray)
    ->Apply(SetReadBinaryColumnArgsWithoutDictEncoding);

//
// Benchmark reading a nested column
//

const std::vector<int64_t> kNestedNullPercents = {0, 1, 50, 99};

// XXX We can use ArgsProduct() starting from Benchmark 1.5.2
static void NestedReadArguments(::benchmark::internal::Benchmark* b) {
  for (const auto null_percentage : kNestedNullPercents) {
    b->Arg(null_percentage);
  }
}

static std::shared_ptr<Array> MakeStructArray(::arrow::random::RandomArrayGenerator* rng,
                                              const ArrayVector& children,
                                              double null_probability,
                                              bool propagate_validity = false) {
  ARROW_CHECK_GT(children.size(), 0);
  const int64_t length = children[0]->length();

  std::shared_ptr<::arrow::Buffer> null_bitmap;
  if (null_probability > 0.0) {
    null_bitmap = rng->NullBitmap(length, null_probability);
    if (propagate_validity) {
      // HACK: the Parquet writer currently doesn't allow non-empty list
      // entries where a parent node is null (for instance, a struct-of-list
      // where the outer struct is marked null but the inner list value is
      // non-empty).
      for (const auto& child : children) {
        null_bitmap = *::arrow::internal::BitmapOr(
            ::arrow::default_memory_pool(), null_bitmap->data(), 0,
            child->null_bitmap_data(), 0, length, 0);
      }
    }
  }
  FieldVector fields(children.size());
  char field_name = 'a';
  for (size_t i = 0; i < children.size(); ++i) {
    fields[i] = field(std::string{field_name++}, children[i]->type(),
                      /*nullable=*/null_probability > 0.0);
  }
  return *::arrow::StructArray::Make(children, std::move(fields), null_bitmap);
}

// Make a (int32, int64) struct array
static std::shared_ptr<Array> MakeStructArray(::arrow::random::RandomArrayGenerator* rng,
                                              int64_t size, double null_probability) {
  auto values1 = rng->Int32(size, -5, 5, null_probability);
  auto values2 = rng->Int64(size, -12345678912345LL, 12345678912345LL, null_probability);
  return MakeStructArray(rng, {values1, values2}, null_probability);
}

static void BM_ReadStructColumn(::benchmark::State& state) {
  constexpr int64_t kNumValues = BENCHMARK_SIZE / 10;
  const double null_probability = static_cast<double>(state.range(0)) / 100.0;
  const bool nullable = (null_probability != 0.0);

  ARROW_CHECK_GE(null_probability, 0.0);

  const int64_t kBytesPerValue = sizeof(int32_t) + sizeof(int64_t);

  ::arrow::random::RandomArrayGenerator rng(42);
  auto array = MakeStructArray(&rng, kNumValues, null_probability);

  BenchmarkReadArray(state, array, nullable, kNumValues, kBytesPerValue * kNumValues);
}

BENCHMARK(BM_ReadStructColumn)->Apply(NestedReadArguments);

static void BM_ReadStructOfStructColumn(::benchmark::State& state) {
  constexpr int64_t kNumValues = BENCHMARK_SIZE / 10;
  const double null_probability = static_cast<double>(state.range(0)) / 100.0;
  const bool nullable = (null_probability != 0.0);

  ARROW_CHECK_GE(null_probability, 0.0);

  const int64_t kBytesPerValue = 2 * (sizeof(int32_t) + sizeof(int64_t));

  ::arrow::random::RandomArrayGenerator rng(42);
  auto values1 = MakeStructArray(&rng, kNumValues, null_probability);
  auto values2 = MakeStructArray(&rng, kNumValues, null_probability);
  auto array = MakeStructArray(&rng, {values1, values2}, null_probability);

  BenchmarkReadArray(state, array, nullable, kNumValues, kBytesPerValue * kNumValues);
}

BENCHMARK(BM_ReadStructOfStructColumn)->Apply(NestedReadArguments);

static void BM_ReadStructOfListColumn(::benchmark::State& state) {
  constexpr int64_t kNumValues = BENCHMARK_SIZE / 10;
  const double null_probability = static_cast<double>(state.range(0)) / 100.0;
  const bool nullable = (null_probability != 0.0);

  ARROW_CHECK_GE(null_probability, 0.0);

  ::arrow::random::RandomArrayGenerator rng(42);

  const int64_t kBytesPerValue = sizeof(int32_t) + sizeof(int64_t);

  auto values1 = rng.Int32(kNumValues, -5, 5, null_probability);
  auto values2 =
      rng.Int64(kNumValues, -12345678912345LL, 12345678912345LL, null_probability);
  auto list1 = rng.List(*values1, kNumValues / 10, null_probability);
  auto list2 = rng.List(*values2, kNumValues / 10, null_probability);
  auto array = MakeStructArray(&rng, {list1, list2}, null_probability,
                               /*propagate_validity =*/true);

  BenchmarkReadArray(state, array, nullable, kNumValues, kBytesPerValue * kNumValues);
}

BENCHMARK(BM_ReadStructOfListColumn)->Apply(NestedReadArguments);

static void BM_ReadListColumn(::benchmark::State& state) {
  constexpr int64_t kNumValues = BENCHMARK_SIZE / 10;
  const double null_probability = static_cast<double>(state.range(0)) / 100.0;
  const bool nullable = (null_probability != 0.0);

  ARROW_CHECK_GE(null_probability, 0.0);

  ::arrow::random::RandomArrayGenerator rng(42);

  auto values = rng.Int64(kNumValues, /*min=*/-5, /*max=*/5, null_probability);
  const int64_t kBytesPerValue = sizeof(int64_t);

  auto array = rng.List(*values, kNumValues / 10, null_probability);

  BenchmarkReadArray(state, array, nullable, kNumValues, kBytesPerValue * kNumValues);
}

BENCHMARK(BM_ReadListColumn)->Apply(NestedReadArguments);

static void BM_ReadListOfStructColumn(::benchmark::State& state) {
  constexpr int64_t kNumValues = BENCHMARK_SIZE / 10;
  const double null_probability = static_cast<double>(state.range(0)) / 100.0;
  const bool nullable = (null_probability != 0.0);

  ARROW_CHECK_GE(null_probability, 0.0);

  ::arrow::random::RandomArrayGenerator rng(42);

  auto values = MakeStructArray(&rng, kNumValues, null_probability);
  const int64_t kBytesPerValue = sizeof(int32_t) + sizeof(int64_t);

  auto array = rng.List(*values, kNumValues / 10, null_probability);

  BenchmarkReadArray(state, array, nullable, kNumValues, kBytesPerValue * kNumValues);
}

BENCHMARK(BM_ReadListOfStructColumn)->Apply(NestedReadArguments);

static void BM_ReadListOfListColumn(::benchmark::State& state) {
  constexpr int64_t kNumValues = BENCHMARK_SIZE / 10;
  const double null_probability = static_cast<double>(state.range(0)) / 100.0;
  const bool nullable = (null_probability != 0.0);

  ARROW_CHECK_GE(null_probability, 0.0);

  ::arrow::random::RandomArrayGenerator rng(42);

  auto values = rng.Int64(kNumValues, /*min=*/-5, /*max=*/5, null_probability);
  const int64_t kBytesPerValue = sizeof(int64_t);

  auto inner = rng.List(*values, kNumValues / 10, null_probability);
  auto array = rng.List(*inner, kNumValues / 100, null_probability);

  BenchmarkReadArray(state, array, nullable, kNumValues, kBytesPerValue * kNumValues);
}

BENCHMARK(BM_ReadListOfListColumn)->Apply(NestedReadArguments);

std::shared_ptr<Array> MakeFloatValues(int64_t num_values, bool nullable = false) {
  ::arrow::FloatBuilder value_builder;
  EXIT_NOT_OK(value_builder.Reserve(num_values));
  for (int64_t i = 0; i < num_values; ++i) {
    if (nullable && (i % 2) == 1) {
      EXIT_NOT_OK(value_builder.AppendNull());
    } else {
      EXIT_NOT_OK(value_builder.Append(static_cast<float>(i % 1024) / 1024.0f));
    }
  }
  std::shared_ptr<Array> values;
  EXIT_NOT_OK(value_builder.Finish(&values));
  return values;
}

int64_t CountAlternatingValidRows(int64_t num_rows) { return (num_rows + 1) / 2; }

std::shared_ptr<Buffer> MakeAlternatingValidityBitmap(int64_t num_rows,
                                                      int64_t* null_count_out) {
  const int64_t valid_count = CountAlternatingValidRows(num_rows);
  *null_count_out = num_rows - valid_count;
  auto bitmap = ::arrow::AllocateEmptyBitmap(num_rows, ::arrow::default_memory_pool())
                    .MoveValueUnsafe();
  ::arrow::bit_util::SetBitsTo(bitmap->mutable_data(), 0, num_rows, false);
  for (int64_t i = 0; i < num_rows; ++i) {
    if ((i % 2) == 0) {
      ::arrow::bit_util::SetBit(bitmap->mutable_data(), i);
    }
  }
  return bitmap;
}

std::shared_ptr<Table> MakeFixedSizeListFloatTable(int64_t num_rows, int32_t list_size,
                                                   bool nullable = false,
                                                   bool element_nullable = false) {
  const int64_t num_values = num_rows * list_size;
  auto values = MakeFloatValues(num_values, element_nullable);

  auto value_field = ::arrow::field("item", ::arrow::float32(), element_nullable);
  auto type = ::arrow::fixed_size_list(value_field, list_size);
  std::shared_ptr<Array> list_array;
  if (nullable) {
    int64_t null_count = 0;
    auto validity = MakeAlternatingValidityBitmap(num_rows, &null_count);
    PARQUET_ASSIGN_OR_THROW(list_array, ::arrow::FixedSizeListArray::FromArrays(
                                            values, type, validity, null_count));
  } else {
    PARQUET_ASSIGN_OR_THROW(list_array,
                            ::arrow::FixedSizeListArray::FromArrays(values, type));
  }
  auto field = ::arrow::field("column", type, nullable);
  return Table::Make(::arrow::schema({field}), {list_array}, num_rows);
}

std::shared_ptr<Array> MakeInt32Values(int64_t num_values) {
  ::arrow::Int32Builder builder;
  EXIT_NOT_OK(builder.Reserve(num_values));
  for (int64_t i = 0; i < num_values; ++i) {
    EXIT_NOT_OK(builder.Append(static_cast<int32_t>(i % 1024)));
  }
  std::shared_ptr<Array> values;
  EXIT_NOT_OK(builder.Finish(&values));
  return values;
}

std::shared_ptr<Table> MakeFixedSizeListStructTable(int64_t num_rows, int32_t list_size) {
  const int64_t num_values = num_rows * list_size;
  auto x = MakeFloatValues(num_values);
  auto y = MakeInt32Values(num_values);
  auto struct_type = ::arrow::struct_({::arrow::field("x", ::arrow::float32(), false),
                                       ::arrow::field("y", ::arrow::int32(), false)});
  auto values = std::make_shared<::arrow::StructArray>(struct_type, num_values,
                                                       ::arrow::ArrayVector{x, y});
  auto type =
      ::arrow::fixed_size_list(::arrow::field("item", struct_type, false), list_size);
  PARQUET_ASSIGN_OR_THROW(auto list_array,
                          ::arrow::FixedSizeListArray::FromArrays(values, type));
  auto field = ::arrow::field("column", type, false);
  return Table::Make(::arrow::schema({field}), {list_array}, num_rows);
}

std::shared_ptr<WriterProperties> FixedSizeListWriterProperties(
    Encoding::type encoding) {
  auto builder = WriterProperties::Builder();
  return builder.disable_dictionary()
      ->disable_statistics()
      ->disable_write_page_index()
      ->encoding(encoding)
      ->build();
}

std::shared_ptr<WriterProperties> FixedSizeListVectorWriterProperties() {
  return FixedSizeListWriterProperties(Encoding::PLAIN);
}

std::shared_ptr<WriterProperties> FixedSizeListVectorByteStreamSplitWriterProperties() {
  return FixedSizeListWriterProperties(Encoding::BYTE_STREAM_SPLIT);
}

// ArrowWriterProperties used by the LIST side. Same as VECTOR side except the
// experimental_vector_encoding flag is off.
std::shared_ptr<ArrowWriterProperties> FixedSizeListAsListArrowWriterProperties() {
  ArrowWriterProperties::Builder builder;
  builder.store_schema();
  return builder.build();
}

std::shared_ptr<ArrowWriterProperties> FixedSizeListVectorArrowWriterProperties() {
  ArrowWriterProperties::Builder builder;
  builder.store_schema();
  builder.enable_experimental_vector_encoding();
  return builder.build();
}

static void BM_WriteFixedSizeListFloatAsList(::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeFixedSizeListFloatTable(num_rows, list_size, /*nullable=*/false);
  const int64_t total_bytes = num_rows * list_size * static_cast<int64_t>(sizeof(float));

  BenchmarkWriteTable(state, *table, FixedSizeListVectorWriterProperties(),
                      FixedSizeListAsListArrowWriterProperties(), num_rows, total_bytes);
}

static void BM_ReadFixedSizeListFloatAsList(::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeFixedSizeListFloatTable(num_rows, list_size, /*nullable=*/false);
  const int64_t total_bytes = num_rows * list_size * static_cast<int64_t>(sizeof(float));

  BenchmarkReadTable(state, *table, FixedSizeListVectorWriterProperties(),
                     FixedSizeListAsListArrowWriterProperties(), num_rows, total_bytes);
}

static void BM_RoundtripFixedSizeListFloatAsList(::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeFixedSizeListFloatTable(num_rows, list_size, /*nullable=*/false);
  const int64_t total_bytes = num_rows * list_size * static_cast<int64_t>(sizeof(float));

  BenchmarkRoundtripTable(state, *table, FixedSizeListVectorWriterProperties(),
                          FixedSizeListAsListArrowWriterProperties(), num_rows,
                          total_bytes);
}

static void BM_WriteFixedSizeListFloatAsListByteStreamSplit(
    ::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeFixedSizeListFloatTable(num_rows, list_size, /*nullable=*/false);
  const int64_t total_bytes = num_rows * list_size * static_cast<int64_t>(sizeof(float));

  BenchmarkWriteTable(state, *table, FixedSizeListVectorByteStreamSplitWriterProperties(),
                      FixedSizeListAsListArrowWriterProperties(), num_rows, total_bytes);
}

static void BM_ReadFixedSizeListFloatAsListByteStreamSplit(::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeFixedSizeListFloatTable(num_rows, list_size, /*nullable=*/false);
  const int64_t total_bytes = num_rows * list_size * static_cast<int64_t>(sizeof(float));

  BenchmarkReadTable(state, *table, FixedSizeListVectorByteStreamSplitWriterProperties(),
                     FixedSizeListAsListArrowWriterProperties(), num_rows, total_bytes);
}

static void BM_RoundtripFixedSizeListFloatAsListByteStreamSplit(
    ::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeFixedSizeListFloatTable(num_rows, list_size, /*nullable=*/false);
  const int64_t total_bytes = num_rows * list_size * static_cast<int64_t>(sizeof(float));

  BenchmarkRoundtripTable(state, *table,
                          FixedSizeListVectorByteStreamSplitWriterProperties(),
                          FixedSizeListAsListArrowWriterProperties(), num_rows,
                          total_bytes);
}

static void BM_WriteFixedSizeListFloatVector(::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeFixedSizeListFloatTable(num_rows, list_size, /*nullable=*/false);
  const int64_t total_bytes = num_rows * list_size * static_cast<int64_t>(sizeof(float));

  BenchmarkWriteTable(state, *table, FixedSizeListVectorWriterProperties(),
                      FixedSizeListVectorArrowWriterProperties(), num_rows, total_bytes);
}

static void BM_ReadFixedSizeListFloatVector(::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeFixedSizeListFloatTable(num_rows, list_size, /*nullable=*/false);
  const int64_t total_bytes = num_rows * list_size * static_cast<int64_t>(sizeof(float));

  BenchmarkReadTable(state, *table, FixedSizeListVectorWriterProperties(),
                     FixedSizeListVectorArrowWriterProperties(), num_rows, total_bytes);
}

static void BM_RoundtripFixedSizeListFloatVector(::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeFixedSizeListFloatTable(num_rows, list_size, /*nullable=*/false);
  const int64_t total_bytes = num_rows * list_size * static_cast<int64_t>(sizeof(float));

  BenchmarkRoundtripTable(state, *table, FixedSizeListVectorWriterProperties(),
                          FixedSizeListVectorArrowWriterProperties(), num_rows,
                          total_bytes);
}

static void BM_WriteFixedSizeListFloatVectorByteStreamSplit(
    ::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeFixedSizeListFloatTable(num_rows, list_size, /*nullable=*/false);
  const int64_t total_bytes = num_rows * list_size * static_cast<int64_t>(sizeof(float));

  BenchmarkWriteTable(state, *table, FixedSizeListVectorByteStreamSplitWriterProperties(),
                      FixedSizeListVectorArrowWriterProperties(), num_rows, total_bytes);
}

static void BM_ReadFixedSizeListFloatVectorByteStreamSplit(
    ::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeFixedSizeListFloatTable(num_rows, list_size, /*nullable=*/false);
  const int64_t total_bytes = num_rows * list_size * static_cast<int64_t>(sizeof(float));

  BenchmarkReadTable(state, *table, FixedSizeListVectorByteStreamSplitWriterProperties(),
                     FixedSizeListVectorArrowWriterProperties(), num_rows, total_bytes);
}

static void BM_RoundtripFixedSizeListFloatVectorByteStreamSplit(
    ::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeFixedSizeListFloatTable(num_rows, list_size, /*nullable=*/false);
  const int64_t total_bytes = num_rows * list_size * static_cast<int64_t>(sizeof(float));

  BenchmarkRoundtripTable(state, *table,
                          FixedSizeListVectorByteStreamSplitWriterProperties(),
                          FixedSizeListVectorArrowWriterProperties(), num_rows,
                          total_bytes);
}

static void BM_WriteFixedSizeListFloatVectorNullable(::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  const int64_t present_rows = CountAlternatingValidRows(num_rows);
  auto table = MakeFixedSizeListFloatTable(num_rows, list_size, /*nullable=*/true);
  const int64_t total_bytes =
      present_rows * list_size * static_cast<int64_t>(sizeof(float));

  BenchmarkWriteTable(state, *table, FixedSizeListVectorWriterProperties(),
                      FixedSizeListVectorArrowWriterProperties(), num_rows, total_bytes);
}

static void BM_ReadFixedSizeListFloatVectorNullable(::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  const int64_t present_rows = CountAlternatingValidRows(num_rows);
  auto table = MakeFixedSizeListFloatTable(num_rows, list_size, /*nullable=*/true);
  const int64_t total_bytes =
      present_rows * list_size * static_cast<int64_t>(sizeof(float));

  BenchmarkReadTable(state, *table, FixedSizeListVectorWriterProperties(),
                     FixedSizeListVectorArrowWriterProperties(), num_rows, total_bytes);
}

static void BM_RoundtripFixedSizeListFloatVectorNullable(::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  const int64_t present_rows = CountAlternatingValidRows(num_rows);
  auto table = MakeFixedSizeListFloatTable(num_rows, list_size, /*nullable=*/true);
  const int64_t total_bytes =
      present_rows * list_size * static_cast<int64_t>(sizeof(float));

  BenchmarkRoundtripTable(state, *table, FixedSizeListVectorWriterProperties(),
                          FixedSizeListVectorArrowWriterProperties(), num_rows,
                          total_bytes);
}

static void BM_RoundtripFixedSizeListStructVector(::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeFixedSizeListStructTable(num_rows, list_size);
  const int64_t total_bytes =
      num_rows * list_size * static_cast<int64_t>(sizeof(float) + sizeof(int32_t));

  BenchmarkRoundtripTable(state, *table, FixedSizeListVectorWriterProperties(),
                          FixedSizeListVectorArrowWriterProperties(), num_rows,
                          total_bytes);
}

// Realistic embedding-table row: scalar metadata columns alongside a FSL<float, N>
// embedding. Used to measure VECTOR vs LIST in a wide-table layout instead of a
// single-column microbenchmark.
std::shared_ptr<Table> MakeRealisticEmbeddingTable(int64_t num_rows, int32_t list_size) {
  const int64_t num_values = num_rows * list_size;

  ::arrow::Int64Builder id_builder;
  EXIT_NOT_OK(id_builder.Reserve(num_rows));
  for (int64_t i = 0; i < num_rows; ++i) {
    EXIT_NOT_OK(id_builder.Append(i));
  }
  std::shared_ptr<Array> id;
  EXIT_NOT_OK(id_builder.Finish(&id));

  auto ts_type = ::arrow::timestamp(::arrow::TimeUnit::MICRO);
  ::arrow::TimestampBuilder ts_builder(ts_type, ::arrow::default_memory_pool());
  EXIT_NOT_OK(ts_builder.Reserve(num_rows));
  const int64_t epoch_us = 1700000000LL * 1000000LL;
  for (int64_t i = 0; i < num_rows; ++i) {
    EXIT_NOT_OK(ts_builder.Append(epoch_us + i * 1000000LL));
  }
  std::shared_ptr<Array> ts;
  EXIT_NOT_OK(ts_builder.Finish(&ts));

  ::arrow::Int32Builder category_builder;
  EXIT_NOT_OK(category_builder.Reserve(num_rows));
  for (int64_t i = 0; i < num_rows; ++i) {
    EXIT_NOT_OK(category_builder.Append(static_cast<int32_t>(i % 64)));
  }
  std::shared_ptr<Array> category;
  EXIT_NOT_OK(category_builder.Finish(&category));

  auto score = MakeFloatValues(num_rows);

  ::arrow::StringBuilder label_builder;
  EXIT_NOT_OK(label_builder.Reserve(num_rows));
  constexpr std::array<const char*, 8> kLabels = {"alpha",   "beta", "gamma", "delta",
                                                  "epsilon", "zeta", "eta",   "theta"};
  for (int64_t i = 0; i < num_rows; ++i) {
    EXIT_NOT_OK(label_builder.Append(kLabels[i % kLabels.size()]));
  }
  std::shared_ptr<Array> label;
  EXIT_NOT_OK(label_builder.Finish(&label));

  auto embedding_values = MakeFloatValues(num_values);
  auto embedding_type = ::arrow::fixed_size_list(
      ::arrow::field("item", ::arrow::float32(), false), list_size);
  std::shared_ptr<Array> embedding;
  PARQUET_ASSIGN_OR_THROW(embedding, ::arrow::FixedSizeListArray::FromArrays(
                                         embedding_values, embedding_type));

  auto schema = ::arrow::schema({
      ::arrow::field("id", ::arrow::int64(), false),
      ::arrow::field("ts", ts_type, false),
      ::arrow::field("category", ::arrow::int32(), false),
      ::arrow::field("score", ::arrow::float32(), false),
      ::arrow::field("label", ::arrow::utf8(), false),
      ::arrow::field("embedding", embedding_type, false),
  });
  return Table::Make(schema, {id, ts, category, score, label, embedding}, num_rows);
}

// Bytes processed counts every column. Both LIST and VECTOR sides report the same
// number so per-column ratios are comparable across sides.
int64_t RealisticEmbeddingTableBytes(int64_t num_rows, int32_t list_size) {
  const int64_t per_row_scalar = sizeof(int64_t)    // id
                                 + sizeof(int64_t)  // ts
                                 + sizeof(int32_t)  // category
                                 + sizeof(float);   // score
  // Average string length for kLabels above is ~5.5 chars; round to 6.
  const int64_t per_row_label = 6;
  const int64_t per_row_embedding = list_size * static_cast<int64_t>(sizeof(float));
  return num_rows * (per_row_scalar + per_row_label + per_row_embedding);
}

static void BM_WriteRealisticEmbeddingRowAsList(::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeRealisticEmbeddingTable(num_rows, list_size);
  const int64_t total_bytes = RealisticEmbeddingTableBytes(num_rows, list_size);

  BenchmarkWriteTable(state, *table, FixedSizeListVectorWriterProperties(),
                      FixedSizeListAsListArrowWriterProperties(), num_rows, total_bytes);
}

static void BM_ReadRealisticEmbeddingRowAsList(::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeRealisticEmbeddingTable(num_rows, list_size);
  const int64_t total_bytes = RealisticEmbeddingTableBytes(num_rows, list_size);

  BenchmarkReadTable(state, *table, FixedSizeListVectorWriterProperties(),
                     FixedSizeListAsListArrowWriterProperties(), num_rows, total_bytes);
}

static void BM_RoundtripRealisticEmbeddingRowAsList(::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeRealisticEmbeddingTable(num_rows, list_size);
  const int64_t total_bytes = RealisticEmbeddingTableBytes(num_rows, list_size);

  BenchmarkRoundtripTable(state, *table, FixedSizeListVectorWriterProperties(),
                          FixedSizeListAsListArrowWriterProperties(), num_rows,
                          total_bytes);
}

static void BM_WriteRealisticEmbeddingRowVector(::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeRealisticEmbeddingTable(num_rows, list_size);
  const int64_t total_bytes = RealisticEmbeddingTableBytes(num_rows, list_size);

  BenchmarkWriteTable(state, *table, FixedSizeListVectorWriterProperties(),
                      FixedSizeListVectorArrowWriterProperties(), num_rows, total_bytes);
}

static void BM_ReadRealisticEmbeddingRowVector(::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeRealisticEmbeddingTable(num_rows, list_size);
  const int64_t total_bytes = RealisticEmbeddingTableBytes(num_rows, list_size);

  BenchmarkReadTable(state, *table, FixedSizeListVectorWriterProperties(),
                     FixedSizeListVectorArrowWriterProperties(), num_rows, total_bytes);
}

static void BM_RoundtripRealisticEmbeddingRowVector(::benchmark::State& state) {
  const int32_t list_size = static_cast<int32_t>(state.range(0));
  const int64_t num_rows = BENCHMARK_SIZE / list_size;
  auto table = MakeRealisticEmbeddingTable(num_rows, list_size);
  const int64_t total_bytes = RealisticEmbeddingTableBytes(num_rows, list_size);

  BenchmarkRoundtripTable(state, *table, FixedSizeListVectorWriterProperties(),
                          FixedSizeListVectorArrowWriterProperties(), num_rows,
                          total_bytes);
}

BENCHMARK(BM_WriteFixedSizeListFloatAsList)->Arg(80)->Arg(768)->Arg(10000);
BENCHMARK(BM_ReadFixedSizeListFloatAsList)->Arg(80)->Arg(768)->Arg(10000);
BENCHMARK(BM_RoundtripFixedSizeListFloatAsList)->Arg(80)->Arg(768)->Arg(10000);
BENCHMARK(BM_WriteFixedSizeListFloatAsListByteStreamSplit)
    ->Arg(80)
    ->Arg(768)
    ->Arg(10000);
BENCHMARK(BM_ReadFixedSizeListFloatAsListByteStreamSplit)
    ->Arg(80)
    ->Arg(768)
    ->Arg(10000);
BENCHMARK(BM_RoundtripFixedSizeListFloatAsListByteStreamSplit)
    ->Arg(80)
    ->Arg(768)
    ->Arg(10000);
BENCHMARK(BM_WriteFixedSizeListFloatVector)->Arg(80)->Arg(768)->Arg(10000);
BENCHMARK(BM_ReadFixedSizeListFloatVector)->Arg(80)->Arg(768)->Arg(10000);
BENCHMARK(BM_RoundtripFixedSizeListFloatVector)->Arg(80)->Arg(768)->Arg(10000);
BENCHMARK(BM_WriteFixedSizeListFloatVectorByteStreamSplit)
    ->Arg(80)
    ->Arg(768)
    ->Arg(10000);
BENCHMARK(BM_ReadFixedSizeListFloatVectorByteStreamSplit)
    ->Arg(80)
    ->Arg(768)
    ->Arg(10000);
BENCHMARK(BM_RoundtripFixedSizeListFloatVectorByteStreamSplit)
    ->Arg(80)
    ->Arg(768)
    ->Arg(10000);
BENCHMARK(BM_WriteFixedSizeListFloatVectorNullable)->Arg(80)->Arg(768)->Arg(10000);
BENCHMARK(BM_ReadFixedSizeListFloatVectorNullable)->Arg(80)->Arg(768)->Arg(10000);
BENCHMARK(BM_RoundtripFixedSizeListFloatVectorNullable)->Arg(80)->Arg(768)->Arg(10000);
BENCHMARK(BM_RoundtripFixedSizeListStructVector)->Arg(80);
BENCHMARK(BM_WriteRealisticEmbeddingRowAsList)->Arg(768);
BENCHMARK(BM_ReadRealisticEmbeddingRowAsList)->Arg(768);
BENCHMARK(BM_RoundtripRealisticEmbeddingRowAsList)->Arg(768);
BENCHMARK(BM_WriteRealisticEmbeddingRowVector)->Arg(768);
BENCHMARK(BM_ReadRealisticEmbeddingRowVector)->Arg(768);
BENCHMARK(BM_RoundtripRealisticEmbeddingRowVector)->Arg(768);

//
// Benchmark different ways of reading select row groups
//

static void BM_ReadIndividualRowGroups(::benchmark::State& state) {
  std::vector<int64_t> values(BENCHMARK_SIZE, 128);
  std::shared_ptr<Table> table = TableFromVector<Int64Type>(values, true);
  auto output = CreateOutputStream();
  // This writes 10 RowGroups
  EXIT_NOT_OK(
      WriteTable(*table, ::arrow::default_memory_pool(), output, BENCHMARK_SIZE / 10));

  PARQUET_ASSIGN_OR_THROW(auto buffer, output->Finish());

  while (state.KeepRunning()) {
    auto reader =
        ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
    auto arrow_reader_result =
        FileReader::Make(::arrow::default_memory_pool(), std::move(reader));
    EXIT_NOT_OK(arrow_reader_result.status());
    auto arrow_reader = std::move(*arrow_reader_result);

    std::vector<std::shared_ptr<Table>> tables;
    for (int i = 0; i < arrow_reader->num_row_groups(); i++) {
      // Only read the even numbered RowGroups
      if ((i % 2) == 0) {
        std::shared_ptr<Table> table;
        EXIT_NOT_OK(arrow_reader->RowGroup(i)->ReadTable(&table));
        tables.push_back(table);
      }
    }

    std::shared_ptr<Table> final_table;
    PARQUET_ASSIGN_OR_THROW(final_table, ConcatenateTables(tables));
  }
  SetBytesProcessed<Int64Type>(state);
}

BENCHMARK(BM_ReadIndividualRowGroups);

static void BM_ReadMultipleRowGroups(::benchmark::State& state) {
  std::vector<int64_t> values(BENCHMARK_SIZE, 128);
  std::shared_ptr<Table> table = TableFromVector<Int64Type>(values, true);
  auto output = CreateOutputStream();
  // This writes 10 RowGroups
  EXIT_NOT_OK(
      WriteTable(*table, ::arrow::default_memory_pool(), output, BENCHMARK_SIZE / 10));
  PARQUET_ASSIGN_OR_THROW(auto buffer, output->Finish());
  std::vector<int> rgs{0, 2, 4, 6, 8};

  while (state.KeepRunning()) {
    auto reader =
        ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
    auto arrow_reader_result =
        FileReader::Make(::arrow::default_memory_pool(), std::move(reader));
    EXIT_NOT_OK(arrow_reader_result.status());
    auto arrow_reader = std::move(*arrow_reader_result);

    PARQUET_ASSIGN_OR_THROW(auto table, arrow_reader->ReadRowGroups(rgs));
  }
  SetBytesProcessed<Int64Type>(state);
}

BENCHMARK(BM_ReadMultipleRowGroups);

static void BM_ReadMultipleRowGroupsGenerator(::benchmark::State& state) {
  std::vector<int64_t> values(BENCHMARK_SIZE, 128);
  std::shared_ptr<Table> table = TableFromVector<Int64Type>(values, true);
  auto output = CreateOutputStream();
  // This writes 10 RowGroups
  EXIT_NOT_OK(
      WriteTable(*table, ::arrow::default_memory_pool(), output, BENCHMARK_SIZE / 10));
  PARQUET_ASSIGN_OR_THROW(auto buffer, output->Finish());
  std::vector<int> rgs{0, 2, 4, 6, 8};

  while (state.KeepRunning()) {
    auto reader =
        ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
    auto arrow_reader_result =
        FileReader::Make(::arrow::default_memory_pool(), std::move(reader));
    EXIT_NOT_OK(arrow_reader_result.status());
    std::shared_ptr<FileReader> arrow_reader = std::move(*arrow_reader_result);
    ASSIGN_OR_ABORT(auto generator,
                    arrow_reader->GetRecordBatchGenerator(arrow_reader, rgs, {0}));
    auto fut = ::arrow::CollectAsyncGenerator(generator);
    ASSIGN_OR_ABORT(auto batches, fut.result());
    ASSIGN_OR_ABORT(auto actual, Table::FromRecordBatches(std::move(batches)));
  }
  SetBytesProcessed<Int64Type>(state);
}

BENCHMARK(BM_ReadMultipleRowGroupsGenerator);

}  // namespace benchmarks
}  // namespace parquet
