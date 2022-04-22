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

#include <functional>

#include "benchmark/benchmark.h"

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {

namespace compute {

constexpr auto kSeed = 0x94378165;

std::vector<int64_t> g_data_sizes = {kL2Size};
static constexpr int64_t kInt64Min = -2000000000;  // 1906-08-16 20:26:40
static constexpr int64_t kInt64Max = 2000000000;   // 2033-05-18 03:33:20

void SetArgs(benchmark::internal::Benchmark* bench) {
  for (const auto inverse_null_proportion : std::vector<ArgsType>({100, 0})) {
    bench->Args({static_cast<ArgsType>(kL2Size), inverse_null_proportion});
  }
}

using UnaryRoundingOp = Result<Datum>(const Datum&, const RoundTemporalOptions,
                                      ExecContext*);
using UnaryOp = Result<Datum>(const Datum&, ExecContext*);

template <UnaryRoundingOp& Op, std::shared_ptr<DataType>& timestamp_type,
          RoundTemporalOptions& options>
static void BenchmarkTemporalRounding(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(int64_t);

  auto rand = random::RandomArrayGenerator(kSeed);
  auto array = std::static_pointer_cast<NumericArray<Int64Type>>(
      rand.Numeric<Int64Type>(array_size, kInt64Min, kInt64Max, args.null_proportion));
  auto timestamp_array = std::make_shared<NumericArray<TimestampType>>(
      timestamp_type, array->length(), array->data()->buffers[1],
      array->data()->buffers[0], array->null_count());

  for (auto _ : state) {
    ABORT_NOT_OK(Op(timestamp_array, options, nullptr).status());
  }

  state.SetItemsProcessed(state.iterations() * array_size);
}

template <UnaryOp& Op, std::shared_ptr<DataType>& timestamp_type>
static void BenchmarkTemporal(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(int64_t);

  auto rand = random::RandomArrayGenerator(kSeed);
  auto array = std::static_pointer_cast<NumericArray<Int64Type>>(
      rand.Numeric<Int64Type>(array_size, kInt64Min, kInt64Max, args.null_proportion));
  auto timestamp_array = std::make_shared<NumericArray<TimestampType>>(
      timestamp_type, array->length(), array->data()->buffers[1],
      array->data()->buffers[0], array->null_count());

  for (auto _ : state) {
    ABORT_NOT_OK(Op(timestamp_array, nullptr).status());
  }

  state.SetItemsProcessed(state.iterations() * array_size);
}

auto zoned = timestamp(TimeUnit::NANO, "Pacific/Marquesas");
auto non_zoned = timestamp(TimeUnit::NANO);

#define DECLARE_TEMPORAL_ROUNDING_BENCHMARKS(OPTIONS)                              \
  BENCHMARK_TEMPLATE(BenchmarkTemporalRounding, CeilTemporal, zoned, OPTIONS)      \
      ->Apply(SetArgs);                                                            \
  BENCHMARK_TEMPLATE(BenchmarkTemporalRounding, FloorTemporal, zoned, OPTIONS)     \
      ->Apply(SetArgs);                                                            \
  BENCHMARK_TEMPLATE(BenchmarkTemporalRounding, RoundTemporal, zoned, OPTIONS)     \
      ->Apply(SetArgs);                                                            \
  BENCHMARK_TEMPLATE(BenchmarkTemporalRounding, CeilTemporal, non_zoned, OPTIONS)  \
      ->Apply(SetArgs);                                                            \
  BENCHMARK_TEMPLATE(BenchmarkTemporalRounding, FloorTemporal, non_zoned, OPTIONS) \
      ->Apply(SetArgs);                                                            \
  BENCHMARK_TEMPLATE(BenchmarkTemporalRounding, RoundTemporal, non_zoned, OPTIONS) \
      ->Apply(SetArgs);

#define DECLARE_TEMPORAL_BENCHMARKS(OP)                                 \
  BENCHMARK_TEMPLATE(BenchmarkTemporal, OP, non_zoned)->Apply(SetArgs); \
  BENCHMARK_TEMPLATE(BenchmarkTemporal, OP, zoned)->Apply(SetArgs);

#define DECLARE_TEMPORAL_BENCHMARKS_ZONED(OP) \
  BENCHMARK_TEMPLATE(BenchmarkTemporal, OP, zoned)->Apply(SetArgs);

// Temporal rounding benchmarks
auto round_1_minute = RoundTemporalOptions(1, CalendarUnit::MINUTE);
auto round_10_minute = RoundTemporalOptions(10, CalendarUnit::MINUTE);
auto round_1_week = RoundTemporalOptions(1, CalendarUnit::WEEK);
auto round_10_week = RoundTemporalOptions(10, CalendarUnit::WEEK);
auto round_1_month = RoundTemporalOptions(1, CalendarUnit::MONTH);
auto round_10_month = RoundTemporalOptions(10, CalendarUnit::MONTH);

DECLARE_TEMPORAL_ROUNDING_BENCHMARKS(round_1_minute);
DECLARE_TEMPORAL_ROUNDING_BENCHMARKS(round_1_week);
DECLARE_TEMPORAL_ROUNDING_BENCHMARKS(round_1_month);
DECLARE_TEMPORAL_ROUNDING_BENCHMARKS(round_10_minute);
DECLARE_TEMPORAL_ROUNDING_BENCHMARKS(round_10_week);
DECLARE_TEMPORAL_ROUNDING_BENCHMARKS(round_10_month);

// Temporal component extraction
DECLARE_TEMPORAL_BENCHMARKS(Year);
DECLARE_TEMPORAL_BENCHMARKS(IsLeapYear);
DECLARE_TEMPORAL_BENCHMARKS(Month);
DECLARE_TEMPORAL_BENCHMARKS(Day);
DECLARE_TEMPORAL_BENCHMARKS(DayOfYear);
DECLARE_TEMPORAL_BENCHMARKS_ZONED(IsDaylightSavings);
DECLARE_TEMPORAL_BENCHMARKS(USYear);
DECLARE_TEMPORAL_BENCHMARKS(ISOYear);
DECLARE_TEMPORAL_BENCHMARKS(ISOWeek);
DECLARE_TEMPORAL_BENCHMARKS(USWeek);
DECLARE_TEMPORAL_BENCHMARKS(Quarter);
DECLARE_TEMPORAL_BENCHMARKS(Hour);
DECLARE_TEMPORAL_BENCHMARKS(Minute);
DECLARE_TEMPORAL_BENCHMARKS(Second);
DECLARE_TEMPORAL_BENCHMARKS(Millisecond);
DECLARE_TEMPORAL_BENCHMARKS(Microsecond);
DECLARE_TEMPORAL_BENCHMARKS(Nanosecond);
DECLARE_TEMPORAL_BENCHMARKS(Subsecond);

}  // namespace compute
}  // namespace arrow
