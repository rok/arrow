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

#include <gtest/gtest.h>
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/formatting.h"

namespace arrow {

using internal::StringFormatter;

class ScalarTemporalTest : public ::testing::Test {};

namespace compute {

TEST(ScalarTemporalTest, TestSimpleTemporalComponentExtraction) {
  const char* times =
      R"(["1970-01-01T00:00:59.123456789","2000-02-29T23:23:23.999999999",
          "1900-01-01T01:59:20.001001001","2033-05-18T03:33:20.000000000"])";
  auto unit = timestamp(TimeUnit::NANO);
  auto timestamps = ArrayFromJSON(unit, times);

  auto year = "[1970, 2000, 1900, 2033]";
  auto month = "[1, 2, 1, 5]";
  auto day = "[1, 29, 1, 18]";
  auto day_of_week = "[4, 2, 1, 3]";
  auto day_of_year = "[1, 60, 1, 138]";
  auto week = "[1, 9, 1, 20]";
  auto quarter = "[1, 1, 1, 2]";
  auto hour = "[0, 23, 1, 3]";
  auto minute = "[0, 23, 59, 33]";
  auto second = "[59, 23, 20, 20]";
  auto millisecond = "[123, 999, 1, 0]";
  auto microsecond = "[456, 999, 1, 0]";
  auto nanosecond = "[789, 999, 1, 0]";

  CheckScalarUnary("year", unit, times, int64(), year);
  CheckScalarUnary("month", unit, times, int64(), month);
  CheckScalarUnary("day", unit, times, int64(), day);
  CheckScalarUnary("day_of_week", unit, times, int64(), day_of_week);
  CheckScalarUnary("day_of_year", unit, times, int64(), day_of_year);
  CheckScalarUnary("week", unit, times, int64(), week);
  CheckScalarUnary("quarter", unit, times, int64(), quarter);
  CheckScalarUnary("hour", unit, times, int64(), hour);
  CheckScalarUnary("minute", unit, times, int64(), minute);
  CheckScalarUnary("second", unit, times, int64(), second);
  CheckScalarUnary("millisecond", unit, times, int64(), millisecond);
  CheckScalarUnary("microsecond", unit, times, int64(), microsecond);
  CheckScalarUnary("nanosecond", unit, times, int64(), nanosecond);

  //  CheckScalarUnary("year", unit, times, int32(), year);
  //  CheckScalarUnary("month", unit, times, uint32(), month);
  //  CheckScalarUnary("day", unit, times, uint32(), day);
  //  CheckScalarUnary("day_of_week", unit, times, uint8(), day_of_week);
  //  CheckScalarUnary("day_of_year", unit, times, uint16(), day_of_year);
  //  CheckScalarUnary("week", unit, times, uint8(), week);
  //  CheckScalarUnary("quarter", unit, times, uint32(), quarter);
  //  CheckScalarUnary("hour", unit, times, uint8(), hour);
  //  CheckScalarUnary("minute", unit, times, uint8(), minute);
  //  CheckScalarUnary("second", unit, times, uint8(), second);
  //  CheckScalarUnary("millisecond", unit, times, uint16(), millisecond);
  //  CheckScalarUnary("microsecond", unit, times, uint16(), microsecond);
  //  CheckScalarUnary("nanosecond", unit, times, uint16(), nanosecond);
}
}  // namespace compute
}  // namespace arrow
