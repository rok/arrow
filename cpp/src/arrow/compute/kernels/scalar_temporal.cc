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

#include "arrow/builder.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/time.h"
#include "arrow/vendored/datetime.h"

namespace arrow {

namespace compute {
namespace internal {

using applicator::ScalarUnaryNotNull;
using applicator::SimpleUnary;
using arrow_vendored::date::days;
using arrow_vendored::date::floor;
using arrow_vendored::date::hh_mm_ss;
using arrow_vendored::date::sys_days;
using arrow_vendored::date::sys_time;
using arrow_vendored::date::trunc;
using arrow_vendored::date::weekday;
using arrow_vendored::date::weeks;
using arrow_vendored::date::year_month_day;
using arrow_vendored::date::years;
using arrow_vendored::date::literals::dec;
using arrow_vendored::date::literals::jan;
using arrow_vendored::date::literals::last;
using arrow_vendored::date::literals::mon;
using arrow_vendored::date::literals::thu;

// Based on ScalarUnaryNotNullStateful. Adds timezone awareness.
template <typename OutType, typename Op>
struct ScalarUnaryStatefulTemporal {
  using ThisType = ScalarUnaryStatefulTemporal<OutType, Op>;
  using OutValue = typename GetOutputType<OutType>::T;

  Op op;
  explicit ScalarUnaryStatefulTemporal(Op op) : op(std::move(op)) {}

  template <typename Type>
  struct ArrayExec {
    static Status Exec(const ThisType& functor, KernelContext* ctx, const ArrayData& arg0,
                       Datum* out) {
      const std::string timezone =
          std::static_pointer_cast<const TimestampType>(arg0.type)->timezone();
      Status st = Status::OK();
      ArrayData* out_arr = out->mutable_array();
      auto out_data = out_arr->GetMutableValues<OutValue>(1);

      if (timezone.empty()) {
        VisitArrayValuesInline<Int64Type>(
            arg0,
            [&](int64_t v) {
              *out_data++ = functor.op.template Call<OutValue>(ctx, v, &st);
            },
            [&]() {
              // null
              ++out_data;
            });
      } else {
        st = Status::Invalid("Timezone aware timestamps not supported. Timezone found: ",
                             timezone);
      }
      return st;
    }
  };

  Status Scalar(KernelContext* ctx, const Scalar& arg0, Datum* out) {
    const std::string timezone =
        std::static_pointer_cast<const TimestampType>(arg0.type)->timezone();
    Status st = Status::OK();
    if (timezone.empty()) {
      if (arg0.is_valid) {
        int64_t arg0_val = UnboxScalar<Int64Type>::Unbox(arg0);
        BoxScalar<OutType>::Box(this->op.template Call<OutValue>(ctx, arg0_val, &st),
                                out->scalar().get());
      }
    } else {
      st = Status::Invalid("Timezone aware timestamps not supported. Timezone found: ",
                           timezone);
    }
    return st;
  }

  Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].kind() == Datum::ARRAY) {
      return ArrayExec<OutType>::Exec(*this, ctx, *batch[0].array(), out);
    } else {
      return Scalar(ctx, *batch[0].scalar(), out);
    }
  }
};

template <typename OutType, typename Op>
struct ScalarUnaryTemporal {
  using OutValue = typename GetOutputType<OutType>::T;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // Seed kernel with dummy state
    ScalarUnaryStatefulTemporal<OutType, Op> kernel({});
    return kernel.Exec(ctx, batch, out);
  }
};

// ----------------------------------------------------------------------
// Extract year from timestamp

template <typename Duration>
struct Year {
  template <typename T>
  static T Call(KernelContext*, int64_t arg, Status*) {
    return static_cast<T>(static_cast<const int32_t>(
        year_month_day(floor<days>(sys_time<Duration>(Duration{arg}))).year()));
  }
};

// ----------------------------------------------------------------------
// Extract month from timestamp

template <typename Duration>
struct Month {
  template <typename T>
  static T Call(KernelContext*, int64_t arg, Status*) {
    return static_cast<T>(static_cast<const uint32_t>(
        year_month_day(floor<days>(sys_time<Duration>(Duration{arg}))).month()));
  }
};

// ----------------------------------------------------------------------
// Extract day from timestamp

template <typename Duration>
struct Day {
  template <typename T>
  static T Call(KernelContext*, int64_t arg, Status*) {
    return static_cast<T>(static_cast<const uint32_t>(
        year_month_day(floor<days>(sys_time<Duration>(Duration{arg}))).day()));
  }
};

// ----------------------------------------------------------------------
// Extract day of week from timestamp

template <typename Duration>
struct DayOfWeek {
  template <typename T>
  static T Call(KernelContext*, int64_t arg, Status*) {
    return static_cast<T>(
        weekday(year_month_day(floor<days>(sys_time<Duration>(Duration{arg}))))
            .iso_encoding());
  }
};

// ----------------------------------------------------------------------
// Extract day of year from timestamp

template <typename Duration>
struct DayOfYear {
  template <typename T>
  static T Call(KernelContext*, int64_t arg, Status*) {
    const auto sd = sys_days{floor<days>(Duration{arg})};
    return static_cast<T>((sd - sys_days(year_month_day(sd).year() / jan / 0)).count());
  }
};

// ----------------------------------------------------------------------
// Extract ISO Year values from timestamp

template <typename Duration>
struct ISOYear {
  template <typename T>
  static T Call(KernelContext*, int64_t arg, Status*) {
    return static_cast<T>(static_cast<const int32_t>(
        year_month_day{sys_days{floor<days>(Duration{arg})} + days{3}}.year()));
  }
};

// ----------------------------------------------------------------------
// Extract ISO week from timestamp

// Based on
// https://github.com/HowardHinnant/date/blob/6e921e1b1d21e84a5c82416ba7ecd98e33a436d0/include/date/iso_week.h#L1503
template <typename Duration>
struct ISOWeek {
  template <typename T>
  static T Call(KernelContext*, int64_t arg, Status*) {
    const auto dp = sys_days{floor<days>(Duration{arg})};
    auto y = year_month_day{dp + days{3}}.year();
    auto start = sys_days((y - years{1}) / dec / thu[last]) + (mon - thu);
    if (dp < start) {
      --y;
      start = sys_days((y - years{1}) / dec / thu[last]) + (mon - thu);
    }
    return static_cast<T>(trunc<weeks>(dp - start).count() + 1);
  }
};

// ----------------------------------------------------------------------
// Extract day of quarter from timestamp

template <typename Duration>
struct Quarter {
  template <typename T>
  static T Call(KernelContext*, int64_t arg, Status*) {
    const auto ymd = year_month_day(floor<days>(sys_time<Duration>(Duration{arg})));
    return static_cast<T>((static_cast<const uint32_t>(ymd.month()) - 1) / 3 + 1);
  }
};

// ----------------------------------------------------------------------
// Extract hour from timestamp

template <typename Duration>
struct Hour {
  template <typename T>
  static T Call(KernelContext*, int64_t arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>((t - floor<days>(t)) / std::chrono::hours(1));
  }
};

// ----------------------------------------------------------------------
// Extract minute from timestamp

template <typename Duration>
struct Minute {
  template <typename T>
  static T Call(KernelContext*, int64_t arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>((t - floor<std::chrono::hours>(t)) / std::chrono::minutes(1));
  }
};

// ----------------------------------------------------------------------
// Extract second from timestamp

template <typename Duration>
struct Second {
  template <typename T>
  static T Call(KernelContext*, int64_t arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>(
        std::chrono::duration<double>(t - floor<std::chrono::minutes>(t)).count());
  }
};

// ----------------------------------------------------------------------
// Extract subsecond from timestamp

template <typename Duration>
struct Subsecond {
  template <typename T>
  static T Call(KernelContext*, int64_t arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>((t - floor<std::chrono::seconds>(t)) /
                          std::chrono::nanoseconds(1));
  }
};

// ----------------------------------------------------------------------
// Extract milliseconds from timestamp

template <typename Duration>
struct Millisecond {
  template <typename T>
  static T Call(KernelContext*, int64_t arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>(
        ((t - floor<std::chrono::seconds>(t)) / std::chrono::milliseconds(1)) % 1000);
  }
};

// ----------------------------------------------------------------------
// Extract microseconds from timestamp

template <typename Duration>
struct Microsecond {
  template <typename T>
  static T Call(KernelContext*, int64_t arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>(
        ((t - floor<std::chrono::seconds>(t)) / std::chrono::microseconds(1)) % 1000);
  }
};

// ----------------------------------------------------------------------
// Extract nanoseconds from timestamp

template <typename Duration>
struct Nanosecond {
  template <typename T>
  static T Call(KernelContext*, int64_t arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>(
        ((t - floor<std::chrono::seconds>(t)) / std::chrono::nanoseconds(1)) % 1000);
  }
};

// ----------------------------------------------------------------------
// Extract ISO calendar values from timestamp

template <typename Duration, typename OutType>
struct ISOCalendar {
  using T = typename OutType::c_type;

  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    using ScalarType = typename TypeTraits<OutType>::ScalarType;
    const auto& out_type = TypeTraits<OutType>::type_singleton();

    const std::string timezone =
        std::static_pointer_cast<const TimestampType>(in.type)->timezone();
    if (!timezone.empty()) {
      return Status::Invalid("Timezone aware timestamps not supported. Timezone found: ",
                             timezone);
    }

    if (!in.is_valid) {
      out->is_valid = false;
    } else {
      const std::shared_ptr<DataType> iso_calendar_type =
          struct_({field("iso_year", out_type), field("iso_week", out_type),
                   field("weekday", out_type)});

      const auto& in_val = internal::UnboxScalar<const TimestampType>::Unbox(in);
      const auto dp = sys_days{floor<days>(Duration{in_val})};
      const auto ymd = year_month_day(dp);
      auto y = year_month_day{dp + days{3}}.year();
      auto start = sys_days((y - years{1}) / dec / thu[last]) + (mon - thu);
      if (dp < start) {
        --y;
        start = sys_days((y - years{1}) / dec / thu[last]) + (mon - thu);
      }

      std::vector<std::shared_ptr<Scalar>> values = {
          std::make_shared<ScalarType>(static_cast<T>(static_cast<int32_t>(ymd.year()))),
          std::make_shared<ScalarType>(
              static_cast<T>(trunc<weeks>(dp - start).count() + 1)),
          std::make_shared<ScalarType>(static_cast<T>(weekday(ymd).iso_encoding()))};
      *checked_cast<StructScalar*>(out) = StructScalar(values, iso_calendar_type);
    }
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    using BuilderType = typename TypeTraits<OutType>::BuilderType;
    const auto& out_type = TypeTraits<OutType>::type_singleton();

    const std::string timezone =
        std::static_pointer_cast<const TimestampType>(in.type)->timezone();
    if (!timezone.empty()) {
      return Status::Invalid("Timezone aware timestamps not supported. Timezone found: ",
                             timezone);
    }

    const std::shared_ptr<DataType> iso_calendar_type =
        struct_({field("iso_year", out_type), field("iso_week", out_type),
                 field("weekday", out_type)});

    std::unique_ptr<ArrayBuilder> array_builder;
    RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), iso_calendar_type, &array_builder));
    StructBuilder* struct_builder = checked_cast<StructBuilder*>(array_builder.get());

    std::vector<BuilderType*> field_builders;
    field_builders.reserve(3);
    for (int i = 0; i < 3; i++) {
      field_builders.push_back(
          checked_cast<BuilderType*>(struct_builder->field_builder(i)));
    }
    auto visit_null = [&]() { return struct_builder->AppendNull(); };
    auto visit_value = [&](int64_t arg) {
      const auto dp = sys_days{floor<days>(Duration{arg})};
      const auto ymd = year_month_day(dp);
      auto y = year_month_day{dp + days{3}}.year();
      auto start = sys_days((y - years{1}) / dec / thu[last]) + (mon - thu);
      if (dp < start) {
        --y;
        start = sys_days((y - years{1}) / dec / thu[last]) + (mon - thu);
      }

      RETURN_NOT_OK(
          field_builders[0]->Append(static_cast<T>(static_cast<int32_t>(ymd.year()))));
      RETURN_NOT_OK(field_builders[1]->Append(
          static_cast<T>(trunc<weeks>(dp - start).count() + 1)));
      RETURN_NOT_OK(
          field_builders[2]->Append(static_cast<T>(weekday(ymd).iso_encoding())));

      return struct_builder->Append();
    };
    RETURN_NOT_OK(VisitArrayDataInline<OutType>(in, visit_value, visit_null));

    std::shared_ptr<Array> out_array;
    RETURN_NOT_OK(struct_builder->Finish(&out_array));
    *out = *std::move(out_array->data());

    return Status::OK();
  }
};

// Generate a kernel given an arithmetic functor
template <template <typename... Args> class KernelGenerator,
          template <typename... Args> class Op, typename Duration>
ArrayKernelExec ExecFromOp(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT8:
      return KernelGenerator<Int8Type, Op<Duration>>::Exec;
    case Type::UINT8:
      return KernelGenerator<UInt8Type, Op<Duration>>::Exec;
    case Type::INT16:
      return KernelGenerator<Int16Type, Op<Duration>>::Exec;
    case Type::UINT16:
      return KernelGenerator<UInt16Type, Op<Duration>>::Exec;
    case Type::INT32:
      return KernelGenerator<Int32Type, Op<Duration>>::Exec;
    case Type::UINT32:
      return KernelGenerator<UInt32Type, Op<Duration>>::Exec;
    case Type::INT64:
    case Type::TIMESTAMP:
      return KernelGenerator<Int64Type, Op<Duration>>::Exec;
    case Type::UINT64:
      return KernelGenerator<UInt64Type, Op<Duration>>::Exec;
    case Type::FLOAT:
      return KernelGenerator<FloatType, Op<Duration>>::Exec;
    case Type::DOUBLE:
      return KernelGenerator<DoubleType, Op<Duration>>::Exec;
    default:
      DCHECK(false);
      return ExecFail;
  }
}

template <template <typename...> class Op>
std::shared_ptr<ScalarFunction> MakeTemporalFunction(
    std::string name, const FunctionDoc* doc,
    std::vector<std::shared_ptr<DataType>> types) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);

  for (auto ty : types) {
    for (auto unit : AllTimeUnits()) {
      ArrayKernelExec exec;
      switch (unit) {
        case TimeUnit::SECOND: {
          exec = ExecFromOp<ScalarUnaryTemporal, Op, std::chrono::seconds>(ty);
        }
        case TimeUnit::MILLI: {
          exec = ExecFromOp<ScalarUnaryTemporal, Op, std::chrono::milliseconds>(ty);
        }
        case TimeUnit::MICRO: {
          exec = ExecFromOp<ScalarUnaryTemporal, Op, std::chrono::microseconds>(ty);
        }
        case TimeUnit::NANO: {
          exec = ExecFromOp<ScalarUnaryTemporal, Op, std::chrono::nanoseconds>(ty);
        }
      }
      ScalarKernel kernel =
          ScalarKernel({match::TimestampTypeUnit(unit)}, OutputType(ty), exec);
      DCHECK_OK(func->AddKernel(kernel));
    }
  }
  return func;
}

// Generate a kernel given an arithmetic functor
template <template <typename... Args> class Op, typename Duration>
ArrayKernelExec SimpleUnaryFromOp(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT32:
      return SimpleUnary<Op<Duration, Int32Type>>;
    case Type::INT64:
      return SimpleUnary<Op<Duration, Int64Type>>;
    default:
      DCHECK(false);
      return ExecFail;
  }
}

template <template <typename...> class Op>
std::shared_ptr<ScalarFunction> MakeSimpleUnaryTemporalFunction(
    std::string name, const FunctionDoc* doc,
    std::vector<std::shared_ptr<DataType>> types) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);

  for (auto ty : types) {
    for (auto unit : AllTimeUnits()) {
      ArrayKernelExec exec;
      switch (unit) {
        case TimeUnit::SECOND: {
          exec = SimpleUnaryFromOp<Op, std::chrono::seconds>(ty);
        }
        case TimeUnit::MILLI: {
          exec = SimpleUnaryFromOp<Op, std::chrono::milliseconds>(ty);
        }
        case TimeUnit::MICRO: {
          exec = SimpleUnaryFromOp<Op, std::chrono::microseconds>(ty);
        }
        case TimeUnit::NANO: {
          exec = SimpleUnaryFromOp<Op, std::chrono::nanoseconds>(ty);
        }
      }
      auto output_type =
          struct_({field("iso_year", ty), field("iso_week", ty), field("weekday", ty)});
      ScalarKernel kernel =
          ScalarKernel({match::TimestampTypeUnit(unit)}, OutputType(output_type), exec);
      DCHECK_OK(func->AddKernel(kernel));
    }
  }
  return func;
}

const FunctionDoc year_doc{"Extract year values", "", {"values"}};
const FunctionDoc month_doc{"Extract month values", "", {"values"}};
const FunctionDoc day_doc{"Extract day values", "", {"values"}};
const FunctionDoc day_of_week_doc{"Extract day of week values", "", {"values"}};
const FunctionDoc day_of_year_doc{"Extract day of year values", "", {"values"}};
const FunctionDoc iso_year_doc{"Extract ISO year values", "", {"values"}};
const FunctionDoc iso_week_doc{"Extract ISO week values", "", {"values"}};
const FunctionDoc iso_calendar_doc{"Extract ISO calendar values", "", {"values"}};
const FunctionDoc quarter_doc{"Extract quarter values", "", {"values"}};
const FunctionDoc hour_doc{"Extract hour values", "", {"values"}};
const FunctionDoc minute_doc{"Extract minute values", "", {"values"}};
const FunctionDoc second_doc{"Extract second values", "", {"values"}};
const FunctionDoc millisecond_doc{"Extract millisecond values", "", {"values"}};
const FunctionDoc microsecond_doc{"Extract microsecond values", "", {"values"}};
const FunctionDoc nanosecond_doc{"Extract nanosecond values", "", {"values"}};
const FunctionDoc subsecond_doc{"Extract subsecond values", "", {"values"}};

void RegisterScalarTemporal(FunctionRegistry* registry) {
  static std::vector<std::shared_ptr<DataType>> kUnsignedFloatTypes8 = {
      uint8(), int8(),   uint16(), int16(),   uint32(),
      int32(), uint64(), int64(),  float32(), float64()};
  static std::vector<std::shared_ptr<DataType>> kUnsignedIntegerTypes8 = {
      uint8(), int8(), uint16(), int16(), uint32(), int32(), uint64(), int64()};
  static std::vector<std::shared_ptr<DataType>> kUnsignedIntegerTypes16 = {
      uint16(), int16(), uint32(), int32(), uint64(), int64()};
  static std::vector<std::shared_ptr<DataType>> kUnsignedIntegerTypes32 = {
      uint32(), int32(), uint64(), int64()};
  static std::vector<std::shared_ptr<DataType>> kSignedIntegerTypes = {int32(), int64()};

  auto year = MakeTemporalFunction<Year>("year", &year_doc, kSignedIntegerTypes);
  DCHECK_OK(registry->AddFunction(std::move(year)));

  auto month = MakeTemporalFunction<Month>("month", &year_doc, kUnsignedIntegerTypes32);
  DCHECK_OK(registry->AddFunction(std::move(month)));

  auto day = MakeTemporalFunction<Day>("day", &year_doc, kUnsignedIntegerTypes32);
  DCHECK_OK(registry->AddFunction(std::move(day)));

  auto day_of_week = MakeTemporalFunction<DayOfWeek>("day_of_week", &day_of_week_doc,
                                                     kUnsignedIntegerTypes8);
  DCHECK_OK(registry->AddFunction(std::move(day_of_week)));

  auto day_of_year = MakeTemporalFunction<DayOfYear>("day_of_year", &day_of_year_doc,
                                                     kUnsignedIntegerTypes16);
  DCHECK_OK(registry->AddFunction(std::move(day_of_year)));

  auto iso_year =
      MakeTemporalFunction<ISOYear>("iso_year", &iso_year_doc, kSignedIntegerTypes);
  DCHECK_OK(registry->AddFunction(std::move(iso_year)));

  auto iso_week =
      MakeTemporalFunction<ISOWeek>("iso_week", &iso_week_doc, kUnsignedIntegerTypes8);
  DCHECK_OK(registry->AddFunction(std::move(iso_week)));

  auto iso_calendar = MakeSimpleUnaryTemporalFunction<ISOCalendar>(
      "iso_calendar", &iso_calendar_doc, kSignedIntegerTypes);
  DCHECK_OK(registry->AddFunction(std::move(iso_calendar)));

  auto quarter =
      MakeTemporalFunction<Quarter>("quarter", &quarter_doc, kUnsignedIntegerTypes32);
  DCHECK_OK(registry->AddFunction(std::move(quarter)));

  auto hour = MakeTemporalFunction<Hour>("hour", &hour_doc, kUnsignedIntegerTypes8);
  DCHECK_OK(registry->AddFunction(std::move(hour)));

  auto minute =
      MakeTemporalFunction<Minute>("minute", &minute_doc, kUnsignedIntegerTypes8);
  DCHECK_OK(registry->AddFunction(std::move(minute)));

  auto second = MakeTemporalFunction<Second>("second", &second_doc, kUnsignedFloatTypes8);
  DCHECK_OK(registry->AddFunction(std::move(second)));

  auto millisecond = MakeTemporalFunction<Millisecond>("millisecond", &millisecond_doc,
                                                       kUnsignedIntegerTypes16);
  DCHECK_OK(registry->AddFunction(std::move(millisecond)));

  auto microsecond = MakeTemporalFunction<Microsecond>("microsecond", &microsecond_doc,
                                                       kUnsignedIntegerTypes16);
  DCHECK_OK(registry->AddFunction(std::move(microsecond)));

  auto nanosecond = MakeTemporalFunction<Nanosecond>("nanosecond", &nanosecond_doc,
                                                     kUnsignedIntegerTypes16);
  DCHECK_OK(registry->AddFunction(std::move(nanosecond)));
  auto subsecond = MakeTemporalFunction<Subsecond>("subsecond", &subsecond_doc,
                                                   kUnsignedIntegerTypes16);
  DCHECK_OK(registry->AddFunction(std::move(subsecond)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
