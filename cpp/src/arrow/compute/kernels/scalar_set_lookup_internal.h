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

#pragma once

#include <cstdint>
#include <memory>

#include "arrow/compute/kernel.h"
#include "arrow/type_fwd.h"

namespace arrow::compute::internal {

constexpr int64_t kIsInSimplificationMaxValueSet = 50;

/// Common state for the is_in and index_in kernels.
struct SetLookupStateBase : public KernelState {
  std::shared_ptr<DataType> value_set_type;
  bool value_set_has_null = false;

  // Bounds are computed once while binding large is_in expressions. They are optional
  // because min_max is not implemented for every type supported by is_in.
  std::shared_ptr<Scalar> value_set_min;
  std::shared_ptr<Scalar> value_set_max;
};

}  // namespace arrow::compute::internal
