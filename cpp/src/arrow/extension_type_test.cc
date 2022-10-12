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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>

#include <gtest/gtest.h>

#include "arrow/array/array_nested.h"
#include "arrow/array/util.h"
#include "arrow/extension_type.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"

namespace arrow {

class Parametric1Array : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

class Parametric2Array : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

// A parametric type where the extension_name() is always the same
class Parametric1Type : public ExtensionType {
 public:
  explicit Parametric1Type(int32_t parameter)
      : ExtensionType(int32()), parameter_(parameter) {}

  int32_t parameter() const { return parameter_; }

  std::string extension_name() const override { return "parametric-type-1"; }

  bool ExtensionEquals(const ExtensionType& other) const override {
    const auto& other_ext = static_cast<const ExtensionType&>(other);
    if (other_ext.extension_name() != this->extension_name()) {
      return false;
    }
    return this->parameter() == static_cast<const Parametric1Type&>(other).parameter();
  }

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override {
    return std::make_shared<Parametric1Array>(data);
  }

  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized) const override {
    DCHECK_EQ(4, serialized.size());
    const int32_t parameter = *reinterpret_cast<const int32_t*>(serialized.data());
    DCHECK(storage_type->Equals(int32()));
    return std::make_shared<Parametric1Type>(parameter);
  }

  std::string Serialize() const override {
    std::string result("    ");
    memcpy(&result[0], &parameter_, sizeof(int32_t));
    return result;
  }

 private:
  int32_t parameter_;
};

// A parametric type where the extension_name() is different for each
// parameter, and must be separately registered
class Parametric2Type : public ExtensionType {
 public:
  explicit Parametric2Type(int32_t parameter)
      : ExtensionType(int32()), parameter_(parameter) {}

  int32_t parameter() const { return parameter_; }

  std::string extension_name() const override {
    std::stringstream ss;
    ss << "parametric-type-2<param=" << parameter_ << ">";
    return ss.str();
  }

  bool ExtensionEquals(const ExtensionType& other) const override {
    const auto& other_ext = static_cast<const ExtensionType&>(other);
    if (other_ext.extension_name() != this->extension_name()) {
      return false;
    }
    return this->parameter() == static_cast<const Parametric2Type&>(other).parameter();
  }

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override {
    return std::make_shared<Parametric2Array>(data);
  }

  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized) const override {
    DCHECK_EQ(4, serialized.size());
    const int32_t parameter = *reinterpret_cast<const int32_t*>(serialized.data());
    DCHECK(storage_type->Equals(int32()));
    return std::make_shared<Parametric2Type>(parameter);
  }

  std::string Serialize() const override {
    std::string result("    ");
    memcpy(&result[0], &parameter_, sizeof(int32_t));
    return result;
  }

 private:
  int32_t parameter_;
};

// An extension type with a non-primitive storage type
class ExtStructArray : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

class ExtStructType : public ExtensionType {
 public:
  ExtStructType()
      : ExtensionType(
            struct_({::arrow::field("a", int64()), ::arrow::field("b", float64())})) {}

  std::string extension_name() const override { return "ext-struct-type"; }

  bool ExtensionEquals(const ExtensionType& other) const override {
    const auto& other_ext = static_cast<const ExtensionType&>(other);
    if (other_ext.extension_name() != this->extension_name()) {
      return false;
    }
    return true;
  }

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override {
    return std::make_shared<ExtStructArray>(data);
  }

  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized) const override {
    if (serialized != "ext-struct-type-unique-code") {
      return Status::Invalid("Type identifier did not match");
    }
    return std::make_shared<ExtStructType>();
  }

  std::string Serialize() const override { return "ext-struct-type-unique-code"; }
};

class TestExtensionType : public ::testing::Test {
 public:
  void SetUp() { ASSERT_OK(RegisterExtensionType(std::make_shared<UuidType>())); }

  void TearDown() {
    if (GetExtensionType("uuid")) {
      ASSERT_OK(UnregisterExtensionType("uuid"));
    }
  }
};

TEST_F(TestExtensionType, ExtensionTypeTest) {
  auto type_not_exist = GetExtensionType("uuid-unknown");
  ASSERT_EQ(type_not_exist, nullptr);

  auto registered_type = GetExtensionType("uuid");
  ASSERT_NE(registered_type, nullptr);

  auto type = uuid();
  ASSERT_EQ(type->id(), Type::EXTENSION);

  const auto& ext_type = static_cast<const ExtensionType&>(*type);
  std::string serialized = ext_type.Serialize();

  ASSERT_OK_AND_ASSIGN(auto deserialized,
                       ext_type.Deserialize(fixed_size_binary(16), serialized));
  ASSERT_TRUE(deserialized->Equals(*type));
  ASSERT_FALSE(deserialized->Equals(*fixed_size_binary(16)));
}

auto RoundtripBatch = [](const std::shared_ptr<RecordBatch>& batch,
                         std::shared_ptr<RecordBatch>* out) {
  ASSERT_OK_AND_ASSIGN(auto out_stream, io::BufferOutputStream::Create());
  ASSERT_OK(ipc::WriteRecordBatchStream({batch}, ipc::IpcWriteOptions::Defaults(),
                                        out_stream.get()));

  ASSERT_OK_AND_ASSIGN(auto complete_ipc_stream, out_stream->Finish());

  io::BufferReader reader(complete_ipc_stream);
  std::shared_ptr<RecordBatchReader> batch_reader;
  ASSERT_OK_AND_ASSIGN(batch_reader, ipc::RecordBatchStreamReader::Open(&reader));
  ASSERT_OK(batch_reader->ReadNext(out));
};

TEST_F(TestExtensionType, IpcRoundtrip) {
  auto ext_arr = ExampleUuid();
  auto batch = RecordBatch::Make(schema({field("f0", uuid())}), 4, {ext_arr});

  std::shared_ptr<RecordBatch> read_batch;
  RoundtripBatch(batch, &read_batch);
  CompareBatch(*batch, *read_batch, false /* compare_metadata */);

  // Wrap type in a ListArray and ensure it also makes it
  auto offsets_arr = ArrayFromJSON(int32(), "[0, 0, 2, 4]");
  ASSERT_OK_AND_ASSIGN(auto list_arr, ListArray::FromArrays(*offsets_arr, *ext_arr));
  batch = RecordBatch::Make(schema({field("f0", list(uuid()))}), 3, {list_arr});
  RoundtripBatch(batch, &read_batch);
  CompareBatch(*batch, *read_batch, false /* compare_metadata */);
}

TEST_F(TestExtensionType, UnrecognizedExtension) {
  auto ext_arr = ExampleUuid();
  auto batch = RecordBatch::Make(schema({field("f0", uuid())}), 4, {ext_arr});

  auto storage_arr = static_cast<const ExtensionArray&>(*ext_arr).storage();

  // Write full IPC stream including schema, then unregister type, then read
  // and ensure that a plain instance of the storage type is created
  ASSERT_OK_AND_ASSIGN(auto out_stream, io::BufferOutputStream::Create());
  ASSERT_OK(ipc::WriteRecordBatchStream({batch}, ipc::IpcWriteOptions::Defaults(),
                                        out_stream.get()));

  ASSERT_OK_AND_ASSIGN(auto complete_ipc_stream, out_stream->Finish());

  ASSERT_OK(UnregisterExtensionType("uuid"));
  auto ext_metadata =
      key_value_metadata({{"ARROW:extension:name", "uuid"},
                          {"ARROW:extension:metadata", "uuid-serialized"}});
  auto ext_field = field("f0", fixed_size_binary(16), true, ext_metadata);
  auto batch_no_ext = RecordBatch::Make(schema({ext_field}), 4, {storage_arr});

  io::BufferReader reader(complete_ipc_stream);
  std::shared_ptr<RecordBatchReader> batch_reader;
  ASSERT_OK_AND_ASSIGN(batch_reader, ipc::RecordBatchStreamReader::Open(&reader));
  std::shared_ptr<RecordBatch> read_batch;
  ASSERT_OK(batch_reader->ReadNext(&read_batch));
  CompareBatch(*batch_no_ext, *read_batch);
}

std::shared_ptr<Array> ExampleParametric(std::shared_ptr<DataType> type,
                                         const std::string& json_data) {
  auto arr = ArrayFromJSON(int32(), json_data);
  auto ext_data = arr->data()->Copy();
  ext_data->type = type;
  return MakeArray(ext_data);
}

TEST_F(TestExtensionType, ParametricTypes) {
  auto p1_type = std::make_shared<Parametric1Type>(6);
  auto p1 = ExampleParametric(p1_type, "[null, 1, 2, 3]");

  auto p2_type = std::make_shared<Parametric1Type>(12);
  auto p2 = ExampleParametric(p2_type, "[2, null, 3, 4]");

  auto p3_type = std::make_shared<Parametric2Type>(2);
  auto p3 = ExampleParametric(p3_type, "[5, 6, 7, 8]");

  auto p4_type = std::make_shared<Parametric2Type>(3);
  auto p4 = ExampleParametric(p4_type, "[5, 6, 7, 9]");

  ASSERT_OK(RegisterExtensionType(std::make_shared<Parametric1Type>(-1)));
  ASSERT_OK(RegisterExtensionType(p3_type));
  ASSERT_OK(RegisterExtensionType(p4_type));

  auto batch = RecordBatch::Make(schema({field("f0", p1_type), field("f1", p2_type),
                                         field("f2", p3_type), field("f3", p4_type)}),
                                 4, {p1, p2, p3, p4});

  std::shared_ptr<RecordBatch> read_batch;
  RoundtripBatch(batch, &read_batch);
  CompareBatch(*batch, *read_batch, false /* compare_metadata */);
}

TEST_F(TestExtensionType, ParametricEquals) {
  auto p1_type = std::make_shared<Parametric1Type>(6);
  auto p2_type = std::make_shared<Parametric1Type>(6);
  auto p3_type = std::make_shared<Parametric1Type>(3);

  ASSERT_TRUE(p1_type->Equals(p2_type));
  ASSERT_FALSE(p1_type->Equals(p3_type));

  ASSERT_EQ(p1_type->fingerprint(), "");
}

std::shared_ptr<Array> ExampleStruct() {
  auto ext_type = std::make_shared<ExtStructType>();
  auto storage_type = ext_type->storage_type();
  auto arr = ArrayFromJSON(storage_type, "[[1, 0.1], [2, 0.2]]");

  auto ext_data = arr->data()->Copy();
  ext_data->type = ext_type;
  return MakeArray(ext_data);
}

TEST_F(TestExtensionType, ValidateExtensionArray) {
  auto ext_arr1 = ExampleUuid();
  auto p1_type = std::make_shared<Parametric1Type>(6);
  auto ext_arr2 = ExampleParametric(p1_type, "[null, 1, 2, 3]");
  auto ext_arr3 = ExampleStruct();
  auto ext_arr4 = ExampleComplex128();

  ASSERT_OK(ext_arr1->ValidateFull());
  ASSERT_OK(ext_arr2->ValidateFull());
  ASSERT_OK(ext_arr3->ValidateFull());
  ASSERT_OK(ext_arr4->ValidateFull());
}

class GeoRasterArray : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

// This attempts to model GDAL Raster Band
// https://gdal.org/user/raster_data_model.html#raster-band
template <typename DType>
class GeoRasterType : public ExtensionType {
  using T = typename DType::c_type;

 public:
  enum BandColor {
    /// the default, nothing is known.
    GCI_Undefined = 0,
    /// independent gray-scale image
    GCI_GrayIndex = 1,
    /// acts as an index into a color table
    GCI_PaletteIndex = 2,
    /// red portion of an RGB or RGBA image
    GCI_RedBand = 3,
    /// green portion of an RGB or RGBA image
    GCI_GreenBand = 4,
    /// blue portion of an RGB or RGBA image
    GCI_BlueBand = 5,
    /// alpha portion of an RGBA image
    GCI_AlphaBand = 6,
    /// hue of an HLS image
    GCI_HueBand = 7,
    /// saturation of an HLS image
    GCI_SaturationBand = 8,
    /// hue of an HLS image
    GCI_LightnessBand = 9,
    /// cyan portion of a CMY or CMYK image
    GCI_CyanBand = 10,
    /// magenta portion of a CMY or CMYK image
    GCI_MagentaBand = 11,
    /// yellow portion of a CMY or CMYK image
    GCI_YellowBand = 12,
    /// black portion of a CMYK image.
    GCI_BlackBand = 13,
  };

  explicit GeoRasterType(
      const std::shared_ptr<DataType>& type, const uint64_t width, const uint64_t height,
      const std::string metadata = "", const std::string description = "",
      const T nodata_value = std::numeric_limits<T>::min(),
      const std::map<T, std::string> category_names = std::map<T, std::string>(),
      const double minimum = std::numeric_limits<T>::min(),
      const double maximum = std::numeric_limits<T>::max(), const T offset = 0,
      const double scale = 1, const std::string unit = "",
      const BandColor band_color = GCI_Undefined)
      : ExtensionType(fixed_size_list(type, static_cast<int>(width * height))),
        type_(type),
        width_(width),
        height_(height),
        metadata_(metadata),
        description_(description),
        nodata_value_(nodata_value),
        category_names_(category_names),
        minimum_(minimum),
        maximum_(maximum),
        offset_(offset),
        scale_(scale),
        unit_(unit),
        band_color_(band_color) {}

  std::string extension_name() const override {
    return "ARROW:extension:name: geoarrow.raster";
  }

  bool ExtensionEquals(const ExtensionType& other) const override {
    const auto& other_ext = static_cast<const GeoRasterType&>(other);
    if (other_ext.type_ == this->type_ && other_ext.width_ == this->width_ &&
        other_ext.height_ == this->height_ && other_ext.metadata_ == this->metadata_ &&
        other_ext.description_ == this->description_ &&
        other_ext.nodata_value_ == this->nodata_value_ &&
        other_ext.category_names_ == this->category_names_ &&
        other_ext.minimum_ == this->minimum_ && other_ext.maximum_ == this->maximum_ &&
        other_ext.offset_ == this->offset_ && other_ext.scale_ == this->scale_ &&
        other_ext.unit_ == this->unit_ && other_ext.band_color_ == this->band_color_) {
      return true;
    }
    return false;
  }

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override {
    return std::make_shared<GeoRasterArray>(data);
  }

  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized) const override {
    if (serialized != extension_name()) {
      return Status::Invalid("Type identifier did not match");
    }
    return std::make_shared<GeoRasterType>(type_, width_, height_);
  }

  std::shared_ptr<DataType> type() const { return type_; }

  std::string Serialize() const override { return extension_name(); }

 private:
  // Byte, UInt16, Int16, UInt32, Int32, UInt64, Int64, Float32, Float64, CInt16, CInt32,
  // CFloat32, and CFloat64.
  const std::shared_ptr<DataType>& type_;

  // Height and width and of raster band.
  const uint64_t width_;
  const uint64_t height_;

  // A list of name/value pair metadata in the same format as the dataset, but of
  // information that is potentially specific to this band.
  // Optional statistics stored in metadata:
  // * STATISTICS_MEAN: mean
  // * STATISTICS_MINIMUM: minimum
  // * STATISTICS_MAXIMUM: maximum
  // * STATISTICS_STDDEV: standard deviation
  // * STATISTICS_APPROXIMATE: only present if GDAL has computed approximate statistics
  // * STATISTICS_VALID_PERCENT: percentage of valid (not nodata) pixel
  const std::string metadata_;

  // An optional description string.
  const std::string description_;

  // An optional single nodata pixel value (see also NODATA_VALUES metadata on the dataset
  // for multi-band style nodata values).
  const T nodata_value_;

  // An optional list of category names (effectively class names in a thematic image).
  const std::map<T, std::string> category_names_;

  // An optional minimum and maximum value.
  const double minimum_, maximum_;

  // An optional offset and scale for transforming raster values into meaning full values
  // (i.e. translate height to meters).
  // Units value = (raw value * scale) + offset
  const T offset_;
  const double scale_;

  // An optional raster unit name. For instance, this might indicate linear units for
  // elevation data.
  const std::string unit_;

  // Color interpretation for the band.
  const BandColor band_color_;
};

TEST_F(TestExtensionType, GeoRasterType) {
  std::vector<int64_t> values = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                                 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                                 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35};
  int width = 3;
  int height = 4;
  auto band_size = width * height;
  int length = static_cast<int>(values.size() / band_size);

  auto storage_type = fixed_size_list(int64(), band_size);
  auto ext_type = std::make_shared<GeoRasterType<Int64Type>>(int64(), width, height);

  auto data = ArrayData::Make(int64(), values.size(), {nullptr, Buffer::Wrap(values)});
  auto storage_arr =
      std::make_shared<FixedSizeListArray>(storage_type, length, MakeArray(data));
  auto raster_arr = std::make_shared<GeoRasterArray>(ext_type, storage_arr);
}

}  // namespace arrow
