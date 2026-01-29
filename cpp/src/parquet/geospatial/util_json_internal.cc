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

#include "parquet/geospatial/util_json_internal.h"

#include <string>

#include <simdjson.h>

#include "arrow/extension_type.h"
#include "arrow/json/json_util.h"
#include "arrow/result.h"
#include "arrow/util/string.h"

#include "parquet/exception.h"
#include "parquet/types.h"

namespace parquet {

namespace {

::arrow::Result<std::string> GeospatialGeoArrowCrsToParquetCrs(
    simdjson::dom::object document) {
  auto crs_field = document["crs"];
  if (crs_field.error() || crs_field.is_null()) {
    // Parquet GEOMETRY/GEOGRAPHY do not have a concept of a null/missing
    // CRS, but an omitted one is more likely to have meant "lon/lat" than
    // a truly unspecified one (i.e., Engineering CRS with arbitrary XY units)
    return "";
  }

  simdjson::dom::element json_crs = crs_field.value();
  if (json_crs.is_string()) {
    std::string_view crs_str = json_crs.get_string().value();
    if (crs_str == "EPSG:4326" || crs_str == "OGC:CRS84") {
      // crs can be left empty because these cases both correspond to
      // longitude/latitude in WGS84 according to the Parquet specification
      return "";
    }
    return std::string(crs_str);
  } else if (json_crs.is_object()) {
    // Attempt to detect common PROJJSON representations of longitude/latitude and return
    // an empty crs to maximize compatibility with readers that do not implement CRS
    // support. PROJJSON stores this in the "id" member like:
    // {..., "id": {"authority": "...", "code": "..."}}
    auto obj = json_crs.get_object().value();
    auto id_field = obj["id"];
    if (!id_field.error() && id_field.is_object()) {
      auto identifier = id_field.get_object().value();
      auto authority_field = identifier["authority"];
      auto code_field = identifier["code"];
      if (!authority_field.error() && !code_field.error()) {
        if (authority_field.is_string() && code_field.is_string()) {
          std::string_view auth = authority_field.get_string().value();
          std::string_view code = code_field.get_string().value();
          if (auth == "OGC" && code == "CRS84") {
            return "";
          } else if (auth == "EPSG" && code == "4326") {
            return "";
          }
        } else if (authority_field.is_string() && code_field.is_int64()) {
          std::string_view auth = authority_field.get_string().value();
          int64_t code = code_field.get_int64().value();
          if (auth == "EPSG" && code == 4326) {
            return "";
          }
        }
      }
    }
    // Return the JSON object as a string using simdjson's minify
    return std::string(simdjson::minify(json_crs));
  }

  // For any other type, serialize it back to JSON
  return std::string(simdjson::minify(json_crs));
}

// Utility for ensuring that a Parquet CRS is valid JSON when written to
// GeoArrow metadata (without escaping it if it is already valid JSON such as
// a PROJJSON string)
std::string EscapeCrsAsJsonIfRequired(std::string_view crs);

::arrow::Result<std::string> MakeGeoArrowCrsMetadata(
    std::string_view crs,
    const std::shared_ptr<const ::arrow::KeyValueMetadata>& metadata) {
  const std::string kSridPrefix{"srid:"};
  const std::string kProjjsonPrefix{"projjson:"};

  // Two recommendations are explicitly mentioned in the Parquet format for the
  // LogicalType crs:
  //
  // - "srid:XXXX" as a way to encode an application-specific integer identifier
  // - "projjson:some_field_name" as a way to avoid repeating PROJJSON strings
  //   unnecessarily (with a suggestion to place them in the file metadata)
  //
  // While we don't currently generate those values to reduce the complexity
  // of the writer, we do interpret these values according to the suggestion in
  // the format and pass on this information to GeoArrow.
  if (crs.empty()) {
    return R"("crs": "OGC:CRS84", "crs_type": "authority_code")";
  } else if (crs.starts_with(kSridPrefix)) {
    return R"("crs": ")" + std::string(crs.substr(kSridPrefix.size())) +
           R"(", "crs_type": "srid")";
  } else if (crs.starts_with(kProjjsonPrefix)) {
    std::string_view metadata_field = crs.substr(kProjjsonPrefix.size());
    if (metadata && metadata->Contains(metadata_field)) {
      ARROW_ASSIGN_OR_RAISE(std::string projjson_value, metadata->Get(metadata_field));
      // This value should be valid JSON, but if it is not, we escape it as a string such
      // that it can be inspected by the consumer of GeoArrow.
      return R"("crs": )" + EscapeCrsAsJsonIfRequired(projjson_value) +
             R"(, "crs_type": "projjson")";
    }
  }

  // Pass on the string directly to GeoArrow. If the string is already valid JSON,
  // insert it directly into GeoArrow's "crs" field. Otherwise, escape it and pass it as a
  // string value.
  return R"("crs": )" + EscapeCrsAsJsonIfRequired(crs);
}

std::string EscapeCrsAsJsonIfRequired(std::string_view crs) {
  simdjson::dom::parser parser;
  simdjson::padded_string padded_crs(crs);
  auto result = parser.parse(padded_crs);
  if (result.error()) {
    // Not valid JSON, escape it as a JSON string
    return ::arrow::json::EscapeJsonString(crs);
  } else {
    // Already valid JSON, return as-is
    return std::string(crs);
  }
}

}  // namespace

::arrow::Result<std::shared_ptr<const LogicalType>> LogicalTypeFromGeoArrowMetadata(
    std::string_view serialized_data) {
  // Parquet has no way to interpret a null or missing CRS, so we choose the most likely
  // intent here (that the user meant to use the default Parquet CRS)
  if (serialized_data.empty() || serialized_data == "{}") {
    return LogicalType::Geometry();
  }

  simdjson::dom::parser parser;
  simdjson::padded_string padded_json(serialized_data);
  auto result = parser.parse(padded_json);
  if (result.error()) {
    return ::arrow::Status::Invalid("Invalid serialized JSON data: ", serialized_data);
  }

  auto document = result.value();
  if (!document.is_object()) {
    return ::arrow::Status::Invalid("Invalid serialized JSON data: not an object: ",
                                    serialized_data);
  }

  auto obj = document.get_object().value();
  ARROW_ASSIGN_OR_RAISE(std::string crs, GeospatialGeoArrowCrsToParquetCrs(obj));

  auto edges_field = obj["edges"];
  if (!edges_field.error() && edges_field.is_string()) {
    std::string_view edges_value = edges_field.get_string().value();
    if (edges_value == "planar") {
      return LogicalType::Geometry(crs);
    } else if (edges_value == "spherical") {
      return LogicalType::Geography(crs,
                                    LogicalType::EdgeInterpolationAlgorithm::SPHERICAL);
    } else {
      return ::arrow::Status::Invalid("Unsupported GeoArrow edge type: ",
                                      serialized_data);
    }
  } else if (!edges_field.error()) {
    // edges field exists but is not a string
    return ::arrow::Status::Invalid("Unsupported GeoArrow edge type: ", serialized_data);
  }

  return LogicalType::Geometry(crs);
}

::arrow::Result<std::shared_ptr<::arrow::DataType>> GeoArrowTypeFromLogicalType(
    const LogicalType& logical_type,
    const std::shared_ptr<const ::arrow::KeyValueMetadata>& metadata,
    const std::shared_ptr<::arrow::DataType>& storage_type) {
  // Check if we have a registered GeoArrow type to read into
  std::shared_ptr<::arrow::ExtensionType> maybe_geoarrow_wkb =
      ::arrow::GetExtensionType("geoarrow.wkb");
  if (!maybe_geoarrow_wkb) {
    return storage_type;
  }

  if (logical_type.is_geometry()) {
    const auto& geospatial_type =
        ::arrow::internal::checked_cast<const GeometryLogicalType&>(logical_type);
    ARROW_ASSIGN_OR_RAISE(std::string crs_metadata,
                          MakeGeoArrowCrsMetadata(geospatial_type.crs(), metadata));

    std::string serialized_data = std::string("{") + crs_metadata + "}";
    return maybe_geoarrow_wkb->Deserialize(storage_type, serialized_data);
  } else if (logical_type.is_geography()) {
    const auto& geospatial_type =
        ::arrow::internal::checked_cast<const GeographyLogicalType&>(logical_type);
    ARROW_ASSIGN_OR_RAISE(std::string crs_metadata,
                          MakeGeoArrowCrsMetadata(geospatial_type.crs(), metadata));
    std::string edges_metadata =
        R"("edges": ")" + std::string(geospatial_type.algorithm_name()) + R"(")";
    std::string serialized_data =
        std::string("{") + crs_metadata + ", " + edges_metadata + "}";
    return maybe_geoarrow_wkb->Deserialize(storage_type, serialized_data);
  } else {
    throw ParquetException("Can't export logical type ", logical_type.ToString(),
                           " as GeoArrow");
  }
}

}  // namespace parquet
