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

#include "arrow/json/chunker.h"

#include <algorithm>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <simdjson.h>

#include "arrow/buffer.h"
#include "arrow/json/options.h"
#include "arrow/util/logging_internal.h"

namespace arrow {

using std::string_view;

namespace json {

static size_t ConsumeWhitespace(string_view view) {
  auto ws_count = view.find_first_not_of(" \t\r\n");
  if (ws_count == string_view::npos) {
    return view.size();
  } else {
    return ws_count;
  }
}

/// Find the end position of the first complete JSON document in the input.
/// Returns:
///   - The position after the first complete document if found
///   - 0 if the input is empty or contains only whitespace
///   - string_view::npos if there is no complete document (partial/invalid)
///
/// This function uses brace/bracket depth tracking to find candidate document
/// boundaries, then validates with simdjson. The validation step is necessary
/// because structurally balanced JSON (e.g., `{1}`) may still be semantically
/// invalid (missing key). Without validation, such invalid documents would be
/// passed to the main parser, causing errors at the wrong stage of processing.
static size_t ConsumeWholeObject(string_view input) {
  if (input.empty()) {
    return 0;
  }

  // Skip leading whitespace
  size_t start = ConsumeWhitespace(input);
  if (start >= input.size()) {
    return 0;  // Only whitespace
  }

  // Scan for document boundaries by tracking brace/bracket depth.
  // When depth returns to 0, we've found a candidate document boundary.
  int depth = 0;
  bool in_string = false;
  bool escape_next = false;
  bool started = false;  // Track if we've seen an opening brace/bracket

  for (size_t i = start; i < input.size(); ++i) {
    char c = input[i];

    if (escape_next) {
      escape_next = false;
      continue;
    }

    if (c == '\\' && in_string) {
      escape_next = true;
      continue;
    }

    if (c == '"') {
      in_string = !in_string;
      continue;
    }

    if (!in_string) {
      if (c == '{' || c == '[') {
        started = true;
        depth++;
      } else if (c == '}' || c == ']') {
        if (!started) {
          // Closing brace/bracket before any opening - invalid document
          return 0;
        }
        depth--;
        if (depth == 0) {
          // Found a candidate document boundary - validate with simdjson
          size_t end_pos = i + 1;
          size_t doc_len = end_pos - start;
          simdjson::padded_string padded(input.data() + start, doc_len);
          simdjson::dom::parser parser;
          if (!parser.parse(padded).error()) {
            return end_pos;
          }
          // Validation failed - structurally complete but semantically invalid
          return string_view::npos;
        } else if (depth < 0) {
          // More closing than opening - invalid
          return 0;
        }
      }
    }
  }

  // No complete document found (still inside a document)
  return string_view::npos;
}

namespace {

// A BoundaryFinder implementation that assumes JSON objects can contain raw newlines,
// and uses actual JSON parsing to delimit them.
class ParsingBoundaryFinder : public BoundaryFinder {
 public:
  Status FindFirst(string_view partial, string_view block, int64_t* out_pos) override {
    // simdjson requires contiguous memory, so concatenate partial and block
    std::string combined;
    combined.reserve(partial.size() + block.size());
    combined.append(partial);
    combined.append(block);

    const size_t start = ConsumeWhitespace(combined);
    if (start < combined.size() && combined[start] != '{' && combined[start] != '[') {
      return Status::Invalid("JSON parse error: Invalid value");
    }

    auto length = ConsumeWholeObject(combined);
    if (length == string_view::npos) {
      *out_pos = -1;
    } else if (ARROW_PREDICT_FALSE(length < partial.size())) {
      return Status::Invalid("JSON parse error: Invalid value");
    } else {
      DCHECK_LE(length, partial.size() + block.size());
      *out_pos = static_cast<int64_t>(length - partial.size());
    }
    return Status::OK();
  }

  Status FindLast(std::string_view block, int64_t* out_pos) override {
    const size_t block_length = block.size();
    size_t consumed_length = 0;

    if (block_length > 0) {
      const size_t start = ConsumeWhitespace(block);
      if (start < block.size() && block[start] != '{' && block[start] != '[') {
        return Status::Invalid("JSON parse error: Invalid value");
      }
    }

    while (consumed_length < block_length) {
      auto length = ConsumeWholeObject(block);
      if (length == string_view::npos || length == 0) {
        // found incomplete object or block is empty
        break;
      }
      consumed_length += length;
      block = block.substr(length);
    }

    if (consumed_length == 0) {
      *out_pos = -1;
    } else {
      consumed_length += ConsumeWhitespace(block);
      DCHECK_LE(consumed_length, block_length);
      *out_pos = static_cast<int64_t>(consumed_length);
    }
    return Status::OK();
  }

  Status FindNth(std::string_view partial, std::string_view block, int64_t count,
                 int64_t* out_pos, int64_t* num_found) override {
    return Status::NotImplemented("ParsingBoundaryFinder::FindNth");
  }
};

}  // namespace

std::unique_ptr<Chunker> MakeChunker(const ParseOptions& options) {
  std::shared_ptr<BoundaryFinder> delimiter;
  if (options.newlines_in_values) {
    delimiter = std::make_shared<ParsingBoundaryFinder>();
  } else {
    delimiter = MakeNewlineBoundaryFinder();
  }
  return std::make_unique<Chunker>(std::move(delimiter));
}

}  // namespace json
}  // namespace arrow
