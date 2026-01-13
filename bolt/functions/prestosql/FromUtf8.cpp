/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */
#include "bolt/functions/prestosql/FromUtf8.h"
#include <optional>
#include "bolt/vector/TypeAliases.h"

namespace bytedance::bolt::functions {

void FromUtf8Function::apply(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args,
    const TypePtr& /* outputType */,
    exec::EvalCtx& context,
    VectorPtr& result) const {
  const auto& input = args[0];

  // Optimize common cases:
  // (1) all-ASCII input;
  // (2) valid UTF-8 input;
  // (3) constant replacement character.

  exec::DecodedArgs decodedArgs(rows, args, context);
  auto& decodedInput = *decodedArgs.at(0);

  // Read the constant replacement if it exisits and verify that it is valid.
  bool constantReplacement =
      args.size() == 1 || decodedArgs.at(1)->isConstantMapping();
  std::string constantReplacementValue = kReplacementChar;

  if (constantReplacement) {
    if (args.size() > 1) {
      auto& decodedReplacement = *decodedArgs.at(1);
      try {
        constantReplacementValue = getReplacementCharacter(
            args[1]->type(), decodedReplacement, rows.begin());
      } catch (const std::exception&) {
        context.setErrors(rows, std::current_exception());
        return;
      }
    }
  }

  // We can only do valid UTF-8 input optimization if replacement is valid and
  // constant otherwise we have to check replacement for each row.
  if (constantReplacement) {
    if (input->loadedVector()
            ->as<SimpleVector<StringView>>()
            ->computeAndSetIsAscii(rows)) {
      // Input strings are all-ASCII.
      toVarcharNoCopy(input, decodedInput, rows, context, result);
      return;
    }
  }

  auto firstInvalidRow = findFirstInvalidRow(decodedInput, rows);

  // We can only do this optimization if replacement is valid and
  // constant otherwise we have to check replacement for each row.
  if (constantReplacement) {
    if (!firstInvalidRow.has_value()) {
      // All inputs are valid UTF-8 strings.
      toVarcharNoCopy(input, decodedInput, rows, context, result);
      return;
    }
  }

  BaseVector::ensureWritable(rows, VARCHAR(), context.pool(), result);
  auto flatResult = result->as<FlatVector<StringView>>();

  // Reserve string buffer capacity.
  size_t totalInputSize = 0;
  rows.applyToSelected([&](auto row) {
    totalInputSize += decodedInput.valueAt<StringView>(row).size();
  });

  flatResult->getBufferWithSpace(totalInputSize);

  if (constantReplacement) {
    rows.applyToSelected([&](auto row) {
      exec::StringWriter<false> writer(flatResult, row);
      auto value = decodedInput.valueAt<StringView>(row);
      if (row < firstInvalidRow) {
        writer.append(value);
        writer.finalize();
      } else {
        fixInvalidUtf8(value, constantReplacementValue, writer);
      }
    });
  } else {
    auto& decodedReplacement = *decodedArgs.at(1);
    context.applyToSelectedNoThrow(rows, [&](auto row) {
      auto replacement =
          getReplacementCharacter(args[1]->type(), decodedReplacement, row);
      exec::StringWriter<false> writer(flatResult, row);
      auto value = decodedInput.valueAt<StringView>(row);
      if (row < firstInvalidRow) {
        writer.append(value);
        writer.finalize();
      } else {
        fixInvalidUtf8(value, replacement, writer);
      }
    });
  }
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
FromUtf8Function::signatures() {
  return {
      // varbinary -> varchar
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varbinary")
          .build(),

      // varbinary, bigint -> varchar
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varbinary")
          .argumentType("bigint")
          .build(),

      // varbinary, varchar -> varchar
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varbinary")
          .argumentType("varchar")
          .build(),
  };
}

std::string FromUtf8Function::codePointToString(int64_t codePoint) {
  std::string result;
  result.resize(4);
  stringImpl::codePointToString(result, codePoint);
  return result;
}

std::string FromUtf8Function::getReplacementCharacter(
    const TypePtr& type,
    DecodedVector& decoded,
    vector_size_t row) const {
  if (type->isBigint()) {
    return codePointToString(decoded.valueAt<int64_t>(row));
  }

  auto replacement = decoded.valueAt<StringView>(row);
  if (!replacement.empty()) {
    auto charLength = tryGetCharLength(replacement.data(), replacement.size());
    BOLT_USER_CHECK_GT(
        charLength, 0, "Replacement is not a valid UTF-8 character");
    BOLT_USER_CHECK_EQ(
        charLength,
        replacement.size(),
        "Replacement string must be empty or a single character")
  }
  return replacement;
}

std::optional<vector_size_t> FromUtf8Function::findFirstInvalidRow(
    const DecodedVector& decodedInput,
    const SelectivityVector& rows) {
  std::optional<vector_size_t> firstInvalidRow;
  rows.testSelected([&](auto row) {
    auto value = decodedInput.valueAt<StringView>(row);

    int32_t pos = 0;
    while (pos < value.size()) {
      auto charLength =
          tryGetCharLength(value.data() + pos, value.size() - pos);
      if (charLength < 0) {
        firstInvalidRow = row;
        return false;
      }

      pos += charLength;
    }

    return true;
  });
  return firstInvalidRow;
}

void FromUtf8Function::toVarcharNoCopy(
    const VectorPtr& input,
    DecodedVector& decodedInput,
    const SelectivityVector& rows,
    const exec::EvalCtx& context,
    VectorPtr& result) const {
  VectorPtr localResult;
  if (decodedInput.isConstantMapping()) {
    auto value = decodedInput.valueAt<StringView>(rows.begin());
    localResult = std::make_shared<ConstantVector<StringView>>(
        context.pool(), rows.end(), false, VARCHAR(), std::move(value));
  } else if (decodedInput.isIdentityMapping()) {
    auto flatInput = decodedInput.base()->asFlatVector<StringView>();

    auto stringBuffers = flatInput->stringBuffers();
    BOLT_CHECK_LE(rows.end(), flatInput->size());
    localResult = std::make_shared<FlatVector<StringView>>(
        context.pool(),
        VARCHAR(),
        nullptr,
        rows.end(),
        flatInput->values(),
        std::move(stringBuffers));
  } else {
    auto base = decodedInput.base();
    if (base->isConstantEncoding()) {
      auto value = decodedInput.valueAt<StringView>(rows.begin());
      localResult = std::make_shared<ConstantVector<StringView>>(
          context.pool(), rows.end(), false, VARCHAR(), std::move(value));
    } else {
      auto flatBase = base->asFlatVector<StringView>();
      auto stringBuffers = flatBase->stringBuffers();

      auto values = AlignedBuffer::allocate<StringView>(
          rows.end(), context.pool(), StringView());
      auto* rawValues = values->asMutable<StringView>();
      rows.applyToSelected([&](auto row) {
        rawValues[row] = decodedInput.valueAt<StringView>(row);
      });

      localResult = std::make_shared<FlatVector<StringView>>(
          context.pool(),
          VARCHAR(),
          nullptr,
          rows.end(),
          std::move(values),
          std::move(stringBuffers));
    }
  }

  context.moveOrCopyResult(localResult, rows, result);
}

void FromUtf8Function::fixInvalidUtf8(
    StringView input,
    const std::string& replacement,
    exec::StringWriter<false>& fixedWriter) const {
  if (input.empty()) {
    fixedWriter.setEmpty();
    return;
  }

  int32_t pos = 0;
  while (pos < input.size()) {
    auto charLength = tryGetCharLength(input.data() + pos, input.size() - pos);
    if (charLength > 0) {
      fixedWriter.append(std::string_view(input.data() + pos, charLength));
      pos += charLength;
      continue;
    }

    if (!replacement.empty()) {
      fixedWriter.append(replacement);
    }

    pos += -charLength;
  }

  fixedWriter.finalize();
}

// static
const std::string FromUtf8Function::kReplacementChar =
    codePointToString(0xFFFD);

} // namespace bytedance::bolt::functions