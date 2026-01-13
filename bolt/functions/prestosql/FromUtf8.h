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
#pragma once

#include "bolt/expression/DecodedArgs.h"
#include "bolt/expression/StringWriter.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/lib/string/StringImpl.h"
#include "bolt/functions/prestosql/Utf8Utils.h"

namespace bytedance::bolt::functions {

class FromUtf8Function : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override;

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures();

 private:
  static const std::string kReplacementChar;

  static std::string codePointToString(int64_t codePoint);

  std::string getReplacementCharacter(
      const TypePtr& type,
      DecodedVector& decoded,
      vector_size_t row) const;

  /// Returns first row that contains invalid UTF-8 string or std::nullopt if
  /// all rows are valid.
  static std::optional<vector_size_t> findFirstInvalidRow(
      const DecodedVector& decodedInput,
      const SelectivityVector& rows);

  void toVarcharNoCopy(
      const VectorPtr& input,
      DecodedVector& decodedInput,
      const SelectivityVector& rows,
      const exec::EvalCtx& context,
      VectorPtr& result) const;

  void fixInvalidUtf8(
      StringView input,
      const std::string& replacement,
      exec::StringWriter<false>& fixedWriter) const;
};

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_from_utf8,
    FromUtf8Function::signatures(),
    std::make_unique<FromUtf8Function>());

} // namespace bytedance::bolt::functions