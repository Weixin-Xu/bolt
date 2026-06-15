/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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
 */

#include <folly/Benchmark.h>
#include <folly/BenchmarkUtil.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <string>
#include <vector>

#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

using namespace bytedance::bolt;

DEFINE_int32(concat_num_batches, 8, "Number of RowVector batches to concat");
DEFINE_int32(concat_rows_per_batch, 4096, "Rows per input RowVector batch");
DEFINE_int32(concat_flat_columns, 8, "Number of top-level flat columns");
DEFINE_int32(
    concat_nested_array_columns,
    128,
    "Number of top-level row(array<T>) columns");
DEFINE_int32(
    concat_string_array_columns,
    2,
    "Number of row(array<string>) columns among nested array columns");
DEFINE_int32(concat_max_array_size, 1024, "Maximum elements per array row");
DEFINE_int32(
    concat_small_array_size,
    64,
    "Most non-null arrays have fewer elements than this value");
DEFINE_int32(
    concat_null_every,
    17,
    "Make every Nth array null. Use 0 to disable nulls");
DEFINE_int32(
    concat_large_array_every,
    257,
    "Make every Nth non-null array use concat_max_array_size");

namespace {

template <TypeKind kind>
void reserveFlatValuesTyped(const VectorPtr& target, vector_size_t size) {
  using T = typename TypeTraits<kind>::NativeType;
  target->asUnchecked<FlatVector<T>>()->mutableValues(size);
}

void reserveFlatValues(const VectorPtr& target, vector_size_t size) {
  if (size == 0 || target->typeKind() == TypeKind::UNKNOWN ||
      target->typeKind() == TypeKind::OPAQUE) {
    return;
  }

  BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      reserveFlatValuesTyped, target->typeKind(), target, size);
}

vector_size_t sumArrayChildren(
    const std::vector<VectorPtr>& sources,
    vector_size_t* totalRows = nullptr) {
  vector_size_t total = 0;
  vector_size_t rows = 0;

  for (const auto& source : sources) {
    rows += source->size();
    const auto* leaf = source->wrappedVector()->asUnchecked<ArrayVectorBase>();
    for (vector_size_t row = 0; row < source->size(); ++row) {
      if (!source->isNullAt(row)) {
        total += leaf->sizeAt(source->wrappedIndex(row));
      }
    }
  }

  if (totalRows) {
    *totalRows = rows;
  }
  return total;
}

std::vector<VectorPtr> arrayElementSources(
    const std::vector<VectorPtr>& sources) {
  std::vector<VectorPtr> children;
  children.reserve(sources.size());
  for (const auto& source : sources) {
    children.push_back(
        source->wrappedVector()->asUnchecked<ArrayVector>()->elements());
  }
  return children;
}

void reserveNestedVectorCapacity(
    const VectorPtr& target,
    const std::vector<VectorPtr>& sources,
    vector_size_t logicalSize) {
  if (std::any_of(sources.begin(), sources.end(), [](const auto& source) {
        return source->mayHaveNulls();
      })) {
    target->mutableNulls(logicalSize);
  }

  switch (target->typeKind()) {
    case TypeKind::ROW: {
      auto* rowTarget = target->asUnchecked<RowVector>();
      for (column_index_t i = 0; i < rowTarget->childrenSize(); ++i) {
        std::vector<VectorPtr> childSources;
        childSources.reserve(sources.size());
        for (const auto& source : sources) {
          childSources.push_back(
              source->wrappedVector()->asUnchecked<RowVector>()->childAt(i));
        }
        reserveNestedVectorCapacity(
            rowTarget->childAt(i), childSources, logicalSize);
      }
      break;
    }
    case TypeKind::ARRAY: {
      auto* arrayTarget = target->asUnchecked<ArrayVector>();
      arrayTarget->mutableOffsets(logicalSize);
      arrayTarget->mutableSizes(logicalSize);

      const auto totalElements = sumArrayChildren(sources);
      auto children = arrayElementSources(sources);
      reserveNestedVectorCapacity(
          arrayTarget->elements(), children, totalElements);
      break;
    }
    default:
      reserveFlatValues(target, logicalSize);
      break;
  }
}

vector_size_t totalRows(const std::vector<RowVectorPtr>& batches) {
  vector_size_t total = 0;
  for (const auto& batch : batches) {
    total += batch->size();
  }
  return total;
}

RowVectorPtr mergeBaseline(
    const std::vector<RowVectorPtr>& batches,
    memory::MemoryPool* pool) {
  auto output = BaseVector::create<RowVector>(
      batches[0]->type(), totalRows(batches), pool);

  vector_size_t offset = 0;
  for (const auto& batch : batches) {
    output->copy(batch.get(), offset, 0, batch->size());
    offset += batch->size();
  }
  return output;
}

RowVectorPtr mergeWithNestedReserve(
    const std::vector<RowVectorPtr>& batches,
    memory::MemoryPool* pool) {
  const auto rows = totalRows(batches);
  auto output = BaseVector::create<RowVector>(batches[0]->type(), rows, pool);

  std::vector<VectorPtr> sources;
  sources.reserve(batches.size());
  for (const auto& batch : batches) {
    sources.push_back(batch);
  }
  reserveNestedVectorCapacity(output, sources, rows);

  vector_size_t offset = 0;
  for (const auto& batch : batches) {
    output->copy(batch.get(), offset, 0, batch->size());
    offset += batch->size();
  }
  return output;
}

vector_size_t arraySizeAt(vector_size_t row, int32_t columnIndex) {
  if (FLAGS_concat_null_every > 0 &&
      (row + columnIndex * 13) % FLAGS_concat_null_every == 0) {
    return 0;
  }
  if (FLAGS_concat_large_array_every > 0 &&
      (row + columnIndex * 29) % FLAGS_concat_large_array_every == 0) {
    return FLAGS_concat_max_array_size;
  }
  return ((row * 1103515245u + columnIndex * 12345u) >> 16) %
      std::max(1, FLAGS_concat_small_array_size);
}

void makeArrayLayout(
    vector_size_t rows,
    int32_t columnIndex,
    std::vector<vector_size_t>& offsets,
    std::vector<vector_size_t>& nulls,
    vector_size_t& numElements) {
  offsets.clear();
  offsets.reserve(rows);
  nulls.clear();
  numElements = 0;

  for (vector_size_t row = 0; row < rows; ++row) {
    offsets.push_back(numElements);
    const bool isNull = FLAGS_concat_null_every > 0 &&
        (row + columnIndex * 13) % FLAGS_concat_null_every == 0;
    if (isNull) {
      nulls.push_back(row);
      continue;
    }
    numElements += arraySizeAt(row, columnIndex);
  }
}

template <typename T>
ArrayVectorPtr makePrimitiveArray(
    test::VectorMaker& maker,
    vector_size_t rows,
    int32_t columnIndex,
    T seed) {
  std::vector<vector_size_t> offsets;
  std::vector<vector_size_t> nulls;
  vector_size_t numElements;
  makeArrayLayout(rows, columnIndex, offsets, nulls, numElements);

  auto elements = maker.flatVector<T>(numElements, [seed](vector_size_t row) {
    return static_cast<T>(seed + row % 10'000);
  });
  return maker.arrayVector(offsets, elements, nulls);
}

ArrayVectorPtr makeStringArray(
    test::VectorMaker& maker,
    vector_size_t rows,
    int32_t columnIndex,
    int32_t seed) {
  std::vector<vector_size_t> offsets;
  std::vector<vector_size_t> nulls;
  vector_size_t numElements;
  makeArrayLayout(rows, columnIndex, offsets, nulls, numElements);

  auto elements = maker.flatVector<std::string>(
      numElements, [columnIndex, seed](vector_size_t row) {
        return "s" + std::to_string(columnIndex) + "_" +
            std::to_string(seed + row % 10'000);
      });
  return maker.arrayVector(offsets, elements, nulls);
}

RowVectorPtr makeNestedArrayRow(
    test::VectorMaker& maker,
    vector_size_t rows,
    int32_t columnIndex,
    int32_t batchIndex) {
  const auto seed =
      batchIndex * FLAGS_concat_nested_array_columns + columnIndex;
  if (columnIndex < FLAGS_concat_string_array_columns) {
    return maker.rowVector(
        {"array_string"}, {makeStringArray(maker, rows, columnIndex, seed)});
  }

  if (columnIndex % 2 == 0) {
    return maker.rowVector(
        {"array_int"},
        {makePrimitiveArray<int32_t>(maker, rows, columnIndex, seed * 1'000)});
  }

  return maker.rowVector(
      {"array_float"},
      {makePrimitiveArray<float>(
          maker, rows, columnIndex, static_cast<float>(seed * 1'000))});
}

RowVectorPtr
makeBatch(test::VectorMaker& maker, vector_size_t rows, int32_t batchIndex) {
  std::vector<std::string> names;
  std::vector<VectorPtr> children;
  names.reserve(FLAGS_concat_flat_columns + FLAGS_concat_nested_array_columns);
  children.reserve(names.capacity());

  for (int32_t i = 0; i < FLAGS_concat_flat_columns; ++i) {
    if (i % 2 == 0) {
      names.push_back("flat_int_" + std::to_string(i));
      children.push_back(
          maker.flatVector<int32_t>(rows, [batchIndex, i](vector_size_t row) {
            return batchIndex * 1'000'000 + i * 10'000 + row;
          }));
    } else {
      names.push_back("flat_float_" + std::to_string(i));
      children.push_back(
          maker.flatVector<float>(rows, [batchIndex, i](vector_size_t row) {
            return static_cast<float>(
                batchIndex * 1'000'000 + i * 10'000 + row);
          }));
    }
  }

  for (int32_t i = 0; i < FLAGS_concat_nested_array_columns; ++i) {
    names.push_back("nested_array_row_" + std::to_string(i));
    children.push_back(makeNestedArrayRow(maker, rows, i, batchIndex));
  }

  return maker.rowVector(names, children);
}

std::vector<RowVectorPtr> makeBatches(memory::MemoryPool* pool) {
  test::VectorMaker maker{pool};
  std::vector<RowVectorPtr> batches;
  batches.reserve(FLAGS_concat_num_batches);
  for (int32_t i = 0; i < FLAGS_concat_num_batches; ++i) {
    batches.push_back(makeBatch(maker, FLAGS_concat_rows_per_batch, i));
  }
  return batches;
}

template <typename Merge>
size_t runConcatBenchmark(size_t iterations, Merge merge) {
  folly::BenchmarkSuspender suspender;
  auto pool = memory::memoryManager()->addLeafPool();
  auto batches = makeBatches(pool.get());
  const auto rows = totalRows(batches);
  suspender.dismiss();

  for (size_t i = 0; i < iterations; ++i) {
    auto output = merge(batches, pool.get());
    folly::doNotOptimizeAway(output);
  }

  return rows * iterations;
}

BENCHMARK_MULTI(baseline, n) {
  return runConcatBenchmark(n, mergeBaseline);
}

BENCHMARK_RELATIVE_MULTI(nestedReserve, n) {
  return runConcatBenchmark(n, mergeWithNestedReserve);
}

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memory::MemoryManager::initialize({});
  folly::runBenchmarks();
  return 0;
}
