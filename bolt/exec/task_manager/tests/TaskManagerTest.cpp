/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include <filesystem>
#include <fstream>
#include <future>

#include "bolt/exec/task_manager/TaskManager.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

namespace bytedance::bolt::exec::test {
namespace {
class TaskManagerTest : public testing::Test,
                        public bolt::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::initialize({});
    }
  }

  TaskManagerOptions options() {
    TaskManagerOptions result;
    result.cpuThreadNumber = 2;
    result.ioThreadNumber = 1;
    result.useHiveConnector = false;
    result.useParquet = false;
    result.useHdfsFilesystem = false;
    result.useLocalFilesystem = true;
    result.periodCleanOldTasksMs = 0;
    return result;
  }

  core::PlanFragment fragment(size_t batches = 1) {
    auto data = makeRowVector({makeFlatVector<int64_t>({1, 2, 3})});
    core::PlanFragment result;
    result.planNode = PlanBuilder()
                          .values(std::vector<RowVectorPtr>(batches, data))
                          .planNode();
    return result;
  }
};

TEST_F(TaskManagerTest, spillDirectoriesAreIsolated) {
  auto directory = TempDirectoryPath::create();
  auto opts = options();
  opts.spillDir = directory->getPath();
  TaskManager manager(opts, {});
  auto first = manager.createTask("first", fragment());
  auto second = manager.createTask("second", fragment());
  const auto firstPath = first->getTask()->spillDirectory();
  const auto secondPath = second->getTask()->spillDirectory();
  ASSERT_NE(firstPath, secondPath);
  ASSERT_NE(firstPath, opts.spillDir);
  ASSERT_NE(secondPath, opts.spillDir);
  EXPECT_TRUE(std::filesystem::exists(firstPath));
  EXPECT_TRUE(std::filesystem::exists(secondPath));
  std::ofstream(secondPath + "/sentinel") << "keep";
  while (first->moveNext()) {
  }
  first->getTask()->taskCompletionFuture().wait();
  first->getTask()->requestCancel().wait();
  // Task::leave fulfills the finish future before Driver::run returns and its
  // executor callback releases DriverCtx::task. Cleanup requires that final
  // driver reference to be gone as well.
  std::weak_ptr<Task> firstTask = first->getTask();
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (firstTask.use_count() > 1 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(firstTask.use_count(), 1);
  first.reset();
  ASSERT_EQ(manager.deleteTask("first", true), 0);
  EXPECT_FALSE(std::filesystem::exists(firstPath));
  EXPECT_TRUE(std::filesystem::exists(secondPath + "/sentinel"));
  EXPECT_TRUE(std::filesystem::exists(opts.spillDir));
}

TEST_F(TaskManagerTest, shutdownAfterPartialConsumption) {
  auto manager = std::make_unique<TaskManager>(
      options(), memory::MemoryManager::Options{});
  auto task =
      manager->createTask("partial", fragment(100), {}, {}, {}, 1, {}, 1);
  ASSERT_TRUE(task->moveNext());
  manager.reset();
  EXPECT_NE(task->getTask()->state(), TaskState::kRunning);
  task.reset();
}

TEST_F(TaskManagerTest, rowVectorOutlivesManager) {
  RowVectorPtr vector;
  {
    TaskManager manager(options(), {});
    auto task = manager.createTask("complete", fragment());
    ASSERT_TRUE(task->moveNext());
    vector = task->currentResult();
    EXPECT_FALSE(task->moveNext());
  }
  EXPECT_EQ(vector->childAt(0)->as<FlatVector<int64_t>>()->valueAt(2), 3);
}

TEST_F(TaskManagerTest, arrowBatchOutlivesManager) {
  std::shared_ptr<arrow::RecordBatch> batch;
  {
    TaskManager manager(options(), {});
    auto task = manager.createTask("complete", fragment());
    ASSERT_TRUE(task->moveNext());
    batch = task->currentArrowResult();
    EXPECT_FALSE(task->moveNext());
  }
  EXPECT_EQ(batch->num_rows(), 3);
}

TEST_F(TaskManagerTest, arrowArrayOutlivesManager) {
  ArrowSchema schema{};
  ArrowArray array{};
  {
    TaskManager manager(options(), {});
    auto task = manager.createTask("complete", fragment());
    ASSERT_TRUE(task->moveNext());
    ASSERT_TRUE(task->convertCurrentToArrow(schema, array));
    EXPECT_FALSE(task->moveNext());
  }
  EXPECT_EQ(array.length, 3);
  EXPECT_EQ(static_cast<const int64_t*>(array.children[0]->buffers[1])[2], 3);
  schema.release(&schema);
  array.release(&array);
}

TEST_F(TaskManagerTest, copiedSizeControlsBackpressure) {
  auto outputPool = memory::memoryManager()->addLeafPool();
  auto values = makeFlatVector<int64_t>({7});
  auto input = makeRowVector({BaseVector::wrapInConstant(10000, 0, values)});
  const auto sourceBytes = input->retainedSize();
  BoltVectorQueue queue(outputPool, sourceBytes + 1);
  ContinueFuture future;
  EXPECT_EQ(queue.enqueue(input, &future), BlockingReason::kWaitForConsumer);
  auto output = queue.dequeue();
  EXPECT_GT(output->retainedSize(), sourceBytes + 1);
  EXPECT_TRUE(future.isReady());
}

TEST_F(TaskManagerTest, nestedStringsOutliveSourcePool) {
  const std::string value(128, 'x');
  auto outputPool = memory::memoryManager()->addLeafPool();
  auto queue = std::make_unique<BoltVectorQueue>(outputPool, 1024 * 1024);
  {
    auto sourcePool = memory::memoryManager()->addLeafPool();
    bolt::test::VectorMaker maker(sourcePool.get());
    auto input =
        maker.rowVector({maker.arrayVector<StringView>({{StringView(value)}})});
    ContinueFuture future;
    EXPECT_EQ(queue->enqueue(input, &future), BlockingReason::kNotBlocked);
    input.reset();
    EXPECT_EQ(sourcePool->usedBytes(), 0);
  }
  auto output = queue->dequeue();
  queue.reset();
  outputPool.reset();
  auto* elements = output->childAt(0)
                       ->as<ArrayVector>()
                       ->elements()
                       ->as<FlatVector<StringView>>();
  EXPECT_EQ(elements->valueAt(0).str(), value);
}

TEST_F(TaskManagerTest, failedTaskWakesConsumer) {
  TaskManager manager(options(), {});
  auto task = manager.createTask("failed", fragment(100), {}, {}, {}, 1, {}, 1);
  ASSERT_TRUE(task->moveNext());
  task->getTask()->setError("test failure");
  EXPECT_ANY_THROW(task->moveNext());
}

TEST_F(TaskManagerTest, multipleManagersAndShutdownRejectsTasks) {
  TaskManager first(options(), {});
  TaskManager second(options(), {});
  first.shutdown();
  EXPECT_ANY_THROW(first.createTask("late", fragment()));
  auto task = second.createTask("valid", fragment());
  ASSERT_TRUE(task->moveNext());
  EXPECT_FALSE(task->moveNext());
}

TEST_F(TaskManagerTest, closingQueueWakesConsumer) {
  auto pool = memory::memoryManager()->addLeafPool();
  BoltVectorQueue queue(pool, 1);
  auto result = std::async(std::launch::async, [&] { return queue.dequeue(); });
  queue.close(true);
  ASSERT_EQ(
      result.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_EQ(result.get(), nullptr);
}

TEST_F(TaskManagerTest, tinyQueueUnblocksProducer) {
  auto pool = memory::memoryManager()->addLeafPool();
  BoltVectorQueue queue(pool, 1);
  queue.setNumProducers(1);
  ContinueFuture future;
  EXPECT_EQ(
      queue.enqueue(makeRowVector({makeFlatVector<int64_t>({1})}), &future),
      BlockingReason::kWaitForConsumer);
  EXPECT_NE(queue.dequeue(), nullptr);
  EXPECT_TRUE(future.isReady());
  queue.close();
  EXPECT_EQ(queue.dequeue(), nullptr);
}
} // namespace
} // namespace bytedance::bolt::exec::test
