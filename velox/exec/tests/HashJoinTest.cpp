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
 */

#include <re2/re2.h>

#include <fmt/format.h>
#include "folly/experimental/EventCount.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/HashBuild.h"
#include "velox/exec/HashJoinBridge.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/TableScan.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/exec/tests/utils/VectorTestUtil.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::common::testutil;

using facebook::velox::test::BatchMaker;

namespace {
struct TestParam {
  int numDrivers;

  explicit TestParam(int _numDrivers) : numDrivers(_numDrivers) {}
};

using SplitInput =
    std::unordered_map<core::PlanNodeId, std::vector<exec::Split>>;

std::function<void(Task* task)> makeAddSplit(
    bool& noMoreSplits,
    SplitInput splits) {
  return [&](Task* task) {
    if (noMoreSplits) {
      return;
    }
    for (auto& [nodeId, nodeSplits] : splits) {
      for (auto& split : nodeSplits) {
        task->addSplit(nodeId, std::move(split));
      }
      task->noMoreSplits(nodeId);
    }
    noMoreSplits = true;
  };
}

// Returns aggregated spilled stats by build and probe operators from 'task'.
std::pair<common::SpillStats, common::SpillStats> taskSpilledStats(
    const exec::Task& task) {
  common::SpillStats buildStats;
  common::SpillStats probeStats;
  auto stats = task.taskStats();
  for (auto& pipeline : stats.pipelineStats) {
    for (auto op : pipeline.operatorStats) {
      if (op.operatorType == "HashBuild") {
        buildStats.spilledInputBytes += op.spilledInputBytes;
        buildStats.spilledBytes += op.spilledBytes;
        buildStats.spilledRows += op.spilledRows;
        buildStats.spilledPartitions += op.spilledPartitions;
        buildStats.spilledFiles += op.spilledFiles;
      } else if (op.operatorType == "HashProbe") {
        probeStats.spilledInputBytes += op.spilledInputBytes;
        probeStats.spilledBytes += op.spilledBytes;
        probeStats.spilledRows += op.spilledRows;
        probeStats.spilledPartitions += op.spilledPartitions;
        probeStats.spilledFiles += op.spilledFiles;
      }
    }
  }
  return {buildStats, probeStats};
}

// Returns aggregated spilled runtime stats by build and probe operators from
// 'task'.
void verifyTaskSpilledRuntimeStats(const exec::Task& task, bool expectedSpill) {
  auto stats = task.taskStats();
  for (auto& pipeline : stats.pipelineStats) {
    for (auto op : pipeline.operatorStats) {
      if ((op.operatorType == "HashBuild") ||
          (op.operatorType == "HashProbe")) {
        if (!expectedSpill) {
          ASSERT_EQ(op.runtimeStats["spillRuns"].count, 0);
          ASSERT_EQ(op.runtimeStats["spillFillTime"].count, 0);
          ASSERT_EQ(op.runtimeStats["spillSortTime"].count, 0);
          ASSERT_EQ(op.runtimeStats["spillSerializationTime"].count, 0);
          ASSERT_EQ(op.runtimeStats["spillFlushTime"].count, 0);
          ASSERT_EQ(op.runtimeStats["spillWrites"].count, 0);
          ASSERT_EQ(op.runtimeStats["spillWriteTime"].count, 0);
        } else {
          if (op.operatorType == "HashBuild") {
            ASSERT_GT(op.runtimeStats["spillRuns"].count, 0);
            ASSERT_GT(op.runtimeStats["spillFillTime"].sum, 0);
          } else {
            ASSERT_EQ(op.runtimeStats["spillRuns"].count, 0);
            ASSERT_EQ(op.runtimeStats["spillFillTime"].sum, 0);
          }
          ASSERT_EQ(op.runtimeStats["spillSortTime"].sum, 0);
          ASSERT_GT(op.runtimeStats["spillSerializationTime"].sum, 0);
          ASSERT_GE(op.runtimeStats["spillFlushTime"].sum, 0);
          // NOTE: spill flush might take less than one microsecond.
          ASSERT_GE(
              op.runtimeStats["spillSerializationTime"].count,
              op.runtimeStats["spillFlushTime"].count);
          ASSERT_GT(op.runtimeStats["spillWrites"].sum, 0);
          ASSERT_GE(op.runtimeStats["spillWriteTime"].sum, 0);
          // NOTE: spill flush might take less than one microsecond.
          ASSERT_GE(
              op.runtimeStats["spillWrites"].count,
              op.runtimeStats["spillWriteTime"].count);
        }
      }
    }
  }
}

static uint64_t getOutputPositions(
    const std::shared_ptr<Task>& task,
    const std::string& operatorType) {
  uint64_t count = 0;
  for (const auto& pipelineStat : task->taskStats().pipelineStats) {
    for (const auto& operatorStat : pipelineStat.operatorStats) {
      if (operatorStat.operatorType == operatorType) {
        count += operatorStat.outputPositions;
      }
    }
  }
  return count;
}

// Returns the max hash build spill level by 'task'.
int32_t maxHashBuildSpillLevel(const exec::Task& task) {
  int32_t maxSpillLevel = -1;
  for (auto& pipelineStat : task.taskStats().pipelineStats) {
    for (auto& operatorStat : pipelineStat.operatorStats) {
      if (operatorStat.operatorType == "HashBuild") {
        if (operatorStat.runtimeStats.count("maxSpillLevel") == 0) {
          continue;
        }
        maxSpillLevel = std::max<int32_t>(
            maxSpillLevel, operatorStat.runtimeStats["maxSpillLevel"].max);
      }
    }
  }
  return maxSpillLevel;
}

std::pair<int32_t, int32_t> numTaskSpillFiles(const exec::Task& task) {
  int32_t numBuildFiles = 0;
  int32_t numProbeFiles = 0;
  for (auto& pipelineStat : task.taskStats().pipelineStats) {
    for (auto& operatorStat : pipelineStat.operatorStats) {
      if (operatorStat.runtimeStats.count("spillFileSize") == 0) {
        continue;
      }
      if (operatorStat.operatorType == "HashBuild") {
        numBuildFiles += operatorStat.runtimeStats["spillFileSize"].count;
        continue;
      }
      if (operatorStat.operatorType == "HashProbe") {
        numProbeFiles += operatorStat.runtimeStats["spillFileSize"].count;
      }
    }
  }
  return {numBuildFiles, numProbeFiles};
}

void abortPool(memory::MemoryPool* pool) {
  try {
    VELOX_FAIL("Manual MemoryPool Abortion");
  } catch (const VeloxException& error) {
    pool->abort(std::current_exception());
  }
}

using JoinResultsVerifier =
    std::function<void(const std::shared_ptr<Task>&, bool)>;

class HashJoinBuilder {
 public:
  HashJoinBuilder(
      memory::MemoryPool& pool,
      DuckDbQueryRunner& duckDbQueryRunner,
      folly::Executor* executor)
      : pool_(pool),
        duckDbQueryRunner_(duckDbQueryRunner),
        executor_(executor) {
    // Small batches create more edge cases.
    fuzzerOpts_.vectorSize = 10;
    fuzzerOpts_.nullRatio = 0.1;
    fuzzerOpts_.stringVariableLength = true;
    fuzzerOpts_.containerVariableLength = true;
  }

  HashJoinBuilder& numDrivers(int32_t numDrivers) {
    numDrivers_ = numDrivers;
    return *this;
  }

  HashJoinBuilder& planNode(core::PlanNodePtr planNode) {
    VELOX_CHECK_NULL(planNode_);
    planNode_ = planNode;
    return *this;
  }

  HashJoinBuilder& keyTypes(const std::vector<TypePtr>& keyTypes) {
    VELOX_CHECK_NULL(probeType_);
    VELOX_CHECK_NULL(buildType_);
    probeType_ = makeProbeType(keyTypes);
    probeKeys_ = makeKeyNames(keyTypes.size(), "t_");
    buildType_ = makeBuildType(keyTypes);
    buildKeys_ = makeKeyNames(keyTypes.size(), "u_");
    return *this;
  }

  HashJoinBuilder& referenceQuery(const std::string& referenceQuery) {
    referenceQuery_ = referenceQuery;
    return *this;
  }

  HashJoinBuilder& probeType(const RowTypePtr& probeType) {
    VELOX_CHECK_NULL(probeType_);
    probeType_ = probeType;
    return *this;
  }

  HashJoinBuilder& probeKeys(const std::vector<std::string>& probeKeys) {
    probeKeys_ = probeKeys;
    return *this;
  }

  HashJoinBuilder& probeFilter(const std::string& probeFilter) {
    probeFilter_ = probeFilter;
    return *this;
  }

  HashJoinBuilder& probeProjections(
      std::vector<std::string>&& probeProjections) {
    probeProjections_ = std::move(probeProjections);
    return *this;
  }

  HashJoinBuilder& probeVectors(int32_t vectorSize, int32_t numVectors) {
    VELOX_CHECK_NOT_NULL(probeType_);
    VELOX_CHECK(probeVectors_.empty());
    auto vectors = makeVectors(vectorSize, numVectors, probeType_);
    return probeVectors(std::move(vectors));
  }

  HashJoinBuilder& probeVectors(std::vector<RowVectorPtr>&& probeVectors) {
    VELOX_CHECK(!probeVectors.empty());
    if (probeType_ == nullptr) {
      probeType_ = asRowType(probeVectors[0]->type());
    }
    probeVectors_ = std::move(probeVectors);
    // NOTE: there is one value node copy per driver thread and if the value
    // node is not parallelizable, then the associated driver pipeline will be
    // single threaded. 'allProbeVectors_' contains the value vectors fed to
    // all the hash probe drivers, which will be used to populate the duckdb
    // as well.
    allProbeVectors_ = makeCopies(probeVectors_, numDrivers_);
    return *this;
  }

  HashJoinBuilder& buildType(const RowTypePtr& buildType) {
    VELOX_CHECK_NULL(buildType_);
    buildType_ = buildType;
    return *this;
  }

  HashJoinBuilder& buildKeys(const std::vector<std::string>& buildKeys) {
    buildKeys_ = buildKeys;
    return *this;
  }

  HashJoinBuilder& buildFilter(const std::string& buildFilter) {
    buildFilter_ = buildFilter;
    return *this;
  }

  HashJoinBuilder& buildProjections(
      std::vector<std::string>&& buildProjections) {
    buildProjections_ = std::move(buildProjections);
    return *this;
  }

  HashJoinBuilder& buildVectors(int32_t vectorSize, int32_t numVectors) {
    VELOX_CHECK_NOT_NULL(buildType_);
    VELOX_CHECK(buildVectors_.empty());
    auto vectors = makeVectors(vectorSize, numVectors, buildType_);
    return buildVectors(std::move(vectors));
  }

  HashJoinBuilder& buildVectors(std::vector<RowVectorPtr>&& buildVectors) {
    VELOX_CHECK(!buildVectors.empty());
    if (buildType_ == nullptr) {
      buildType_ = asRowType(buildVectors[0]->type());
    }
    buildVectors_ = std::move(buildVectors);
    // NOTE: there is one value node copy per driver thread and if the value
    // node is not parallelizable, then the associated driver pipeline will be
    // single threaded. 'allBuildVectors_' contains the value vectors fed to
    // all the hash build drivers, which will be used to populate the duckdb
    // as well.
    allBuildVectors_ = makeCopies(buildVectors_, numDrivers_);
    return *this;
  }

  HashJoinBuilder& joinType(core::JoinType joinType) {
    joinType_ = joinType;
    return *this;
  }

  HashJoinBuilder& nullAware(bool nullAware) {
    nullAware_ = nullAware;
    return *this;
  }

  HashJoinBuilder& joinFilter(const std::string& joinFilter) {
    joinFilter_ = joinFilter;
    return *this;
  }

  HashJoinBuilder& joinOutputLayout(
      std::vector<std::string>&& joinOutputLayout) {
    joinOutputLayout_ = std::move(joinOutputLayout);
    return *this;
  }

  HashJoinBuilder& outputProjections(
      std::vector<std::string>&& outputProjections) {
    outputProjections_ = std::move(outputProjections);
    return *this;
  }

  HashJoinBuilder& inputSplits(const SplitInput& inputSplits) {
    makeInputSplits_ = [inputSplits] { return inputSplits; };
    return *this;
  }

  HashJoinBuilder& makeInputSplits(
      std::function<SplitInput()>&& makeInputSplits) {
    makeInputSplits_ = makeInputSplits;
    return *this;
  }

  HashJoinBuilder& config(const std::string& key, const std::string& value) {
    configs_[key] = value;
    return *this;
  }

  HashJoinBuilder& spillMemoryThreshold(uint64_t threshold) {
    spillMemoryThreshold_ = threshold;
    return *this;
  }

  HashJoinBuilder& injectSpill(bool injectSpill) {
    injectSpill_ = injectSpill;
    return *this;
  }

  HashJoinBuilder& maxSpillLevel(int32_t maxSpillLevel) {
    maxSpillLevel_ = maxSpillLevel;
    return *this;
  }

  HashJoinBuilder& checkSpillStats(bool checkSpillStats) {
    checkSpillStats_ = checkSpillStats;
    return *this;
  }

  HashJoinBuilder& queryPool(std::shared_ptr<memory::MemoryPool>&& queryPool) {
    queryPool_ = queryPool;
    return *this;
  }

  HashJoinBuilder& hashProbeFinishEarlyOnEmptyBuild(bool value) {
    hashProbeFinishEarlyOnEmptyBuild_ = value;
    return *this;
  }

  HashJoinBuilder& spillDirectory(const std::string& spillDirectory) {
    spillDirectory_ = spillDirectory;
    return *this;
  }

  HashJoinBuilder& verifier(JoinResultsVerifier testVerifier) {
    testVerifier_ = std::move(testVerifier);
    return *this;
  }

  void run() {
    if (planNode_ != nullptr) {
      runTest(planNode_);
      return;
    }

    ASSERT_FALSE(referenceQuery_.empty());
    ASSERT_TRUE(probeType_ != nullptr);
    ASSERT_FALSE(probeKeys_.empty());
    ASSERT_TRUE(buildType_ != nullptr);
    ASSERT_FALSE(buildKeys_.empty());
    ASSERT_EQ(probeKeys_.size(), buildKeys_.size());

    if (joinOutputLayout_.empty()) {
      joinOutputLayout_ = concat(probeType_->names(), buildType_->names());
    }

    createDuckDbTable("t", allProbeVectors_);
    createDuckDbTable("u", allBuildVectors_);

    struct TestSettings {
      int probeParallelize;
      int buildParallelize;

      std::string debugString() const {
        return fmt::format(
            "probeParallelize: {}, buildParallelize: {}",
            probeParallelize,
            buildParallelize);
      }
    };

    std::vector<TestSettings> testSettings;
    testSettings.push_back({
        true,
        true,
    });
    if (numDrivers_ != 1) {
      testSettings.push_back({true, false});
      testSettings.push_back({false, true});
    }

    for (const auto& testData : testSettings) {
      SCOPED_TRACE(fmt::format(
          "{} numDrivers: {}", testData.debugString(), numDrivers_));
      auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
      std::shared_ptr<const core::HashJoinNode> joinNode;
      auto planNode =
          PlanBuilder(planNodeIdGenerator, &pool_)
              .values(
                  testData.probeParallelize ? probeVectors_ : allProbeVectors_,
                  testData.probeParallelize)
              .optionalFilter(probeFilter_)
              .optionalProject(probeProjections_)
              .hashJoin(
                  probeKeys_,
                  buildKeys_,
                  PlanBuilder(planNodeIdGenerator)
                      .values(
                          testData.buildParallelize ? buildVectors_
                                                    : allBuildVectors_,
                          testData.buildParallelize)
                      .optionalFilter(buildFilter_)
                      .optionalProject(buildProjections_)
                      .planNode(),
                  joinFilter_,
                  joinOutputLayout_,
                  joinType_,
                  nullAware_)
              .capturePlanNode<core::HashJoinNode>(joinNode)
              .optionalProject(outputProjections_)
              .planNode();

      runTest(planNode);
    }
  }

 private:
  // NOTE: if 'vectorSize' is 0, then 'numVectors' is ignored and the function
  // returns a single empty batch.
  std::vector<RowVectorPtr> makeVectors(
      vector_size_t vectorSize,
      vector_size_t numVectors,
      RowTypePtr rowType,
      double nullRatio = 0.1,
      bool shuffle = true) {
    VELOX_CHECK_GE(vectorSize, 0);
    VELOX_CHECK_GT(numVectors, 0);

    std::vector<RowVectorPtr> vectors;
    vectors.reserve(numVectors);
    if (vectorSize != 0) {
      fuzzerOpts_.vectorSize = vectorSize;
      fuzzerOpts_.nullRatio = nullRatio;
      VectorFuzzer fuzzer(fuzzerOpts_, &pool_);
      for (int32_t i = 0; i < numVectors; ++i) {
        vectors.push_back(fuzzer.fuzzInputRow(rowType));
      }
    } else {
      vectors.push_back(RowVector::createEmpty(rowType, &pool_));
    }
    // NOTE: we generate a number of vectors with a fresh new fuzzer init with
    // the same fix seed. The purpose is to ensure we have sufficient match if
    // we use the row type for both build and probe inputs. Here we shuffle
    // the built vectors to introduce some randomness during the join
    // execution.
    if (shuffle) {
      shuffleBatches(vectors);
    }
    return vectors;
  }

  static RowTypePtr makeProbeType(const std::vector<TypePtr>& keyTypes) {
    return makeRowType(keyTypes, "t_");
  }

  static RowTypePtr makeBuildType(const std::vector<TypePtr>& keyTypes) {
    return makeRowType(keyTypes, "u_");
  }

  static RowTypePtr makeRowType(
      const std::vector<TypePtr>& keyTypes,
      const std::string& namePrefix) {
    std::vector<std::string> names = makeKeyNames(keyTypes.size(), namePrefix);
    names.push_back(fmt::format("{}data", namePrefix));

    std::vector<TypePtr> types = keyTypes;
    types.push_back(VARCHAR());

    return ROW(std::move(names), std::move(types));
  }

  static std::vector<std::string> makeKeyNames(
      int32_t cnt,
      const std::string& prefix) {
    std::vector<std::string> names;
    for (int i = 0; i < cnt; ++i) {
      names.push_back(fmt::format("{}k{}", prefix, i));
    }
    return names;
  }

  void createDuckDbTable(
      const std::string& tableName,
      const std::vector<RowVectorPtr>& data) {
    duckDbQueryRunner_.createTable(tableName, data);
  }

  void runTest(const core::PlanNodePtr& planNode) {
    runTest(planNode, false, maxSpillLevel_.value_or(-1));
    if (injectSpill_) {
      if (maxSpillLevel_.has_value()) {
        runTest(planNode, true, maxSpillLevel_.value(), 100);
      } else {
        runTest(planNode, true, 0, 100);
        runTest(planNode, true, 2, 100);
      }
    }
  }

  void runTest(
      const core::PlanNodePtr& planNode,
      bool injectSpill,
      int32_t maxSpillLevel = -1,
      uint32_t maxDriverYieldTimeMs = 0) {
    AssertQueryBuilder builder(planNode, duckDbQueryRunner_);
    builder.maxDrivers(numDrivers_);
    if (makeInputSplits_) {
      for (const auto& splitEntry : makeInputSplits_()) {
        builder.splits(splitEntry.first, splitEntry.second);
      }
    }
    auto queryCtx = std::make_shared<core::QueryCtx>(executor_);
    std::shared_ptr<TempDirectoryPath> spillDirectory;
    if (injectSpill) {
      spillDirectory = exec::test::TempDirectoryPath::create();
      builder.spillDirectory(spillDirectory->path);
      config(core::QueryConfig::kSpillEnabled, "true");
      config(core::QueryConfig::kMaxSpillLevel, std::to_string(maxSpillLevel));
      config(core::QueryConfig::kJoinSpillEnabled, "true");
      // Disable write buffering to ease test verification. For example, we want
      // many spilled vectors in a spilled file to trigger recursive spilling.
      config(core::QueryConfig::kSpillWriteBufferSize, std::to_string(0));
      config(core::QueryConfig::kTestingSpillPct, "100");
    } else if (spillMemoryThreshold_ != 0) {
      spillDirectory = exec::test::TempDirectoryPath::create();
      builder.spillDirectory(spillDirectory->path);
      config(core::QueryConfig::kSpillEnabled, "true");
      config(core::QueryConfig::kMaxSpillLevel, std::to_string(maxSpillLevel));
      config(core::QueryConfig::kJoinSpillEnabled, "true");
      config(
          core::QueryConfig::kJoinSpillMemoryThreshold,
          std::to_string(spillMemoryThreshold_));
    } else if (!spillDirectory_.empty()) {
      builder.spillDirectory(spillDirectory_);
      config(core::QueryConfig::kSpillEnabled, "true");
      config(core::QueryConfig::kJoinSpillEnabled, "true");
    } else {
      config(core::QueryConfig::kSpillEnabled, "false");
    }
    config(
        core::QueryConfig::kHashProbeFinishEarlyOnEmptyBuild,
        hashProbeFinishEarlyOnEmptyBuild_ ? "true" : "false");
    if (maxDriverYieldTimeMs != 0) {
      config(
          core::QueryConfig::kDriverCpuTimeSliceLimitMs,
          std::to_string(maxDriverYieldTimeMs));
    }

    if (!configs_.empty()) {
      auto configCopy = configs_;
      queryCtx->testingOverrideConfigUnsafe(std::move(configCopy));
    }
    if (queryPool_ != nullptr) {
      queryCtx->testingOverrideMemoryPool(queryPool_);
    }
    builder.queryCtx(queryCtx);

    SCOPED_TRACE(
        injectSpill ? fmt::format("With Max Spill Level: {}", maxSpillLevel)
                    : "Without Spill");
    ASSERT_EQ(memory::spillMemoryPool()->stats().currentBytes, 0);
    const uint64_t peakSpillMemoryUsage =
        memory::spillMemoryPool()->stats().peakBytes;
    auto task = builder.assertResults(referenceQuery_);
    const auto statsPair = taskSpilledStats(*task);
    if (injectSpill) {
      if (checkSpillStats_) {
        ASSERT_GT(statsPair.first.spilledRows, 0);
        ASSERT_GT(statsPair.second.spilledRows, 0);
        ASSERT_GT(statsPair.first.spilledBytes, 0);
        ASSERT_GT(statsPair.second.spilledBytes, 0);
        ASSERT_GT(statsPair.first.spilledInputBytes, 0);
        ASSERT_GT(statsPair.second.spilledInputBytes, 0);
        ASSERT_GT(statsPair.first.spilledPartitions, 0);
        ASSERT_GT(statsPair.second.spilledPartitions, 0);
        ASSERT_GT(statsPair.first.spilledFiles, 0);
        ASSERT_GT(statsPair.second.spilledFiles, 0);
        if (maxSpillLevel != -1) {
          ASSERT_EQ(maxHashBuildSpillLevel(*task), maxSpillLevel);
        }
        verifyTaskSpilledRuntimeStats(*task, true);
      }
      if (statsPair.first.spilledBytes > 0 &&
          memory::spillMemoryPool()->trackUsage()) {
        ASSERT_GT(memory::spillMemoryPool()->stats().peakBytes, 0);
        ASSERT_GE(
            memory::spillMemoryPool()->stats().peakBytes, peakSpillMemoryUsage);
      }
      // NOTE: if 'spillDirectory_' is not empty and spill threshold is not
      // set, the test might trigger spilling by its own.
    } else if (spillDirectory_.empty() && spillMemoryThreshold_ == 0) {
      ASSERT_EQ(statsPair.first.spilledRows, 0);
      ASSERT_EQ(statsPair.first.spilledInputBytes, 0);
      ASSERT_EQ(statsPair.first.spilledBytes, 0);
      ASSERT_EQ(statsPair.first.spilledPartitions, 0);
      ASSERT_EQ(statsPair.first.spilledFiles, 0);
      ASSERT_EQ(statsPair.second.spilledRows, 0);
      ASSERT_EQ(statsPair.second.spilledBytes, 0);
      ASSERT_EQ(statsPair.second.spilledBytes, 0);
      ASSERT_EQ(statsPair.second.spilledPartitions, 0);
      ASSERT_EQ(statsPair.second.spilledFiles, 0);
      verifyTaskSpilledRuntimeStats(*task, false);
    }
    // Customized test verification.
    if (testVerifier_ != nullptr) {
      testVerifier_(task, injectSpill);
    }
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
    ASSERT_EQ(memory::spillMemoryPool()->stats().currentBytes, 0);
  }

  VectorFuzzer::Options fuzzerOpts_;
  memory::MemoryPool& pool_;
  DuckDbQueryRunner& duckDbQueryRunner_;
  folly::Executor* executor_;

  int32_t numDrivers_{1};
  core::JoinType joinType_{core::JoinType::kInner};
  bool nullAware_{false};
  std::string referenceQuery_;

  RowTypePtr probeType_;
  std::vector<std::string> probeKeys_;
  RowTypePtr buildType_;
  std::vector<std::string> buildKeys_;

  std::string probeFilter_;
  std::vector<std::string> probeProjections_;
  std::vector<RowVectorPtr> probeVectors_;
  std::vector<RowVectorPtr> allProbeVectors_;
  std::string buildFilter_;
  std::vector<std::string> buildProjections_;
  std::vector<RowVectorPtr> buildVectors_;
  std::vector<RowVectorPtr> allBuildVectors_;
  std::string joinFilter_;
  std::vector<std::string> joinOutputLayout_;
  std::vector<std::string> outputProjections_;

  uint64_t spillMemoryThreshold_{0};
  bool injectSpill_{true};
  // If not set, then the test will run the test with different settings:
  // 0, 2.
  std::optional<int32_t> maxSpillLevel_;
  bool checkSpillStats_{true};

  std::shared_ptr<memory::MemoryPool> queryPool_;
  std::string spillDirectory_;
  bool hashProbeFinishEarlyOnEmptyBuild_{true};

  SplitInput inputSplits_;
  std::function<SplitInput()> makeInputSplits_;
  core::PlanNodePtr planNode_;
  std::unordered_map<std::string, std::string> configs_;

  JoinResultsVerifier testVerifier_{};
};

class HashJoinTest : public HiveConnectorTestBase {
 protected:
  HashJoinTest() : HashJoinTest(TestParam(1)) {}

  explicit HashJoinTest(const TestParam& param)
      : numDrivers_(param.numDrivers) {}

  void SetUp() override {
    HiveConnectorTestBase::SetUp();

    probeType_ =
        ROW({{"t_k1", INTEGER()}, {"t_k2", VARCHAR()}, {"t_v1", VARCHAR()}});
    buildType_ =
        ROW({{"u_k1", INTEGER()}, {"u_k2", VARCHAR()}, {"u_v1", INTEGER()}});
    fuzzerOpts_ = {
        .vectorSize = 1024,
        .nullRatio = 0,
        .stringLength = 1024,
        .stringVariableLength = false,
        .allowLazyVector = false};
  }

  // Make splits with each plan node having a number of source files.
  SplitInput makeSpiltInput(
      const std::vector<core::PlanNodeId>& nodeIds,
      const std::vector<std::vector<std::shared_ptr<TempFilePath>>>& files) {
    VELOX_CHECK_EQ(nodeIds.size(), files.size());
    SplitInput splitInput;
    for (int i = 0; i < nodeIds.size(); ++i) {
      std::vector<exec::Split> splits;
      splits.reserve(files[i].size());
      for (const auto& file : files[i]) {
        splits.push_back(exec::Split(makeHiveConnectorSplit(file->path)));
      }
      splitInput.emplace(nodeIds[i], std::move(splits));
    }
    return splitInput;
  }

  static uint64_t getInputPositions(
      const std::shared_ptr<Task>& task,
      int operatorIndex) {
    auto stats = task->taskStats().pipelineStats.front().operatorStats;
    return stats[operatorIndex].inputPositions;
  }

  static uint64_t getOutputPositions(
      const std::shared_ptr<Task>& task,
      const std::string& operatorType) {
    uint64_t count = 0;
    for (const auto& pipelineStat : task->taskStats().pipelineStats) {
      for (const auto& operatorStat : pipelineStat.operatorStats) {
        if (operatorStat.operatorType == operatorType) {
          count += operatorStat.outputPositions;
        }
      }
    }
    return count;
  }

  static RuntimeMetric getFiltersProduced(
      const std::shared_ptr<Task>& task,
      int operatorIndex) {
    return getOperatorRuntimeStats(
        task, operatorIndex, "dynamicFiltersProduced");
  }

  static RuntimeMetric getFiltersAccepted(
      const std::shared_ptr<Task>& task,
      int operatorIndex) {
    return getOperatorRuntimeStats(
        task, operatorIndex, "dynamicFiltersAccepted");
  }

  static RuntimeMetric getReplacedWithFilterRows(
      const std::shared_ptr<Task>& task,
      int operatorIndex) {
    return getOperatorRuntimeStats(
        task, operatorIndex, "replacedWithDynamicFilterRows");
  }

  static RuntimeMetric getOperatorRuntimeStats(
      const std::shared_ptr<Task>& task,
      int32_t operatorIndex,
      const std::string& statsName) {
    auto stats = task->taskStats().pipelineStats.front().operatorStats;
    return stats[operatorIndex].runtimeStats[statsName];
  }

  static core::JoinType flipJoinType(core::JoinType joinType) {
    switch (joinType) {
      case core::JoinType::kInner:
        return joinType;
      case core::JoinType::kLeft:
        return core::JoinType::kRight;
      case core::JoinType::kRight:
        return core::JoinType::kLeft;
      case core::JoinType::kFull:
        return joinType;
      case core::JoinType::kLeftSemiFilter:
        return core::JoinType::kRightSemiFilter;
      case core::JoinType::kLeftSemiProject:
        return core::JoinType::kRightSemiProject;
      case core::JoinType::kRightSemiFilter:
        return core::JoinType::kLeftSemiFilter;
      case core::JoinType::kRightSemiProject:
        return core::JoinType::kLeftSemiProject;
      default:
        VELOX_FAIL("Cannot flip join type: {}", core::joinTypeName(joinType));
    }
  }

  static core::PlanNodePtr flipJoinSides(const core::PlanNodePtr& plan) {
    auto joinNode = std::dynamic_pointer_cast<const core::HashJoinNode>(plan);
    VELOX_CHECK_NOT_NULL(joinNode);
    return std::make_shared<core::HashJoinNode>(
        joinNode->id(),
        flipJoinType(joinNode->joinType()),
        joinNode->isNullAware(),
        joinNode->rightKeys(),
        joinNode->leftKeys(),
        joinNode->filter(),
        joinNode->sources()[1],
        joinNode->sources()[0],
        joinNode->outputType());
  }

  static void reclaimAndRestoreCapacity(
      const Operator* op,
      uint64_t targetBytes,
      memory::MemoryReclaimer::Stats& reclaimerStats) {
    const auto oldCapacity = op->pool()->capacity();
    op->pool()->reclaim(targetBytes, 0, reclaimerStats);
    dynamic_cast<memory::MemoryPoolImpl*>(op->pool())
        ->testingSetCapacity(oldCapacity);
  }

  const int32_t numDrivers_;

  // The default left and right table types used for test.
  RowTypePtr probeType_;
  RowTypePtr buildType_;
  VectorFuzzer::Options fuzzerOpts_;

  memory::MemoryReclaimer::Stats reclaimerStats_;
  friend class HashJoinBuilder;
};

class MultiThreadedHashJoinTest
    : public HashJoinTest,
      public testing::WithParamInterface<TestParam> {
 public:
  MultiThreadedHashJoinTest() : HashJoinTest(GetParam()) {}

  static std::vector<TestParam> getTestParams() {
    return std::vector<TestParam>({TestParam{1}, TestParam{3}});
  }
};

TEST_P(MultiThreadedHashJoinTest, bigintArray) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t.t_k0 = u.u_k0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, outOfJoinKeyColumnOrder) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeType(probeType_)
      .probeKeys({"t_k2"})
      .probeVectors(5, 10)
      .buildType(buildType_)
      .buildKeys({"u_k2"})
      .buildVectors(5, 15)
      .joinOutputLayout({"t_k1", "t_k2", "u_k1", "u_k2", "u_v1"})
      .referenceQuery(
          "SELECT t_k1, t_k2, u_k1, u_k2, u_v1 FROM t, u WHERE t_k2 = u_k2")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, emptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (const auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .keyTypes({BIGINT()})
        .probeVectors(1600, 5)
        .buildVectors(0, 5)
        .referenceQuery(
            "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t_k0 = u_k0")
        .checkSpillStats(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
          // Check the hash probe has processed probe input rows.
          if (finishOnEmpty) {
            ASSERT_EQ(getInputPositions(task, 1), 0);
          } else {
            ASSERT_GT(getInputPositions(task, 1), 0);
          }
        })
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, emptyProbe) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT()})
      .probeVectors(0, 5)
      .buildVectors(1500, 5)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t_k0 = u_k0")
      .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
        const auto statsPair = taskSpilledStats(*task);
        if (hasSpill) {
          ASSERT_GT(statsPair.first.spilledRows, 0);
          ASSERT_GT(statsPair.first.spilledBytes, 0);
          ASSERT_GT(statsPair.first.spilledPartitions, 0);
          ASSERT_GT(statsPair.first.spilledFiles, 0);
          // There is no spilling at empty probe side.
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_GT(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
        } else {
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
        }
      })
      .run();
}

TEST_P(MultiThreadedHashJoinTest, emptyProbeWithSpillMemoryThreshold) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT()})
      .probeVectors(0, 5)
      .buildVectors(1500, 5)
      .injectSpill(false)
      .spillMemoryThreshold(1)
      .maxSpillLevel(0)
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t_k0 = u_k0")
      .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
        const auto statsPair = taskSpilledStats(*task);
        ASSERT_GT(statsPair.first.spilledRows, 0);
        ASSERT_GT(statsPair.first.spilledBytes, 0);
        ASSERT_GT(statsPair.first.spilledPartitions, 0);
        ASSERT_GT(statsPair.first.spilledFiles, 0);
        ASSERT_EQ(statsPair.second.spilledRows, 0);
        ASSERT_EQ(statsPair.second.spilledBytes, 0);
        ASSERT_GT(statsPair.second.spilledPartitions, 0);
        ASSERT_EQ(statsPair.second.spilledFiles, 0);
      })
      .run();
}

DEBUG_ONLY_TEST_P(
    MultiThreadedHashJoinTest,
    raceBetweenTaskTerminationAndThesholdTriggeredSpill) {
  std::atomic<Task*> task{nullptr};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::addInput",
      std::function<void(Operator*)>([&](Operator* op) {
        if (op->operatorType() != "HashBuild") {
          return;
        }
        task = op->testingOperatorCtx()->task().get();
      }));
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::SpillOperatorGroup::runSpill",
      std::function<void(Operator*)>([&](Operator* op) {
        ASSERT_TRUE(task.load() != nullptr);
        task.load()->requestAbort();
        // Wait a bit to ensure the other peer hash build drivers have been
        // closed.
        std::this_thread::sleep_for(std::chrono::milliseconds(1'000));
      }));

  VectorFuzzer fuzzer({.vectorSize = 10}, pool());
  const int32_t numBuildVectors = 2;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    auto vector = fuzzer.fuzzRow(buildType_);
    // Build the build vector with the same join key to make sure there is only
    // one hash build operator running.
    vector->childAt(0) = makeFlatVector<int32_t>(
        vector->size(), [](auto /*unused*/) { return 1; });
    buildVectors.push_back(std::move(vector));
  }
  const int32_t numProbeVectors = 2;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors, true)
                  .hashJoin(
                      {"t_k1"},
                      {"u_k1"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors, true)
                          // NOTE: add local partition here to ensure all the
                          // build inputs go to the same hash build operator.
                          .localPartition({"u_k1"})
                          .planNode(),
                      "",
                      {"t_k1", "t_k2"},
                      core::JoinType::kInner)
                  .planNode();

  VELOX_ASSERT_THROW(
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(plan)
          .injectSpill(false)
          .checkSpillStats(false)
          .spillMemoryThreshold(1)
          .maxSpillLevel(0)
          .numDrivers(numDrivers_)
          .referenceQuery(
              "SELECT t.t_k1, t.t_k2 from t, u WHERE t.t_k1 = u.u_k1")
          .run(),
      "Aborted for external error");
}

TEST_P(MultiThreadedHashJoinTest, normalizedKey) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT(), VARCHAR()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_k1, t_data, u_k0, u_k1, u_data FROM t, u WHERE t_k0 = u_k0 AND t_k1 = u_k1")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, normalizedKeyOverflow) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .keyTypes({BIGINT(), VARCHAR(), BIGINT(), BIGINT(), BIGINT(), BIGINT()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_k1, t_k2, t_k3, t_k4, t_k5, t_data, u_k0, u_k1, u_k2, u_k3, u_k4, u_k5, u_data FROM t, u WHERE t_k0 = u_k0 AND t_k1 = u_k1 AND t_k2 = u_k2 AND t_k3 = u_k3 AND t_k4 = u_k4 AND t_k5 = u_k5")
      .run();
}

DEBUG_ONLY_TEST_P(MultiThreadedHashJoinTest, parallelJoinBuildCheck) {
  std::atomic<bool> isParallelBuild{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashTable::parallelJoinBuild",
      std::function<void(void*)>([&](void*) { isParallelBuild = true; }));
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT(), VARCHAR()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_k1, t_data, u_k0, u_k1, u_data FROM t, u WHERE t_k0 = u_k0 AND t_k1 = u_k1")
      .injectSpill(false)
      .run();
  ASSERT_EQ(numDrivers_ == 1, !isParallelBuild);
}

DEBUG_ONLY_TEST_P(
    MultiThreadedHashJoinTest,
    raceBetweenTaskTerminateAndTableBuild) {
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashBuild::finishHashBuild",
      std::function<void(Operator*)>([&](Operator* op) {
        auto task = op->testingOperatorCtx()->task();
        task->requestAbort();
      }));
  VELOX_ASSERT_THROW(
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .numDrivers(numDrivers_)
          .keyTypes({BIGINT(), VARCHAR()})
          .probeVectors(1600, 5)
          .buildVectors(1500, 5)
          .referenceQuery(
              "SELECT t_k0, t_k1, t_data, u_k0, u_k1, u_data FROM t, u WHERE t_k0 = u_k0 AND t_k1 = u_k1")
          .injectSpill(false)
          .run(),
      "Aborted for external error");
}

TEST_P(MultiThreadedHashJoinTest, allTypes) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .keyTypes(
          {BIGINT(),
           VARCHAR(),
           REAL(),
           DOUBLE(),
           INTEGER(),
           SMALLINT(),
           TINYINT()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_k1, t_k2, t_k3, t_k4, t_k5, t_k6, t_data, u_k0, u_k1, u_k2, u_k3, u_k4, u_k5, u_k6, u_data FROM t, u WHERE t_k0 = u_k0 AND t_k1 = u_k1 AND t_k2 = u_k2 AND t_k3 = u_k3 AND t_k4 = u_k4 AND t_k5 = u_k5 AND t_k6 = u_k6")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, filter) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .joinFilter("((t_k0 % 100) + (u_k0 % 100)) % 40 < 20")
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t_k0 = u_k0 AND ((t_k0 % 100) + (u_k0 % 100)) % 40 < 20")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, nullAwareAntiJoinWithNull) {
  struct {
    double probeNullRatio;
    double buildNullRatio;

    std::string debugString() const {
      return fmt::format(
          "probeNullRatio: {}, buildNullRatio: {}",
          probeNullRatio,
          buildNullRatio);
    }
  } testSettings[] = {
      {0.0, 1.0}, {0.0, 0.1}, {0.1, 1.0}, {0.1, 0.1}, {1.0, 1.0}, {1.0, 0.1}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    std::vector<RowVectorPtr> probeVectors =
        makeBatches(5, 3, probeType_, pool_.get(), testData.probeNullRatio);

    // The first half number of build batches having no nulls to trigger it
    // later during the processing.
    std::vector<RowVectorPtr> buildVectors = mergeBatches(
        makeBatches(5, 6, buildType_, pool_.get(), 0.0),
        makeBatches(5, 6, buildType_, pool_.get(), testData.buildNullRatio));

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeType(probeType_)
        .probeKeys({"t_k2"})
        .probeVectors(std::move(probeVectors))
        .buildType(buildType_)
        .buildKeys({"u_k2"})
        .buildVectors(std::move(buildVectors))
        .joinType(core::JoinType::kAnti)
        .nullAware(true)
        .joinOutputLayout({"t_k1", "t_k2"})
        .referenceQuery(
            "SELECT t_k1, t_k2 FROM t WHERE t.t_k2 NOT IN (SELECT u_k2 FROM u)")
        // NOTE: we might not trigger spilling at build side if we detect the
        // null join key in the build rows early.
        .checkSpillStats(false)
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, rightSemiJoinFilterWithLargeOutput) {
  // Build the identical left and right vectors to generate large join
  // outputs.
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(4, [&](uint32_t /*unused*/) {
        return makeRowVector(
            {"t0", "t1"},
            {makeFlatVector<int32_t>(2048, [](auto row) { return row; }),
             makeFlatVector<int32_t>(2048, [](auto row) { return row; })});
      });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(4, [&](uint32_t /*unused*/) {
        return makeRowVector(
            {"u0", "u1"},
            {makeFlatVector<int32_t>(2048, [](auto row) { return row; }),
             makeFlatVector<int32_t>(2048, [](auto row) { return row; })});
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"t0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u0"})
      .buildVectors(std::move(buildVectors))
      .joinType(core::JoinType::kRightSemiFilter)
      .joinOutputLayout({"u1"})
      .referenceQuery("SELECT u.u1 FROM u WHERE u.u0 IN (SELECT t0 FROM t)")
      .run();
}

/// Test hash join where build-side keys come from a small range and allow for
/// array-based lookup instead of a hash table.
TEST_P(MultiThreadedHashJoinTest, arrayBasedLookup) {
  auto oddIndices = makeIndices(500, [](auto i) { return 2 * i + 1; });

  std::vector<RowVectorPtr> probeVectors = {
      // Join key vector is flat.
      makeRowVector({
          makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
          makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
      }),
      // Join key vector is constant. There is a match in the build side.
      makeRowVector({
          makeConstant(4, 2'000),
          makeFlatVector<int64_t>(2'000, [](auto row) { return row; }),
      }),
      // Join key vector is constant. There is no match.
      makeRowVector({
          makeConstant(5, 2'000),
          makeFlatVector<int64_t>(2'000, [](auto row) { return row; }),
      }),
      // Join key vector is a dictionary.
      makeRowVector({
          wrapInDictionary(
              oddIndices,
              500,
              makeFlatVector<int32_t>(1'000, [](auto row) { return row * 4; })),
          makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
      })};

  // 100 key values in [0, 198] range.
  std::vector<RowVectorPtr> buildVectors = {
      makeRowVector(
          {makeFlatVector<int32_t>(100, [](auto row) { return row / 2; })}),
      makeRowVector(
          {makeFlatVector<int32_t>(100, [](auto row) { return row * 2; })}),
      makeRowVector(
          {makeFlatVector<int32_t>(100, [](auto row) { return row; })})};

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"c0"})
      .buildVectors(std::move(buildVectors))
      .joinOutputLayout({"c1"})
      .outputProjections({"c1 + 1"})
      .referenceQuery("SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0")
      .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
        if (hasSpill) {
          return;
        }
        auto joinStats = task->taskStats()
                             .pipelineStats.back()
                             .operatorStats.back()
                             .runtimeStats;
        ASSERT_EQ(151, joinStats["distinctKey0"].sum);
        ASSERT_EQ(200, joinStats["rangeKey0"].sum);
      })
      .run();
}

TEST_P(MultiThreadedHashJoinTest, joinSidesDifferentSchema) {
  // In this join, the tables have different schema. LHS table t has schema
  // {INTEGER, VARCHAR, INTEGER}. RHS table u has schema {INTEGER, REAL,
  // INTEGER}. The filter predicate uses
  // a column from the right table  before the left and the corresponding
  // columns at the same channel number(1) have different types. This has been
  // a source of crashes in the join logic.
  size_t batchSize = 100;

  std::vector<std::string> stringVector = {"aaa", "bbb", "ccc", "ddd", "eee"};
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
            makeFlatVector<StringView>(
                batchSize,
                [&](auto row) {
                  return StringView(stringVector[row % stringVector.size()]);
                }),
            makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
        });
      });
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
            makeFlatVector<double>(
                batchSize, [](auto row) { return row * 5.0; }),
            makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
        });
      });

  // In this hash join the 2 tables have a common key which is the
  // first channel in both tables.
  const std::string referenceQuery =
      "SELECT t.c0 * t.c2/2 FROM "
      "  t, u "
      "  WHERE t.c0 = u.c0 AND "
      // TODO: enable ltrim test after the race condition in expression
      // execution gets fixed.
      //"  u.c2 > 10 AND ltrim(t.c1) = 'aaa'";
      "  u.c2 > 10";

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"t_c0"})
      .probeVectors(std::move(probeVectors))
      .probeProjections({"c0 AS t_c0", "c1 AS t_c1", "c2 AS t_c2"})
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1", "c2 AS u_c2"})
      //.joinFilter("u_c2 > 10 AND ltrim(t_c1) == 'aaa'")
      .joinFilter("u_c2 > 10")
      .joinOutputLayout({"t_c0", "t_c2"})
      .outputProjections({"t_c0 * t_c2/2"})
      .referenceQuery(referenceQuery)
      .run();
}

TEST_P(MultiThreadedHashJoinTest, innerJoinWithEmptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    std::vector<RowVectorPtr> probeVectors = makeBatches(5, [&](int32_t batch) {
      return makeRowVector({
          makeFlatVector<int32_t>(
              123,
              [batch](auto row) { return row * 11 / std::max(batch, 1); },
              nullEvery(13)),
          makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
      });
    });
    std::vector<RowVectorPtr> buildVectors =
        makeBatches(10, [&](int32_t batch) {
          return makeRowVector({makeFlatVector<int32_t>(
              123,
              [batch](auto row) { return row % std::max(batch, 1); },
              nullEvery(7))});
        });

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"c0"})
        .buildVectors(std::move(buildVectors))
        .buildFilter("c0 < 0")
        .joinOutputLayout({"c1"})
        .referenceQuery("SELECT null LIMIT 0")
        .checkSpillStats(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
          ASSERT_EQ(maxHashBuildSpillLevel(*task), -1);
          // Check the hash probe has processed probe input rows.
          if (finishOnEmpty) {
            ASSERT_EQ(getInputPositions(task, 1), 0);
          } else {
            ASSERT_GT(getInputPositions(task, 1), 0);
          }
        })
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, leftSemiJoinFilter) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeType(probeType_)
      .probeVectors(174, 5)
      .probeKeys({"t_k1"})
      .buildType(buildType_)
      .buildVectors(133, 4)
      .buildKeys({"u_k1"})
      .joinType(core::JoinType::kLeftSemiFilter)
      .joinOutputLayout({"t_k2"})
      .referenceQuery("SELECT t_k2 FROM t WHERE t_k1 IN (SELECT u_k1 FROM u)")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, leftSemiJoinFilterWithEmptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (const auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    std::vector<RowVectorPtr> probeVectors =
        makeBatches(10, [&](int32_t /*unused*/) {
          return makeRowVector({
              makeFlatVector<int32_t>(
                  1'234, [](auto row) { return row % 11; }, nullEvery(13)),
              makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
          });
        });
    std::vector<RowVectorPtr> buildVectors =
        makeBatches(10, [&](int32_t /*unused*/) {
          return makeRowVector({
              makeFlatVector<int32_t>(
                  123, [](auto row) { return row % 5; }, nullEvery(7)),
          });
        });

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"c0"})
        .buildVectors(std::move(buildVectors))
        .joinType(core::JoinType::kLeftSemiFilter)
        .joinFilter("c0 < 0")
        .joinOutputLayout({"c1"})
        .referenceQuery(
            "SELECT t.c1 FROM t WHERE t.c0 IN (SELECT c0 FROM u WHERE c0 < 0)")
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, leftSemiJoinFilterWithExtraFilter) {
  std::vector<RowVectorPtr> probeVectors = makeBatches(5, [&](int32_t batch) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeFlatVector<int32_t>(
                250, [batch](auto row) { return row % (11 + batch); }),
            makeFlatVector<int32_t>(
                250, [batch](auto row) { return row * batch; }),
        });
  });

  std::vector<RowVectorPtr> buildVectors = makeBatches(5, [&](int32_t batch) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeFlatVector<int32_t>(
                123, [batch](auto row) { return row % (5 + batch); }),
            makeFlatVector<int32_t>(
                123, [batch](auto row) { return row * batch; }),
        });
  });

  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kLeftSemiFilter)
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(
            "SELECT t.* FROM t WHERE EXISTS (SELECT u0 FROM u WHERE t0 = u0)")
        .run();
  }

  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kLeftSemiFilter)
        .joinFilter("t1 != u1")
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(
            "SELECT t.* FROM t WHERE EXISTS (SELECT u0, u1 FROM u WHERE t0 = u0 AND t1 <> u1)")
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, rightSemiJoinFilter) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeType(probeType_)
      .probeVectors(133, 3)
      .probeKeys({"t_k1"})
      .buildType(buildType_)
      .buildVectors(174, 4)
      .buildKeys({"u_k1"})
      .joinType(core::JoinType::kRightSemiFilter)
      .joinOutputLayout({"u_k2"})
      .referenceQuery("SELECT u_k2 FROM u WHERE u_k1 IN (SELECT t_k1 FROM t)")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, rightSemiJoinFilterWithEmptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (const auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    // probeVectors size is greater than buildVector size.
    std::vector<RowVectorPtr> probeVectors =
        makeBatches(5, [&](uint32_t /*unused*/) {
          return makeRowVector(
              {"t0", "t1"},
              {makeFlatVector<int32_t>(
                   431, [](auto row) { return row % 11; }, nullEvery(13)),
               makeFlatVector<int32_t>(431, [](auto row) { return row; })});
        });

    std::vector<RowVectorPtr> buildVectors =
        makeBatches(5, [&](uint32_t /*unused*/) {
          return makeRowVector(
              {"u0", "u1"},
              {
                  makeFlatVector<int32_t>(
                      434, [](auto row) { return row % 5; }, nullEvery(7)),
                  makeFlatVector<int32_t>(434, [](auto row) { return row; }),
              });
        });

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(buildVectors))
        .buildFilter("u0 < 0")
        .joinType(core::JoinType::kRightSemiFilter)
        .joinOutputLayout({"u1"})
        .referenceQuery(
            "SELECT u.u1 FROM u WHERE u.u0 IN (SELECT t0 FROM t) AND u.u0 < 0")
        .checkSpillStats(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
          ASSERT_EQ(maxHashBuildSpillLevel(*task), -1);
          // Check the hash probe has processed probe input rows.
          if (finishOnEmpty) {
            ASSERT_EQ(getInputPositions(task, 1), 0);
          } else {
            ASSERT_GT(getInputPositions(task, 1), 0);
          }
        })
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, rightSemiJoinFilterWithAllMatches) {
  // Make build side larger to test all rows are returned.
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(3, [&](uint32_t /*unused*/) {
        return makeRowVector(
            {"t0", "t1"},
            {
                makeFlatVector<int32_t>(
                    123, [](auto row) { return row % 5; }, nullEvery(7)),
                makeFlatVector<int32_t>(123, [](auto row) { return row; }),
            });
      });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](uint32_t /*unused*/) {
        return makeRowVector(
            {"u0", "u1"},
            {makeFlatVector<int32_t>(
                 314, [](auto row) { return row % 11; }, nullEvery(13)),
             makeFlatVector<int32_t>(314, [](auto row) { return row; })});
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"t0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u0"})
      .buildVectors(std::move(buildVectors))
      .joinType(core::JoinType::kRightSemiFilter)
      .joinOutputLayout({"u1"})
      .referenceQuery("SELECT u.u1 FROM u WHERE u.u0 IN (SELECT t0 FROM t)")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, rightSemiJoinFilterWithExtraFilter) {
  auto probeVectors = makeBatches(4, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeFlatVector<int32_t>(345, [](auto row) { return row; }),
            makeFlatVector<int32_t>(345, [](auto row) { return row; }),
        });
  });

  auto buildVectors = makeBatches(4, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeFlatVector<int32_t>(250, [](auto row) { return row; }),
            makeFlatVector<int32_t>(250, [](auto row) { return row; }),
        });
  });

  // Always true filter.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kRightSemiFilter)
        .joinFilter("t1 > -1")
        .joinOutputLayout({"u0", "u1"})
        .referenceQuery(
            "SELECT u.* FROM u WHERE EXISTS (SELECT t0 FROM t WHERE u0 = t0 AND t1 > -1)")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          ASSERT_EQ(
              getOutputPositions(task, "HashProbe"), 200 * 5 * numDrivers_);
        })
        .run();
  }

  // Always false filter.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kRightSemiFilter)
        .joinFilter("t1 > 100000")
        .joinOutputLayout({"u0", "u1"})
        .referenceQuery(
            "SELECT u.* FROM u WHERE EXISTS (SELECT t0 FROM t WHERE u0 = t0 AND t1 > 100000)")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          ASSERT_EQ(getOutputPositions(task, "HashProbe"), 0);
        })
        .run();
  }

  // Selective filter.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kRightSemiFilter)
        .joinFilter("t1 % 5 = 0")
        .joinOutputLayout({"u0", "u1"})
        .referenceQuery(
            "SELECT u.* FROM u WHERE EXISTS (SELECT t0 FROM t WHERE u0 = t0 AND t1 % 5 = 0)")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          ASSERT_EQ(
              getOutputPositions(task, "HashProbe"), 200 / 5 * 5 * numDrivers_);
        })
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, semiFilterOverLazyVectors) {
  auto probeVectors = makeBatches(1, [&](auto /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
            makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
        });
  });

  auto buildVectors = makeBatches(3, [&](auto /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeFlatVector<int32_t>(
                1'000, [](auto row) { return -100 + (row / 5); }),
            makeFlatVector<int64_t>(
                1'000, [](auto row) { return -1000 + (row / 5) * 10; }),
        });
  });

  std::shared_ptr<TempFilePath> probeFile = TempFilePath::create();
  writeToFile(probeFile->path, probeVectors);

  std::shared_ptr<TempFilePath> buildFile = TempFilePath::create();
  writeToFile(buildFile->path, buildVectors);

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  core::PlanNodeId probeScanId;
  core::PlanNodeId buildScanId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(asRowType(probeVectors[0]->type()))
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      {"t0"},
                      {"u0"},
                      PlanBuilder(planNodeIdGenerator)
                          .tableScan(asRowType(buildVectors[0]->type()))
                          .capturePlanNodeId(buildScanId)
                          .planNode(),
                      "",
                      {"t0", "t1"},
                      core::JoinType::kLeftSemiFilter)
                  .planNode();

  SplitInput splitInput = {
      {probeScanId, {exec::Split(makeHiveConnectorSplit(probeFile->path))}},
      {buildScanId, {exec::Split(makeHiveConnectorSplit(buildFile->path))}},
  };

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .inputSplits(splitInput)
      .checkSpillStats(false)
      .referenceQuery("SELECT t0, t1 FROM t WHERE t0 IN (SELECT u0 FROM u)")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .inputSplits(splitInput)
      .checkSpillStats(false)
      .referenceQuery("SELECT t0, t1 FROM t WHERE t0 IN (SELECT u0 FROM u)")
      .run();

  // With extra filter.
  planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  plan = PlanBuilder(planNodeIdGenerator)
             .tableScan(asRowType(probeVectors[0]->type()))
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"t0"},
                 {"u0"},
                 PlanBuilder(planNodeIdGenerator)
                     .tableScan(asRowType(buildVectors[0]->type()))
                     .capturePlanNodeId(buildScanId)
                     .planNode(),
                 "(t1 + u1) % 3 = 0",
                 {"t0", "t1"},
                 core::JoinType::kLeftSemiFilter)
             .planNode();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .inputSplits(splitInput)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1 FROM t WHERE t0 IN (SELECT u0 FROM u WHERE (t1 + u1) % 3 = 0)")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .inputSplits(splitInput)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1 FROM t WHERE t0 IN (SELECT u0 FROM u WHERE (t1 + u1) % 3 = 0)")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, nullAwareAntiJoin) {
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(5, [&](uint32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                1'000, [](auto row) { return row % 11; }, nullEvery(13)),
            makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
        });
      });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](uint32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                1'234, [](auto row) { return row % 5; }, nullEvery(7)),
        });
      });

  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildFilter("c0 IS NOT NULL")
        .joinType(core::JoinType::kAnti)
        .nullAware(true)
        .joinOutputLayout({"c1"})
        .referenceQuery(
            "SELECT t.c1 FROM t WHERE t.c0 NOT IN (SELECT c0 FROM u WHERE c0 IS NOT NULL)")
        .checkSpillStats(false)
        .run();
  }

  // Empty build side.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildFilter("c0 < 0")
        .joinType(core::JoinType::kAnti)
        .nullAware(true)
        .joinOutputLayout({"c1"})
        .referenceQuery(
            "SELECT t.c1 FROM t WHERE t.c0 NOT IN (SELECT c0 FROM u WHERE c0 < 0)")
        .checkSpillStats(false)
        .run();
  }

  // Build side with nulls. Null-aware Anti join always returns nothing.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"c0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kAnti)
        .nullAware(true)
        .joinOutputLayout({"c1"})
        .referenceQuery(
            "SELECT t.c1 FROM t WHERE t.c0 NOT IN (SELECT c0 FROM u)")
        .checkSpillStats(false)
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, nullAwareAntiJoinWithFilter) {
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector(
            {"t0", "t1"},
            {
                makeFlatVector<int32_t>(128, [](auto row) { return row % 11; }),
                makeFlatVector<int32_t>(128, [](auto row) { return row; }),
            });
      });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector(
            {"u0", "u1"},
            {
                makeFlatVector<int32_t>(123, [](auto row) { return row % 5; }),
                makeFlatVector<int32_t>(123, [](auto row) { return row; }),
            });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"t0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u0"})
      .buildVectors(std::move(buildVectors))
      .joinType(core::JoinType::kAnti)
      .nullAware(true)
      .joinFilter("t1 != u1")
      .joinOutputLayout({"t0", "t1"})
      .referenceQuery(
          "SELECT t.* FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE t0 = u0 AND t1 <> u1)")
      .checkSpillStats(false)
      .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
        // Verify spilling is not triggered in case of null-aware anti-join
        // with filter.
        const auto statsPair = taskSpilledStats(*task);
        ASSERT_EQ(statsPair.first.spilledRows, 0);
        ASSERT_EQ(statsPair.first.spilledBytes, 0);
        ASSERT_EQ(statsPair.first.spilledPartitions, 0);
        ASSERT_EQ(statsPair.first.spilledFiles, 0);
        ASSERT_EQ(statsPair.second.spilledRows, 0);
        ASSERT_EQ(statsPair.second.spilledBytes, 0);
        ASSERT_EQ(statsPair.second.spilledPartitions, 0);
        ASSERT_EQ(statsPair.second.spilledFiles, 0);
        verifyTaskSpilledRuntimeStats(*task, false);
        ASSERT_EQ(maxHashBuildSpillLevel(*task), -1);
      })
      .run();
}

TEST_P(MultiThreadedHashJoinTest, nullAwareAntiJoinWithFilterAndEmptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (const auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    auto probeVectors = makeBatches(4, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"t0", "t1"},
          {
              makeNullableFlatVector<int32_t>({std::nullopt, 1, 2}),
              makeFlatVector<int32_t>({0, 1, 2}),
          });
    });
    auto buildVectors = makeBatches(4, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"u0", "u1"},
          {
              makeNullableFlatVector<int32_t>({3, 2, 3}),
              makeFlatVector<int32_t>({0, 2, 3}),
          });
    });

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::vector<RowVectorPtr>(probeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::vector<RowVectorPtr>(buildVectors))
        .buildFilter("u0 < 0")
        .joinType(core::JoinType::kAnti)
        .nullAware(true)
        .joinFilter("u1 > t1")
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(
            "SELECT t.* FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE u0 < 0 AND u.u0 = t.t0)")
        .checkSpillStats(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          // Verify spilling is not triggered in case of null-aware anti-join
          // with filter.
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
          ASSERT_EQ(maxHashBuildSpillLevel(*task), -1);
        })
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, nullAwareAntiJoinWithFilterAndNullKey) {
  auto probeVectors = makeBatches(4, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeNullableFlatVector<int32_t>({std::nullopt, 1, 2}),
            makeFlatVector<int32_t>({0, 1, 2}),
        });
  });
  auto buildVectors = makeBatches(4, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeNullableFlatVector<int32_t>({std::nullopt, 2, 3}),
            makeFlatVector<int32_t>({0, 2, 3}),
        });
  });

  std::vector<std::string> filters({"u1 > t1", "u1 * t1 > 0"});
  for (const std::string& filter : filters) {
    const auto referenceSql = fmt::format(
        "SELECT t.* FROM t WHERE t0 NOT IN (SELECT u0 FROM u WHERE {})",
        filter);

    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kAnti)
        .nullAware(true)
        .joinFilter(filter)
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(referenceSql)
        .checkSpillStats(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          // Verify spilling is not triggered in case of null-aware anti-join
          // with filter.
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
          ASSERT_EQ(maxHashBuildSpillLevel(*task), -1);
        })
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, nullAwareAntiJoinWithFilterOnNullableColumn) {
  const std::string referenceSql =
      "SELECT t.* FROM t WHERE t0 NOT IN (SELECT u0 FROM u WHERE t1 <> u1)";
  const std::string joinFilter = "t1 <> u1";
  {
    SCOPED_TRACE("null filter column");
    auto probeVectors = makeBatches(3, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"t0", "t1"},
          {
              makeFlatVector<int32_t>(200, [](auto row) { return row % 11; }),
              makeFlatVector<int32_t>(200, folly::identity, nullEvery(97)),
          });
    });
    auto buildVectors = makeBatches(3, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"u0", "u1"},
          {
              makeFlatVector<int32_t>(234, [](auto row) { return row % 5; }),
              makeFlatVector<int32_t>(234, folly::identity, nullEvery(91)),
          });
    });
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(buildVectors))
        .joinType(core::JoinType::kAnti)
        .nullAware(true)
        .joinFilter(joinFilter)
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(referenceSql)
        .checkSpillStats(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          // Verify spilling is not triggered in case of null-aware anti-join
          // with filter.
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
          ASSERT_EQ(maxHashBuildSpillLevel(*task), -1);
        })
        .run();
  }

  {
    SCOPED_TRACE("null filter and key column");
    auto probeVectors = makeBatches(3, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"t0", "t1"},
          {
              makeFlatVector<int32_t>(
                  200, [](auto row) { return row % 11; }, nullEvery(23)),
              makeFlatVector<int32_t>(200, folly::identity, nullEvery(29)),
          });
    });
    auto buildVectors = makeBatches(3, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"u0", "u1"},
          {
              makeFlatVector<int32_t>(
                  234, [](auto row) { return row % 5; }, nullEvery(31)),
              makeFlatVector<int32_t>(234, folly::identity, nullEvery(37)),
          });
    });
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(buildVectors))
        .joinType(core::JoinType::kAnti)
        .nullAware(true)
        .joinFilter(joinFilter)
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(referenceSql)
        .checkSpillStats(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          // Verify spilling is not triggered in case of null-aware anti-join
          // with filter.
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
          ASSERT_EQ(maxHashBuildSpillLevel(*task), -1);
        })
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, antiJoin) {
  auto probeVectors = makeBatches(4, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeNullableFlatVector<int32_t>({std::nullopt, 1, 2}),
            makeFlatVector<int32_t>({0, 1, 2}),
        });
  });
  auto buildVectors = makeBatches(4, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeNullableFlatVector<int32_t>({std::nullopt, 2, 3}),
            makeFlatVector<int32_t>({0, 2, 3}),
        });
  });
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"t0"})
      .probeVectors(std::vector<RowVectorPtr>(probeVectors))
      .buildKeys({"u0"})
      .buildVectors(std::vector<RowVectorPtr>(buildVectors))
      .joinType(core::JoinType::kAnti)
      .joinOutputLayout({"t0", "t1"})
      .referenceQuery(
          "SELECT t.* FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE u.u0 = t.t0)")
      .run();

  std::vector<std::string> filters({
      "u1 > t1",
      "u1 * t1 > 0",
      // This filter is true on rows without a match. It should not prevent
      // the row from being returned.
      "coalesce(u1, t1, 0::integer) is not null",
      // This filter throws if evaluated on rows without a match. The join
      // should not evaluate filter on those rows and therefore should not
      // fail.
      "t1 / coalesce(u1, 0::integer) is not null",
      // This filter triggers memory pool allocation at
      // HashBuild::setupFilterForAntiJoins, which should not be invoked in
      // operator's constructor.
      "contains(array[1, 2, NULL], 1)",
  });
  for (const std::string& filter : filters) {
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::vector<RowVectorPtr>(probeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::vector<RowVectorPtr>(buildVectors))
        .joinType(core::JoinType::kAnti)
        .joinFilter(filter)
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(fmt::format(
            "SELECT t.* FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE u.u0 = t.t0 AND {})",
            filter))
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, antiJoinWithFilterAndEmptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (const auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    auto probeVectors = makeBatches(4, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"t0", "t1"},
          {
              makeNullableFlatVector<int32_t>({std::nullopt, 1, 2}),
              makeFlatVector<int32_t>({0, 1, 2}),
          });
    });
    auto buildVectors = makeBatches(4, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"u0", "u1"},
          {
              makeNullableFlatVector<int32_t>({3, 2, 3}),
              makeFlatVector<int32_t>({0, 2, 3}),
          });
    });

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::vector<RowVectorPtr>(probeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::vector<RowVectorPtr>(buildVectors))
        .buildFilter("u0 < 0")
        .joinType(core::JoinType::kAnti)
        .joinFilter("u1 > t1")
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(
            "SELECT t.* FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE u0 < 0 AND u.u0 = t.t0)")
        .checkSpillStats(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
          ASSERT_EQ(maxHashBuildSpillLevel(*task), -1);
        })
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, leftJoin) {
  // Left side keys are [0, 1, 2,..20].
  // Use 3-rd column as row number to allow for asserting the order of
  // results.
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        77, [](auto row) { return row % 21; }, nullEvery(13)),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        97,
                        [](auto row) { return (row + 3) % 21; },
                        nullEvery(13)),
                    makeFlatVector<int32_t>(97, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(
                        97, [](auto row) { return 97 + row; }),
                });
          }),
      true);

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                73, [](auto row) { return row % 5; }, nullEvery(7)),
            makeFlatVector<int32_t>(
                73, [](auto row) { return -111 + row * 2; }, nullEvery(7)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kLeft)
      //.joinOutputLayout({"row_number", "c0", "c1", "u_c1"})
      .joinOutputLayout({"row_number", "c0", "c1", "u_c0"})
      .referenceQuery(
          "SELECT t.row_number, t.c0, t.c1, u.c0 FROM t LEFT JOIN u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, leftJoinWithEmptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (const auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    // Left side keys are [0, 1, 2,..10].
    // Use 3-rd column as row number to allow for asserting the order of
    // results.
    std::vector<RowVectorPtr> probeVectors = mergeBatches(
        makeBatches(
            3,
            [&](int32_t /*unused*/) {
              return makeRowVector(
                  {"c0", "c1", "row_number"},
                  {
                      makeFlatVector<int32_t>(
                          77, [](auto row) { return row % 11; }, nullEvery(13)),
                      makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                      makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                  });
            }),
        makeBatches(
            2,
            [&](int32_t /*unused*/) {
              return makeRowVector(
                  {"c0", "c1", "row_number"},
                  {
                      makeFlatVector<int32_t>(
                          97,
                          [](auto row) { return (row + 3) % 11; },
                          nullEvery(13)),
                      makeFlatVector<int32_t>(97, [](auto row) { return row; }),
                      makeFlatVector<int32_t>(
                          97, [](auto row) { return 97 + row; }),
                  });
            }),
        true);

    std::vector<RowVectorPtr> buildVectors =
        makeBatches(3, [&](int32_t /*unused*/) {
          return makeRowVector({
              makeFlatVector<int32_t>(
                  73, [](auto row) { return row % 5; }, nullEvery(7)),
              makeFlatVector<int32_t>(
                  73, [](auto row) { return -111 + row * 2; }, nullEvery(7)),
          });
        });

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(buildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .buildFilter("c0 < 0")
        .joinType(core::JoinType::kLeft)
        .joinOutputLayout({"row_number", "c1"})
        .referenceQuery(
            "SELECT t.row_number, t.c1 FROM t LEFT JOIN (SELECT c0 FROM u WHERE c0 < 0) u ON t.c0 = u.c0")
        .checkSpillStats(false)
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, leftJoinWithNoJoin) {
  // Left side keys are [0, 1, 2,..10].
  // Use 3-rd column as row number to allow for asserting the order of
  // results.
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        77, [](auto row) { return row % 11; }, nullEvery(13)),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        97,
                        [](auto row) { return (row + 3) % 11; },
                        nullEvery(13)),
                    makeFlatVector<int32_t>(97, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(
                        97, [](auto row) { return 97 + row; }),
                });
          }),
      true);

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                73, [](auto row) { return row % 5; }, nullEvery(7)),
            makeFlatVector<int32_t>(
                73, [](auto row) { return -111 + row * 2; }, nullEvery(7)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 - 123::INTEGER AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kLeft)
      .joinOutputLayout({"row_number", "c0", "u_c1"})
      .referenceQuery(
          "SELECT t.row_number, t.c0, u.c1 FROM t LEFT JOIN (SELECT c0 - 123::INTEGER AS u_c0, c1 FROM u) u ON t.c0 = u.u_c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, leftJoinWithAllMatch) {
  // Left side keys are [0, 1, 2,..10].
  // Use 3-rd column as row number to allow for asserting the order of
  // results.
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        77, [](auto row) { return row % 11; }, nullEvery(13)),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        97,
                        [](auto row) { return (row + 3) % 11; },
                        nullEvery(13)),
                    makeFlatVector<int32_t>(97, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(
                        97, [](auto row) { return 97 + row; }),
                });
          }),
      true);

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                73, [](auto row) { return row % 5; }, nullEvery(7)),
            makeFlatVector<int32_t>(
                73, [](auto row) { return -111 + row * 2; }, nullEvery(7)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .probeFilter("c0 < 5")
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kLeft)
      .joinOutputLayout({"row_number", "c0", "c1", "u_c1"})
      .referenceQuery(
          "SELECT t.row_number, t.c0, t.c1, u.c1 FROM (SELECT * FROM t WHERE c0 < 5) t LEFT JOIN u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, leftJoinWithFilter) {
  // Left side keys are [0, 1, 2,..10].
  // Use 3-rd column as row number to allow for asserting the order of
  // results.
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        77, [](auto row) { return row % 11; }, nullEvery(13)),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        97,
                        [](auto row) { return (row + 3) % 11; },
                        nullEvery(13)),
                    makeFlatVector<int32_t>(97, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(
                        97, [](auto row) { return 97 + row; }),
                });
          }),
      true);

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                73, [](auto row) { return row % 5; }, nullEvery(7)),
            makeFlatVector<int32_t>(
                73, [](auto row) { return -111 + row * 2; }, nullEvery(7)),
        });
      });

  // Additional filter.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kLeft)
        .joinFilter("(c1 + u_c1) % 2 = 1")
        .joinOutputLayout({"row_number", "c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.row_number, t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 1")
        .run();
  }

  // No rows pass the additional filter.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kLeft)
        .joinFilter("(c1 + u_c1) % 2  = 3")
        .joinOutputLayout({"row_number", "c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.row_number, t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 3")
        .run();
  }
}

/// Tests left join with a filter that may evaluate to true, false or null.
/// Makes sure that null filter results are handled correctly, e.g. as if the
/// filter returned false.
TEST_P(MultiThreadedHashJoinTest, leftJoinWithNullableFilter) {
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          5,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
                makeNullableFlatVector<int32_t>(
                    {10, std::nullopt, 30, std::nullopt, 50}),
            });
          }),
      makeBatches(
          5,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
                makeNullableFlatVector<int32_t>(
                    {std::nullopt, 20, 30, std::nullopt, 50}),
            });
          }),
      true);

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector({makeFlatVector<int32_t>({1, 2, 10, 30, 40})});
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0"})
      .joinType(core::JoinType::kLeft)
      .joinFilter("c1 + u_c0 > 0")
      .joinOutputLayout({"c0", "c1", "u_c0"})
      .referenceQuery(
          "SELECT * FROM t LEFT JOIN u ON (t.c0 = u.c0 AND t.c1 + u.c0 > 0)")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, rightJoin) {
  // Left side keys are [0, 1, 2,..20].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137, [](auto row) { return row % 21; }, nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    234,
                    [](auto row) { return (row + 3) % 21; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(234, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kRight)
      .joinOutputLayout({"c0", "c1", "u_c1"})
      .referenceQuery(
          "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, rightJoinWithEmptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (const auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    // Left side keys are [0, 1, 2,..10].
    std::vector<RowVectorPtr> probeVectors = mergeBatches(
        makeBatches(
            3,
            [&](int32_t /*unused*/) {
              return makeRowVector({
                  makeFlatVector<int32_t>(
                      137, [](auto row) { return row % 11; }, nullEvery(13)),
                  makeFlatVector<int32_t>(137, [](auto row) { return row; }),
              });
            }),
        makeBatches(
            3,
            [&](int32_t /*unused*/) {
              return makeRowVector({
                  makeFlatVector<int32_t>(
                      234,
                      [](auto row) { return (row + 3) % 11; },
                      nullEvery(13)),
                  makeFlatVector<int32_t>(234, [](auto row) { return row; }),
              });
            }),
        true);

    // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
    std::vector<RowVectorPtr> buildVectors =
        makeBatches(3, [&](int32_t /*unused*/) {
          return makeRowVector({
              makeFlatVector<int32_t>(
                  123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
              makeFlatVector<int32_t>(
                  123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
          });
        });

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(buildVectors))
        .buildFilter("c0 > 100")
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kRight)
        .joinOutputLayout({"c1"})
        .referenceQuery("SELECT null LIMIT 0")
        .checkSpillStats(false)
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, rightJoinWithAllMatch) {
  // Left side keys are [0, 1, 2,..20].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137, [](auto row) { return row % 21; }, nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    234,
                    [](auto row) { return (row + 3) % 21; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(234, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildFilter("c0 >= 0")
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kRight)
      .joinOutputLayout({"c0", "c1", "u_c1"})
      .referenceQuery(
          "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN (SELECT * FROM u WHERE c0 >= 0) u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, rightJoinWithFilter) {
  // Left side keys are [0, 1, 2,..20].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137, [](auto row) { return row % 21; }, nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    234,
                    [](auto row) { return (row + 3) % 21; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(234, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  // Filter with passed rows.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kRight)
        .joinFilter("(c1 + u_c1) % 2 = 1")
        .joinOutputLayout({"c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 1")
        .run();
  }

  // Filter without passed rows.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kRight)
        .joinFilter("(c1 + u_c1) % 2 = 3")
        .joinOutputLayout({"c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 3")
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, fullJoin) {
  // Left side keys are [0, 1, 2,..20].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    213, [](auto row) { return row % 21; }, nullEvery(13)),
                makeFlatVector<int32_t>(213, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137,
                    [](auto row) { return (row + 3) % 21; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1,
  // 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kFull)
      .joinOutputLayout({"c0", "c1", "u_c1"})
      .referenceQuery(
          "SELECT t.c0, t.c1, u.c1 FROM t FULL OUTER JOIN u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, fullJoinWithEmptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (const auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    // Left side keys are [0, 1, 2,..10].
    std::vector<RowVectorPtr> probeVectors = mergeBatches(
        makeBatches(
            3,
            [&](int32_t /*unused*/) {
              return makeRowVector({
                  makeFlatVector<int32_t>(
                      213, [](auto row) { return row % 11; }, nullEvery(13)),
                  makeFlatVector<int32_t>(213, [](auto row) { return row; }),
              });
            }),
        makeBatches(
            2,
            [&](int32_t /*unused*/) {
              return makeRowVector({
                  makeFlatVector<int32_t>(
                      137,
                      [](auto row) { return (row + 3) % 11; },
                      nullEvery(13)),
                  makeFlatVector<int32_t>(137, [](auto row) { return row; }),
              });
            }),
        true);

    // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
    std::vector<RowVectorPtr> buildVectors =
        makeBatches(3, [&](int32_t /*unused*/) {
          return makeRowVector({
              makeFlatVector<int32_t>(
                  123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
              makeFlatVector<int32_t>(
                  123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
          });
        });

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(buildVectors))
        .buildFilter("c0 > 100")
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kFull)
        .joinOutputLayout({"c1"})
        .referenceQuery(
            "SELECT t.c1 FROM t FULL OUTER JOIN (SELECT * FROM u WHERE c0 > 100) u ON t.c0 = u.c0")
        .checkSpillStats(false)
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, fullJoinWithNoMatch) {
  // Left side keys are [0, 1, 2,..10].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    213, [](auto row) { return row % 11; }, nullEvery(13)),
                makeFlatVector<int32_t>(213, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137,
                    [](auto row) { return (row + 3) % 11; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildFilter("c0 < 0")
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kFull)
      .joinOutputLayout({"c1"})
      .referenceQuery(
          "SELECT t.c1 FROM t FULL OUTER JOIN (SELECT * FROM u WHERE c0 < 0) u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, fullJoinWithFilters) {
  // Left side keys are [0, 1, 2,..10].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    213, [](auto row) { return row % 11; }, nullEvery(13)),
                makeFlatVector<int32_t>(213, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137,
                    [](auto row) { return (row + 3) % 11; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  // Filter with passed rows.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kFull)
        .joinFilter("(c1 + u_c1) % 2 = 1")
        .joinOutputLayout({"c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.c0, t.c1, u.c1 FROM t FULL OUTER JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 1")
        .run();
  }

  // Filter without passed rows.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kFull)
        .joinFilter("(c1 + u_c1) % 2 = 3")
        .joinOutputLayout({"c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.c0, t.c1, u.c1 FROM t FULL OUTER JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 3")
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, noSpillLevelLimit) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .keyTypes({INTEGER()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t.t_k0 = u.u_k0")
      .maxSpillLevel(-1)
      .config(core::QueryConfig::kSpillStartPartitionBit, "48")
      .config(core::QueryConfig::kJoinSpillPartitionBits, "3")
      .checkSpillStats(false)
      .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
        if (!hasSpill) {
          return;
        }
        ASSERT_EQ(maxHashBuildSpillLevel(*task), 4);
      })
      .run();
}

// Verify that dynamic filter pushed down from null-aware right semi project
// join into table scan doesn't filter out nulls.
TEST_F(HashJoinTest, nullAwareRightSemiProjectOverScan) {
  auto probe = makeRowVector(
      {"t0"},
      {
          makeNullableFlatVector<int32_t>({1, std::nullopt, 2}),
      });

  auto build = makeRowVector(
      {"u0"},
      {
          makeNullableFlatVector<int32_t>({1, 2, 3, std::nullopt}),
      });

  std::shared_ptr<TempFilePath> probeFile = TempFilePath::create();
  writeToFile(probeFile->path, {probe});

  std::shared_ptr<TempFilePath> buildFile = TempFilePath::create();
  writeToFile(buildFile->path, {build});

  createDuckDbTable("t", {probe});
  createDuckDbTable("u", {build});

  core::PlanNodeId probeScanId;
  core::PlanNodeId buildScanId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(asRowType(probe->type()))
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      {"t0"},
                      {"u0"},
                      PlanBuilder(planNodeIdGenerator)
                          .tableScan(asRowType(build->type()))
                          .capturePlanNodeId(buildScanId)
                          .planNode(),
                      "",
                      {"u0", "match"},
                      core::JoinType::kRightSemiProject,
                      true /*nullAware*/)
                  .planNode();

  SplitInput splitInput = {
      {probeScanId, {exec::Split(makeHiveConnectorSplit(probeFile->path))}},
      {buildScanId, {exec::Split(makeHiveConnectorSplit(buildFile->path))}},
  };

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .inputSplits(splitInput)
      .checkSpillStats(false)
      .referenceQuery("SELECT u0, u0 IN (SELECT t0 FROM t) FROM u")
      .run();
}

TEST_F(HashJoinTest, duplicateJoinKeys) {
  auto leftVectors = makeBatches(3, [&](int32_t /*unused*/) {
    return makeRowVector({
        makeNullableFlatVector<int64_t>(
            {1, 2, 2, 3, 3, std::nullopt, 4, 5, 5, 6, 7}),
        makeNullableFlatVector<int64_t>(
            {1, 2, 2, std::nullopt, 3, 3, 4, 5, 5, 6, 8}),
    });
  });

  auto rightVectors = makeBatches(3, [&](int32_t /*unused*/) {
    return makeRowVector({
        makeNullableFlatVector<int64_t>({1, 1, 3, 4, std::nullopt, 5, 7, 8}),
        makeNullableFlatVector<int64_t>({1, 1, 3, 4, 5, std::nullopt, 7, 8}),
    });
  });

  createDuckDbTable("t", leftVectors);
  createDuckDbTable("u", rightVectors);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto assertPlan = [&](const std::vector<std::string>& leftProject,
                        const std::vector<std::string>& leftKeys,
                        const std::vector<std::string>& rightProject,
                        const std::vector<std::string>& rightKeys,
                        const std::vector<std::string>& outputLayout,
                        core::JoinType joinType,
                        const std::string& query) {
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(leftVectors)
                    .project(leftProject)
                    .hashJoin(
                        leftKeys,
                        rightKeys,
                        PlanBuilder(planNodeIdGenerator)
                            .values(rightVectors)
                            .project(rightProject)
                            .planNode(),
                        "",
                        outputLayout,
                        joinType)
                    .planNode();
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(plan)
        .referenceQuery(query)
        .run();
  };

  std::vector<std::pair<core::JoinType, std::string>> joins = {
      {core::JoinType::kInner, "INNER JOIN"},
      {core::JoinType::kLeft, "LEFT JOIN"},
      {core::JoinType::kRight, "RIGHT JOIN"},
      {core::JoinType::kFull, "FULL OUTER JOIN"}};

  for (const auto& [joinType, joinTypeSql] : joins) {
    // Duplicate keys on the build side.
    assertPlan(
        {"c0 AS t0", "c1 as t1"}, // leftProject
        {"t0", "t1"}, // leftKeys
        {"c0 AS u0"}, // rightProject
        {"u0", "u0"}, // rightKeys
        {"t0", "t1", "u0"}, // outputLayout
        joinType,
        "SELECT t.c0, t.c1, u.c0 FROM t " + joinTypeSql +
            " u ON t.c0 = u.c0 and t.c1 = u.c0");
  }

  for (const auto& [joinType, joinTypeSql] : joins) {
    // Duplicated keys on the probe side.
    assertPlan(
        {"c0 AS t0"}, // leftProject
        {"t0", "t0"}, // leftKeys
        {"c0 AS u0", "c1 AS u1"}, // rightProject
        {"u0", "u1"}, // rightKeys
        {"t0", "u0", "u1"}, // outputLayout
        joinType,
        "SELECT t.c0, u.c0, u.c1 FROM t " + joinTypeSql +
            " u ON t.c0 = u.c0 and t.c0 = u.c1");
  }
}

TEST_F(HashJoinTest, semiProject) {
  // Some keys have multiple rows: 2, 3, 5.
  auto probeVectors = makeBatches(3, [&](int32_t /*unused*/) {
    return makeRowVector({
        makeFlatVector<int64_t>({1, 2, 2, 3, 3, 3, 4, 5, 5, 6, 7}),
        makeFlatVector<int64_t>({10, 20, 21, 30, 31, 32, 40, 50, 51, 60, 70}),
    });
  });

  // Some keys are missing: 2, 6.
  // Some have multiple rows: 1, 5.
  // Some keys are not present on probe side: 8.
  auto buildVectors = makeBatches(3, [&](int32_t /*unused*/) {
    return makeRowVector({
        makeFlatVector<int64_t>({1, 1, 3, 4, 5, 5, 7, 8}),
        makeFlatVector<int64_t>({100, 101, 300, 400, 500, 501, 700, 800}),
    });
  });

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors)
                  .project({"c0 AS t0", "c1 AS t1"})
                  .hashJoin(
                      {"t0"},
                      {"u0"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors)
                          .project({"c0 AS u0", "c1 AS u1"})
                          .planNode(),
                      "",
                      {"t0", "t1", "match"},
                      core::JoinType::kLeftSemiProject)
                  .planNode();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery(
          "SELECT t.c0, t.c1, EXISTS (SELECT * FROM u WHERE t.c0 = u.c0) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery(
          "SELECT t.c0, t.c1, EXISTS (SELECT * FROM u WHERE t.c0 = u.c0) FROM t")
      .run();

  // With extra filter.
  planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  plan = PlanBuilder(planNodeIdGenerator)
             .values(probeVectors)
             .project({"c0 AS t0", "c1 AS t1"})
             .hashJoin(
                 {"t0"},
                 {"u0"},
                 PlanBuilder(planNodeIdGenerator)
                     .values(buildVectors)
                     .project({"c0 AS u0", "c1 AS u1"})
                     .planNode(),
                 "t1 * 10 <> u1",
                 {"t0", "t1", "match"},
                 core::JoinType::kLeftSemiProject)
             .planNode();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery(
          "SELECT t.c0, t.c1, EXISTS (SELECT * FROM u WHERE t.c0 = u.c0 AND t.c1 * 10 <> u.c1) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery(
          "SELECT t.c0, t.c1, EXISTS (SELECT * FROM u WHERE t.c0 = u.c0 AND t.c1 * 10 <> u.c1) FROM t")
      .run();

  // Empty build side.
  planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  plan = PlanBuilder(planNodeIdGenerator)
             .values(probeVectors)
             .project({"c0 AS t0", "c1 AS t1"})
             .hashJoin(
                 {"t0"},
                 {"u0"},
                 PlanBuilder(planNodeIdGenerator)
                     .values(buildVectors)
                     .project({"c0 AS u0", "c1 AS u1"})
                     .filter("u0 < 0")
                     .planNode(),
                 "",
                 {"t0", "t1", "match"},
                 core::JoinType::kLeftSemiProject)
             .planNode();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery(
          "SELECT t.c0, t.c1, EXISTS (SELECT * FROM u WHERE u.c0 < 0 AND t.c0 = u.c0) FROM t")
      // NOTE: there is no spilling in empty build test case as all the
      // build-side rows have been filtered out.
      .checkSpillStats(false)
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery(
          "SELECT t.c0, t.c1, EXISTS (SELECT * FROM u WHERE u.c0 < 0 AND t.c0 = u.c0) FROM t")
      // NOTE: there is no spilling in empty build test case as all the
      // build-side rows have been filtered out.
      .checkSpillStats(false)
      .run();
}

TEST_F(HashJoinTest, semiProjectWithNullKeys) {
  // Some keys have multiple rows: 2, 3, 5.
  auto probeVectors = makeBatches(3, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeNullableFlatVector<int64_t>(
                {1, 2, 2, 3, 3, 3, 4, std::nullopt, 5, 5, 6, 7}),
            makeFlatVector<int64_t>(
                {10, 20, 21, 30, 31, 32, 40, -1, 50, 51, 60, 70}),
        });
  });

  // Some keys are missing: 2, 6.
  // Some have multiple rows: 1, 5.
  // Some keys are not present on probe side: 8.
  auto buildVectors = makeBatches(3, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeNullableFlatVector<int64_t>(
                {1, 1, 3, 4, std::nullopt, 5, 5, 7, 8}),
            makeFlatVector<int64_t>(
                {100, 101, 300, 400, -100, 500, 501, 700, 800}),
        });
  });

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto makePlan = [&](bool nullAware,
                      const std::string& probeFilter = "",
                      const std::string& buildFilter = "") {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    return PlanBuilder(planNodeIdGenerator)
        .values(probeVectors)
        .optionalFilter(probeFilter)
        .hashJoin(
            {"t0"},
            {"u0"},
            PlanBuilder(planNodeIdGenerator)
                .values(buildVectors)
                .optionalFilter(buildFilter)
                .planNode(),
            "",
            {"t0", "t1", "match"},
            core::JoinType::kLeftSemiProject,
            nullAware)
        .planNode();
  };

  // Null join keys on both sides.
  auto plan = makePlan(false /*nullAware*/);

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0) FROM t")
      .run();

  plan = makePlan(true /*nullAware*/);

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery("SELECT t0, t1, t0 IN (SELECT u0 FROM u) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery("SELECT t0, t1, t0 IN (SELECT u0 FROM u) FROM t")
      .run();

  // Null join keys on build side-only.
  plan = makePlan(false /*nullAware*/, "t0 IS NOT NULL");

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0) FROM t WHERE t0 IS NOT NULL")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0) FROM t WHERE t0 IS NOT NULL")
      .run();

  plan = makePlan(true /*nullAware*/, "t0 IS NOT NULL");

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u) FROM t WHERE t0 IS NOT NULL")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u) FROM t WHERE t0 IS NOT NULL")
      .run();

  // Null join keys on probe side-only.
  plan = makePlan(false /*nullAware*/, "", "u0 IS NOT NULL");

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0 AND u0 IS NOT NULL) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0 AND u0 IS NOT NULL) FROM t")
      .run();

  plan = makePlan(true /*nullAware*/, "", "u0 IS NOT NULL");

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE u0 IS NOT NULL) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE u0 IS NOT NULL) FROM t")
      .run();

  // Empty build side.
  plan = makePlan(false /*nullAware*/, "", "u0 < 0");

  HashJoinBuilder(*pool_, duckDbQueryRunner_, executor_.get())
      .planNode(plan)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0 AND u0 < 0) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, executor_.get())
      .planNode(flipJoinSides(plan))
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0 AND u0 < 0) FROM t")
      .run();

  plan = makePlan(true /*nullAware*/, "", "u0 < 0");

  HashJoinBuilder(*pool_, duckDbQueryRunner_, executor_.get())
      .planNode(plan)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE u0 < 0) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, executor_.get())
      .planNode(flipJoinSides(plan))
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE u0 < 0) FROM t")
      .run();

  // Build side with all rows having null join keys.
  plan = makePlan(false /*nullAware*/, "", "u0 IS NULL");

  HashJoinBuilder(*pool_, duckDbQueryRunner_, executor_.get())
      .planNode(plan)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0 AND u0 IS NULL) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, executor_.get())
      .planNode(flipJoinSides(plan))
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0 AND u0 IS NULL) FROM t")
      .run();

  plan = makePlan(true /*nullAware*/, "", "u0 IS NULL");

  HashJoinBuilder(*pool_, duckDbQueryRunner_, executor_.get())
      .planNode(plan)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE u0 IS NULL) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, executor_.get())
      .planNode(flipJoinSides(plan))
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE u0 IS NULL) FROM t")
      .run();
}

TEST_F(HashJoinTest, semiProjectWithFilter) {
  auto probeVectors = makeBatches(3, [&](auto /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeNullableFlatVector<int32_t>({1, 2, 3, std::nullopt, 5}),
            makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
        });
  });

  auto buildVectors = makeBatches(3, [&](auto /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeNullableFlatVector<int32_t>({1, 2, 3, std::nullopt}),
            makeFlatVector<int64_t>({11, 22, 33, 44}),
        });
  });

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto makePlan = [&](bool nullAware, const std::string& filter) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    return PlanBuilder(planNodeIdGenerator)
        .values(probeVectors)
        .hashJoin(
            {"t0"},
            {"u0"},
            PlanBuilder(planNodeIdGenerator).values(buildVectors).planNode(),
            filter,
            {"t0", "t1", "match"},
            core::JoinType::kLeftSemiProject,
            nullAware)
        .planNode();
  };

  std::vector<std::string> filters = {
      "t1 <> u1",
      "t1 < u1",
      "t1 > u1",
      "t1 is not null AND u1 is not null",
      "t1 is null OR u1 is null",
  };
  for (const auto& filter : filters) {
    auto plan = makePlan(true /*nullAware*/, filter);

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(plan)
        .referenceQuery(fmt::format(
            "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE {}) FROM t", filter))
        .injectSpill(false)
        .run();

    plan = makePlan(false /*nullAware*/, filter);

    // DuckDB Exists operator returns NULL when u0 or t0 is NULL. We exclude
    // these values.
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(plan)
        .referenceQuery(fmt::format(
            "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE (u0 is not null OR t0 is not null) AND u0 = t0 AND {}) FROM t",
            filter))
        .injectSpill(false)
        .run();
  }
}

TEST_F(HashJoinTest, nullAwareRightSemiProjectWithFilterNotAllowed) {
  auto probe = makeRowVector(ROW({"t0", "t1"}, {INTEGER(), BIGINT()}), 10);
  auto build = makeRowVector(ROW({"u0", "u1"}, {INTEGER(), BIGINT()}), 10);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  VELOX_ASSERT_THROW(
      PlanBuilder(planNodeIdGenerator)
          .values({probe})
          .hashJoin(
              {"t0"},
              {"u0"},
              PlanBuilder(planNodeIdGenerator).values({build}).planNode(),
              "t1 > u1",
              {"u0", "u1", "match"},
              core::JoinType::kRightSemiProject,
              true /* nullAware */),
      "Null-aware right semi project join doesn't support extra filter");
}

TEST_F(HashJoinTest, nullAwareMultiKeyNotAllowed) {
  auto probe = makeRowVector(
      ROW({"t0", "t1", "t2"}, {INTEGER(), BIGINT(), VARCHAR()}), 10);
  auto build = makeRowVector(
      ROW({"u0", "u1", "u2"}, {INTEGER(), BIGINT(), VARCHAR()}), 10);

  // Null-aware left semi project join.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  VELOX_ASSERT_THROW(
      PlanBuilder(planNodeIdGenerator)
          .values({probe})
          .hashJoin(
              {"t0", "t1"},
              {"u0", "u1"},
              PlanBuilder(planNodeIdGenerator).values({build}).planNode(),
              "",
              {"t0", "t1", "match"},
              core::JoinType::kLeftSemiProject,
              true /* nullAware */),
      "Null-aware joins allow only one join key");

  // Null-aware right semi project join.
  VELOX_ASSERT_THROW(
      PlanBuilder(planNodeIdGenerator)
          .values({probe})
          .hashJoin(
              {"t0", "t1"},
              {"u0", "u1"},
              PlanBuilder(planNodeIdGenerator).values({build}).planNode(),
              "",
              {"u0", "u1", "match"},
              core::JoinType::kRightSemiProject,
              true /* nullAware */),
      "Null-aware joins allow only one join key");

  // Null-aware anti join.
  VELOX_ASSERT_THROW(
      PlanBuilder(planNodeIdGenerator)
          .values({probe})
          .hashJoin(
              {"t0", "t1"},
              {"u0", "u1"},
              PlanBuilder(planNodeIdGenerator).values({build}).planNode(),
              "",
              {"t0", "t1"},
              core::JoinType::kAnti,
              true /* nullAware */),
      "Null-aware joins allow only one join key");
}

TEST_F(HashJoinTest, semiProjectOverLazyVectors) {
  auto probeVectors = makeBatches(1, [&](auto /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
            makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
        });
  });

  auto buildVectors = makeBatches(3, [&](auto /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeFlatVector<int32_t>(
                1'000, [](auto row) { return -100 + (row / 5); }),
            makeFlatVector<int64_t>(
                1'000, [](auto row) { return -1000 + (row / 5) * 10; }),
        });
  });

  std::shared_ptr<TempFilePath> probeFile = TempFilePath::create();
  writeToFile(probeFile->path, probeVectors);

  std::shared_ptr<TempFilePath> buildFile = TempFilePath::create();
  writeToFile(buildFile->path, buildVectors);

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  core::PlanNodeId probeScanId;
  core::PlanNodeId buildScanId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(asRowType(probeVectors[0]->type()))
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      {"t0"},
                      {"u0"},
                      PlanBuilder(planNodeIdGenerator)
                          .tableScan(asRowType(buildVectors[0]->type()))
                          .capturePlanNodeId(buildScanId)
                          .planNode(),
                      "",
                      {"t0", "t1", "match"},
                      core::JoinType::kLeftSemiProject)
                  .planNode();

  SplitInput splitInput = {
      {probeScanId, {exec::Split(makeHiveConnectorSplit(probeFile->path))}},
      {buildScanId, {exec::Split(makeHiveConnectorSplit(buildFile->path))}},
  };

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .inputSplits(splitInput)
      .checkSpillStats(false)
      .referenceQuery("SELECT t0, t1, t0 IN (SELECT u0 FROM u) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .inputSplits(splitInput)
      .checkSpillStats(false)
      .referenceQuery("SELECT t0, t1, t0 IN (SELECT u0 FROM u) FROM t")
      .run();

  // With extra filter.
  planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  plan = PlanBuilder(planNodeIdGenerator)
             .tableScan(asRowType(probeVectors[0]->type()))
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"t0"},
                 {"u0"},
                 PlanBuilder(planNodeIdGenerator)
                     .tableScan(asRowType(buildVectors[0]->type()))
                     .capturePlanNodeId(buildScanId)
                     .planNode(),
                 "(t1 + u1) % 3 = 0",
                 {"t0", "t1", "match"},
                 core::JoinType::kLeftSemiProject)
             .planNode();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .inputSplits(splitInput)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE (t1 + u1) % 3 = 0) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .inputSplits(splitInput)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE (t1 + u1) % 3 = 0) FROM t")
      .run();
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    HashJoinTest,
    MultiThreadedHashJoinTest,
    testing::ValuesIn(MultiThreadedHashJoinTest::getTestParams()));

// TODO: try to parallelize the following test cases if possible.
TEST_F(HashJoinTest, memory) {
  // Measures memory allocation in a 1:n hash join followed by
  // projection and aggregation. We expect vectors to be mostly
  // reused, except for t_k0 + 1, which is a dictionary after the
  // join.
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(10, [&](int32_t /*unused*/) {
        return std::dynamic_pointer_cast<RowVector>(
            BatchMaker::createBatch(probeType_, 1000, *pool_));
      });

  // auto buildType = makeRowType(keyTypes, "u_");
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(10, [&](int32_t /*unused*/) {
        return std::dynamic_pointer_cast<RowVector>(
            BatchMaker::createBatch(buildType_, 1000, *pool_));
      });

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  CursorParameters params;
  params.planNode = PlanBuilder(planNodeIdGenerator)
                        .values(probeVectors, true)
                        .hashJoin(
                            {"t_k1"},
                            {"u_k1"},
                            PlanBuilder(planNodeIdGenerator)
                                .values(buildVectors, true)
                                .planNode(),
                            "",
                            concat(probeType_->names(), buildType_->names()))
                        .project({"t_k1 % 1000 AS k1", "u_k1 % 1000 AS k2"})
                        .singleAggregation({}, {"sum(k1)", "sum(k2)"})
                        .planNode();
  params.queryCtx = std::make_shared<core::QueryCtx>(driverExecutor_.get());
  auto [taskCursor, rows] = readCursor(params, [](Task*) {});
  EXPECT_GT(3'500, params.queryCtx->pool()->stats().numAllocs);
  EXPECT_GT(18'000'000, params.queryCtx->pool()->stats().cumulativeBytes);
}

TEST_F(HashJoinTest, lazyVectors) {
  // a dataset of multiple row groups with multiple columns. We create
  // different dictionary wrappings for different columns and load the
  // rows in scope at different times.
  auto probeVectors = makeBatches(3, [&](int32_t /*unused*/) {
    return makeRowVector(
        {makeFlatVector<int32_t>(3'000, [](auto row) { return row; }),
         makeFlatVector<int64_t>(30'000, [](auto row) { return row % 23; }),
         makeFlatVector<int32_t>(30'000, [](auto row) { return row % 31; }),
         makeFlatVector<StringView>(30'000, [](auto row) {
           return StringView::makeInline(fmt::format("{}   string", row % 43));
         })});
  });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(4, [&](int32_t /*unused*/) {
        return makeRowVector(
            {makeFlatVector<int32_t>(1'000, [](auto row) { return row * 3; }),
             makeFlatVector<int64_t>(
                 10'000, [](auto row) { return row % 31; })});
      });

  std::vector<std::shared_ptr<TempFilePath>> tempFiles;

  for (const auto& probeVector : probeVectors) {
    tempFiles.push_back(TempFilePath::create());
    writeToFile(tempFiles.back()->path, probeVector);
  }
  createDuckDbTable("t", probeVectors);

  for (const auto& buildVector : buildVectors) {
    tempFiles.push_back(TempFilePath::create());
    writeToFile(tempFiles.back()->path, buildVector);
  }
  createDuckDbTable("u", buildVectors);

  auto makeInputSplits = [&](const core::PlanNodeId& probeScanId,
                             const core::PlanNodeId& buildScanId) {
    return [&] {
      std::vector<exec::Split> probeSplits;
      for (int i = 0; i < probeVectors.size(); ++i) {
        probeSplits.push_back(
            exec::Split(makeHiveConnectorSplit(tempFiles[i]->path)));
      }
      std::vector<exec::Split> buildSplits;
      for (int i = 0; i < buildVectors.size(); ++i) {
        buildSplits.push_back(exec::Split(
            makeHiveConnectorSplit(tempFiles[probeSplits.size() + i]->path)));
      }
      SplitInput splits;
      splits.emplace(probeScanId, probeSplits);
      splits.emplace(buildScanId, buildSplits);
      return splits;
    };
  };

  {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId probeScanId;
    core::PlanNodeId buildScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(ROW({"c0", "c1"}, {INTEGER(), BIGINT()}))
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      {"c0"},
                      {"c0"},
                      PlanBuilder(planNodeIdGenerator)
                          .tableScan(ROW({"c0"}, {INTEGER()}))
                          .capturePlanNodeId(buildScanId)
                          .planNode(),
                      "",
                      {"c1"})
                  .project({"c1 + 1"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .makeInputSplits(makeInputSplits(probeScanId, buildScanId))
        .referenceQuery("SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0")
        .run();
  }

  {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId probeScanId;
    core::PlanNodeId buildScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      ROW({"c0", "c1", "c2", "c3"},
                          {INTEGER(), BIGINT(), INTEGER(), VARCHAR()}))
                  .capturePlanNodeId(probeScanId)
                  .filter("c2 < 29")
                  .hashJoin(
                      {"c0"},
                      {"bc0"},
                      PlanBuilder(planNodeIdGenerator)
                          .tableScan(ROW({"c0", "c1"}, {INTEGER(), BIGINT()}))
                          .capturePlanNodeId(buildScanId)
                          .project({"c0 as bc0", "c1 as bc1"})
                          .planNode(),
                      "(c1 + bc1) % 33 < 27",
                      {"c1", "bc1", "c3"})
                  .project({"c1 + 1", "bc1", "length(c3)"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .makeInputSplits(makeInputSplits(probeScanId, buildScanId))
        .referenceQuery(
            "SELECT t.c1 + 1, U.c1, length(t.c3) FROM t, u WHERE t.c0 = u.c0 and t.c2 < 29 and (t.c1 + u.c1) % 33 < 27")
        .run();
  }
}

TEST_F(HashJoinTest, dynamicFilters) {
  const int32_t numSplits = 10;
  const int32_t numRowsProbe = 333;
  const int32_t numRowsBuild = 100;

  std::vector<RowVectorPtr> probeVectors;
  probeVectors.reserve(numSplits);

  std::vector<std::shared_ptr<TempFilePath>> tempFiles;
  for (int32_t i = 0; i < numSplits; ++i) {
    auto rowVector = makeRowVector({
        makeFlatVector<int32_t>(
            numRowsProbe, [&](auto row) { return row - i * 10; }),
        makeFlatVector<int64_t>(numRowsProbe, [](auto row) { return row; }),
    });
    probeVectors.push_back(rowVector);
    tempFiles.push_back(TempFilePath::create());
    writeToFile(tempFiles.back()->path, rowVector);
  }
  auto makeInputSplits = [&](const core::PlanNodeId& nodeId) {
    return [&] {
      std::vector<exec::Split> probeSplits;
      for (auto& file : tempFiles) {
        probeSplits.push_back(exec::Split(makeHiveConnectorSplit(file->path)));
      }
      SplitInput splits;
      splits.emplace(nodeId, probeSplits);
      return splits;
    };
  };

  // 100 key values in [35, 233] range.
  std::vector<RowVectorPtr> buildVectors;
  for (int i = 0; i < 5; ++i) {
    buildVectors.push_back(makeRowVector({
        makeFlatVector<int32_t>(
            numRowsBuild / 5,
            [i](auto row) { return 35 + 2 * (row + i * numRowsBuild / 5); }),
        makeFlatVector<int64_t>(numRowsBuild / 5, [](auto row) { return row; }),
    }));
  }
  std::vector<RowVectorPtr> keyOnlyBuildVectors;
  for (int i = 0; i < 5; ++i) {
    keyOnlyBuildVectors.push_back(
        makeRowVector({makeFlatVector<int32_t>(numRowsBuild / 5, [i](auto row) {
          return 35 + 2 * (row + i * numRowsBuild / 5);
        })}));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto probeType = ROW({"c0", "c1"}, {INTEGER(), BIGINT()});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .values(buildVectors)
                       .project({"c0 AS u_c0", "c1 AS u_c1"})
                       .planNode();
  auto keyOnlyBuildSide = PlanBuilder(planNodeIdGenerator, pool_.get())
                              .values(keyOnlyBuildVectors)
                              .project({"c0 AS u_c0"})
                              .planNode();

  // Basic push-down.
  {
    // Inner join.
    core::PlanNodeId probeScanId;
    auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(probeType)
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      {"c0"},
                      {"u_c0"},
                      buildSide,
                      "",
                      {"c0", "c1", "u_c1"},
                      core::JoinType::kInner)
                  .project({"c0", "c1 + 1", "c1 + u_c1"})
                  .planNode();
    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT t.c0, t.c1 + 1, t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getInputPositions(task, 1), numRowsProbe * numSplits);
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
            }
          })
          .run();
    }

    // Left semi join.
    op = PlanBuilder(planNodeIdGenerator, pool_.get())
             .tableScan(probeType)
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"c0", "c1"},
                 core::JoinType::kLeftSemiFilter)
             .project({"c0", "c1 + 1"})
             .planNode();

    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT t.c0, t.c1 + 1 FROM t WHERE t.c0 IN (SELECT c0 FROM u)")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
              ASSERT_EQ(getInputPositions(task, 1), numRowsProbe * numSplits);
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
            }
          })
          .run();
    }

    // Right semi join.
    op = PlanBuilder(planNodeIdGenerator, pool_.get())
             .tableScan(probeType)
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"u_c0", "u_c1"},
                 core::JoinType::kRightSemiFilter)
             .project({"u_c0", "u_c1 + 1"})
             .planNode();

    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT u.c0, u.c1 + 1 FROM u WHERE u.c0 IN (SELECT c0 FROM t)")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_EQ(getInputPositions(task, 1), numRowsProbe * numSplits);
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
            }
          })
          .run();
    }
  }

  // Basic push-down with column names projected out of the table scan
  // having different names than column names in the files.
  {
    auto scanOutputType = ROW({"a", "b"}, {INTEGER(), BIGINT()});
    ColumnHandleMap assignments;
    assignments["a"] = regularColumn("c0", INTEGER());
    assignments["b"] = regularColumn("c1", BIGINT());

    core::PlanNodeId probeScanId;
    auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .startTableScan()
                  .outputType(scanOutputType)
                  .assignments(assignments)
                  .endTableScan()
                  .capturePlanNodeId(probeScanId)
                  .hashJoin({"a"}, {"u_c0"}, buildSide, "", {"a", "b", "u_c1"})
                  .project({"a", "b + 1", "b + u_c1"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .makeInputSplits(makeInputSplits(probeScanId))
        .referenceQuery(
            "SELECT t.c0, t.c1 + 1, t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
          if (hasSpill) {
            // Dynamic filtering should be disabled with spilling triggered.
            ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
            ASSERT_EQ(getInputPositions(task, 1), numRowsProbe * numSplits);
          } else {
            ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
            ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
          }
        })
        .run();
  }

  // Push-down that requires merging filters.
  {
    core::PlanNodeId probeScanId;
    auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(probeType, {"c0 < 500::INTEGER"})
                  .capturePlanNodeId(probeScanId)
                  .hashJoin({"c0"}, {"u_c0"}, buildSide, "", {"c1", "u_c1"})
                  .project({"c1 + u_c1"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .makeInputSplits(makeInputSplits(probeScanId))
        .referenceQuery(
            "SELECT t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0 AND t.c0 < 500")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
          if (hasSpill) {
            // Dynamic filtering should be disabled with spilling triggered.
            ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
            ASSERT_EQ(getInputPositions(task, 1), numRowsProbe * numSplits);
          } else {
            ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
            ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
          }
        })
        .run();
  }

  // Push-down that turns join into a no-op.
  {
    core::PlanNodeId probeScanId;
    auto op =
        PlanBuilder(planNodeIdGenerator, pool_.get())
            .tableScan(probeType)
            .capturePlanNodeId(probeScanId)
            .hashJoin({"c0"}, {"u_c0"}, keyOnlyBuildSide, "", {"c0", "c1"})
            .project({"c0", "c1 + 1"})
            .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .makeInputSplits(makeInputSplits(probeScanId))
        .referenceQuery("SELECT t.c0, t.c1 + 1 FROM t, u WHERE t.c0 = u.c0")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
          if (hasSpill) {
            // Dynamic filtering should be disabled with spilling triggered.
            ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
            ASSERT_EQ(getInputPositions(task, 1), numRowsProbe * numSplits);
          } else {
            ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(
                getReplacedWithFilterRows(task, 1).sum,
                numRowsBuild * numSplits);
            ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
          }
        })
        .run();
  }

  // Push-down that turns join into a no-op with output having a different
  // number of columns than the input.
  {
    core::PlanNodeId probeScanId;
    auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(probeType)
                  .capturePlanNodeId(probeScanId)
                  .hashJoin({"c0"}, {"u_c0"}, keyOnlyBuildSide, "", {"c0"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .makeInputSplits(makeInputSplits(probeScanId))
        .referenceQuery("SELECT t.c0 FROM t JOIN u ON (t.c0 = u.c0)")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
          if (hasSpill) {
            // Dynamic filtering should be disabled with spilling triggered.
            ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
            ASSERT_EQ(getInputPositions(task, 1), numRowsProbe * numSplits);
          } else {
            ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(
                getReplacedWithFilterRows(task, 1).sum,
                numRowsBuild * numSplits);
            ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
          }
        })
        .run();
  }

  // Push-down that requires merging filters and turns join into a no-op.
  {
    core::PlanNodeId probeScanId;
    auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(probeType, {"c0 < 500::INTEGER"})
                  .capturePlanNodeId(probeScanId)
                  .hashJoin({"c0"}, {"u_c0"}, keyOnlyBuildSide, "", {"c1"})
                  .project({"c1 + 1"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .makeInputSplits(makeInputSplits(probeScanId))
        .referenceQuery(
            "SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0 AND t.c0 < 500")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
          if (hasSpill) {
            // Dynamic filtering should be disabled with spilling triggered.
            ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
            ASSERT_EQ(getInputPositions(task, 1), numRowsProbe * numSplits);
          } else {
            ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
            ASSERT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
            ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
          }
        })
        .run();
  }

  // Push-down with highly selective filter in the scan.
  {
    // Inner join.
    core::PlanNodeId probeScanId;
    auto op =
        PlanBuilder(planNodeIdGenerator, pool_.get())
            .tableScan(probeType, {"c0 < 200::INTEGER"})
            .capturePlanNodeId(probeScanId)
            .hashJoin(
                {"c0"}, {"u_c0"}, buildSide, "", {"c1"}, core::JoinType::kInner)
            .project({"c1 + 1"})
            .planNode();

    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0 AND t.c0 < 200")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
            }
          })
          .run();
    }

    // Left semi join.
    op = PlanBuilder(planNodeIdGenerator, pool_.get())
             .tableScan(probeType, {"c0 < 200::INTEGER"})
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"c1"},
                 core::JoinType::kLeftSemiFilter)
             .project({"c1 + 1"})
             .planNode();

    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT t.c1 + 1 FROM t WHERE t.c0 IN (SELECT c0 FROM u) AND t.c0 < 200")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
            }
          })
          .run();
    }

    // Right semi join.
    op = PlanBuilder(planNodeIdGenerator, pool_.get())
             .tableScan(probeType, {"c0 < 200::INTEGER"})
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"u_c1"},
                 core::JoinType::kRightSemiFilter)
             .project({"u_c1 + 1"})
             .planNode();

    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT u.c1 + 1 FROM u WHERE u.c0 IN (SELECT c0 FROM t) AND u.c0 < 200")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
            }
          })
          .run();
    }
  }

  // Disable filter push-down by using values in place of scan.
  {
    auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .values(probeVectors)
                  .hashJoin({"c0"}, {"u_c0"}, buildSide, "", {"c1"})
                  .project({"c1 + 1"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .referenceQuery("SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
          ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
          ASSERT_EQ(numRowsProbe * numSplits, getInputPositions(task, 1));
        })
        .run();
  }

  // Disable filter push-down by using an expression as the join key on the
  // probe side.
  {
    core::PlanNodeId probeScanId;
    auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(probeType)
                  .capturePlanNodeId(probeScanId)
                  .project({"cast(c0 + 1 as integer) AS t_key", "c1"})
                  .hashJoin({"t_key"}, {"u_c0"}, buildSide, "", {"c1"})
                  .project({"c1 + 1"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .makeInputSplits(makeInputSplits(probeScanId))
        .referenceQuery("SELECT t.c1 + 1 FROM t, u WHERE (t.c0 + 1) = u.c0")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
          ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
          ASSERT_EQ(numRowsProbe * numSplits, getInputPositions(task, 1));
        })
        .run();
  }
}

TEST_F(HashJoinTest, dynamicFiltersWithSkippedSplits) {
  const int32_t numSplits = 20;
  const int32_t numNonSkippedSplits = 10;
  const int32_t numRowsProbe = 333;
  const int32_t numRowsBuild = 100;

  std::vector<RowVectorPtr> probeVectors;
  probeVectors.reserve(numSplits);

  std::vector<std::shared_ptr<TempFilePath>> tempFiles;
  // Each split has a column containing
  // the split number. This is used to filter out whole splits based
  // on metadata. We test how using metadata for dropping splits
  // interactts with dynamic filters. In specific, if the first split
  // is discarded based on metadata, the dynamic filters must not be
  // lost even if there is no actual reader for the split.
  for (int32_t i = 0; i < numSplits; ++i) {
    auto rowVector = makeRowVector({
        makeFlatVector<int32_t>(
            numRowsProbe, [&](auto row) { return row - i * 10; }),
        makeFlatVector<int64_t>(numRowsProbe, [](auto row) { return row; }),
        makeFlatVector<int64_t>(
            numRowsProbe, [&](auto /*row*/) { return i % 2 == 0 ? 0 : i; }),
    });
    probeVectors.push_back(rowVector);
    tempFiles.push_back(TempFilePath::create());
    writeToFile(tempFiles.back()->path, rowVector);
  }

  auto makeInputSplits = [&](const core::PlanNodeId& nodeId) {
    return [&] {
      std::vector<exec::Split> probeSplits;
      for (auto& file : tempFiles) {
        probeSplits.push_back(exec::Split(makeHiveConnectorSplit(file->path)));
      }
      // We add splits that have no rows.
      auto makeEmpty = [&]() {
        return exec::Split(HiveConnectorSplitBuilder(tempFiles.back()->path)
                               .start(10000000)
                               .length(1)
                               .build());
      };
      std::vector<exec::Split> emptyFront = {makeEmpty(), makeEmpty()};
      std::vector<exec::Split> emptyMiddle = {makeEmpty(), makeEmpty()};
      probeSplits.insert(
          probeSplits.begin(), emptyFront.begin(), emptyFront.end());
      probeSplits.insert(
          probeSplits.begin() + 13, emptyMiddle.begin(), emptyMiddle.end());
      SplitInput splits;
      splits.emplace(nodeId, probeSplits);
      return splits;
    };
  };

  // 100 key values in [35, 233] range.
  std::vector<RowVectorPtr> buildVectors;
  for (int i = 0; i < 5; ++i) {
    buildVectors.push_back(makeRowVector({
        makeFlatVector<int32_t>(
            numRowsBuild / 5,
            [i](auto row) { return 35 + 2 * (row + i * numRowsBuild / 5); }),
        makeFlatVector<int64_t>(numRowsBuild / 5, [](auto row) { return row; }),
    }));
  }
  std::vector<RowVectorPtr> keyOnlyBuildVectors;
  for (int i = 0; i < 5; ++i) {
    keyOnlyBuildVectors.push_back(
        makeRowVector({makeFlatVector<int32_t>(numRowsBuild / 5, [i](auto row) {
          return 35 + 2 * (row + i * numRowsBuild / 5);
        })}));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto probeType = ROW({"c0", "c1", "c2"}, {INTEGER(), BIGINT(), BIGINT()});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .values(buildVectors)
                       .project({"c0 AS u_c0", "c1 AS u_c1"})
                       .planNode();
  auto keyOnlyBuildSide = PlanBuilder(planNodeIdGenerator, pool_.get())
                              .values(keyOnlyBuildVectors)
                              .project({"c0 AS u_c0"})
                              .planNode();

  // Basic push-down.
  {
    // Inner join.
    core::PlanNodeId probeScanId;
    auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(probeType, {"c2 > 0"})
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      {"c0"},
                      {"u_c0"},
                      buildSide,
                      "",
                      {"c0", "c1", "u_c1"},
                      core::JoinType::kInner)
                  .project({"c0", "c1 + 1", "c1 + u_c1"})
                  .planNode();
    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .numDrivers(1)
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT t.c0, t.c1 + 1, t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0 AND t.c2 > 0")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(
                  getInputPositions(task, 1),
                  numRowsProbe * numNonSkippedSplits);
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
              ASSERT_LT(
                  getInputPositions(task, 1),
                  numRowsProbe * numNonSkippedSplits);
            }
          })
          .run();
    }

    // Left semi join.
    op = PlanBuilder(planNodeIdGenerator, pool_.get())
             .tableScan(probeType, {"c2 > 0"})
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"c0", "c1"},
                 core::JoinType::kLeftSemiFilter)
             .project({"c0", "c1 + 1"})
             .planNode();

    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .numDrivers(1)
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT t.c0, t.c1 + 1 FROM t WHERE t.c0 IN (SELECT c0 FROM u) AND t.c2 > 0")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
              ASSERT_EQ(
                  getInputPositions(task, 1),
                  numRowsProbe * numNonSkippedSplits);
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(
                  getInputPositions(task, 1),
                  numRowsProbe * numNonSkippedSplits);
            }
          })
          .run();
    }

    // Right semi join.
    op = PlanBuilder(planNodeIdGenerator, pool_.get())
             .tableScan(probeType, {"c2 > 0"})
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"u_c0", "u_c1"},
                 core::JoinType::kRightSemiFilter)
             .project({"u_c0", "u_c1 + 1"})
             .planNode();

    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .numDrivers(1)
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT u.c0, u.c1 + 1 FROM u WHERE u.c0 IN (SELECT c0 FROM t WHERE t.c2 > 0)")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_EQ(
                  getInputPositions(task, 1),
                  numRowsProbe * numNonSkippedSplits);
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(
                  getInputPositions(task, 1),
                  numRowsProbe * numNonSkippedSplits);
            }
          })
          .run();
    }
  }
}

TEST_F(HashJoinTest, dynamicFiltersAppliedToPreloadedSplits) {
  vector_size_t size = 1000;
  const int32_t numSplits = 5;

  std::vector<RowVectorPtr> probeVectors;
  probeVectors.reserve(numSplits);

  // Prepare probe side table.
  std::vector<std::shared_ptr<TempFilePath>> tempFiles;
  std::vector<exec::Split> probeSplits;
  for (int32_t i = 0; i < numSplits; ++i) {
    auto rowVector = makeRowVector(
        {"p0", "p1"},
        {
            makeFlatVector<int64_t>(
                size, [&](auto row) { return (row + 1) * (i + 1); }),
            makeFlatVector<int64_t>(size, [&](auto /*row*/) { return i; }),
        });
    probeVectors.push_back(rowVector);
    tempFiles.push_back(TempFilePath::create());
    writeToFile(tempFiles.back()->path, rowVector);
    auto split = HiveConnectorSplitBuilder(tempFiles.back()->path)
                     .partitionKey("p1", std::to_string(i))
                     .build();
    probeSplits.push_back(exec::Split(split));
  }

  auto outputType = ROW({"p0", "p1"}, {BIGINT(), BIGINT()});
  ColumnHandleMap assignments = {
      {"p0", regularColumn("p0", BIGINT())},
      {"p1", partitionKey("p1", BIGINT())}};
  createDuckDbTable("p", probeVectors);

  // Prepare build side table.
  std::vector<RowVectorPtr> buildVectors{
      makeRowVector({"b0"}, {makeFlatVector<int64_t>({0, numSplits})})};
  createDuckDbTable("b", buildVectors);

  // Executing the join with p1=b0, we expect a dynamic filter for p1 to prune
  // the entire file/split. There are total of five splits, and all except the
  // first one are expected to be pruned. The result 'preloadedSplits' > 1
  // confirms the successful push of dynamic filters to the preloading data
  // source.
  core::PlanNodeId probeScanId;
  core::PlanNodeId joinNodeId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op =
      PlanBuilder(planNodeIdGenerator)
          .startTableScan()
          .outputType(outputType)
          .assignments(assignments)
          .endTableScan()
          .capturePlanNodeId(probeScanId)
          .hashJoin(
              {"p1"},
              {"b0"},
              PlanBuilder(planNodeIdGenerator).values(buildVectors).planNode(),
              "",
              {"p0"},
              core::JoinType::kInner)
          .capturePlanNodeId(joinNodeId)
          .project({"p0"})
          .planNode();
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(std::move(op))
      .config(core::QueryConfig::kMaxSplitPreloadPerDriver, "3")
      .injectSpill(false)
      .inputSplits({{probeScanId, probeSplits}})
      .referenceQuery("select p.p0 from p, b where b.b0 = p.p1")
      .checkSpillStats(false)
      .verifier([&](const std::shared_ptr<Task>& task, bool /*hasSpill*/) {
        auto planStats = toPlanStats(task->taskStats());
        auto getStatSum = [&](const core::PlanNodeId& id,
                              const std::string& name) {
          return planStats.at(id).customStats.at(name).sum;
        };
        ASSERT_EQ(1, getStatSum(joinNodeId, "dynamicFiltersProduced"));
        ASSERT_EQ(1, getStatSum(probeScanId, "dynamicFiltersAccepted"));
        ASSERT_EQ(4, getStatSum(probeScanId, "skippedSplits"));
        ASSERT_LT(1, getStatSum(probeScanId, "preloadedSplits"));
      })
      .run();
}

// Verify the size of the join output vectors when projecting build-side
// variable-width column.
TEST_F(HashJoinTest, memoryUsage) {
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(10, [&](int32_t /*unused*/) {
        return makeRowVector(
            {makeFlatVector<int32_t>(1'000, [](auto row) { return row % 5; })});
      });
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector(
            {"u_c0", "u_c1"},
            {makeFlatVector<int32_t>({0, 1, 2}),
             makeFlatVector<std::string>({
                 std::string(40, 'a'),
                 std::string(50, 'b'),
                 std::string(30, 'c'),
             })});
      });
  core::PlanNodeId joinNodeId;

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors)
                  .hashJoin(
                      {"c0"},
                      {"u_c0"},
                      PlanBuilder(planNodeIdGenerator)
                          .values({buildVectors})
                          .planNode(),
                      "",
                      {"c0", "u_c1"})
                  .capturePlanNodeId(joinNodeId)
                  .singleAggregation({}, {"count(1)"})
                  .planNode();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(std::move(plan))
      .referenceQuery("SELECT 30000")
      .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
        if (hasSpill) {
          return;
        }
        auto planStats = toPlanStats(task->taskStats());
        auto outputBytes = planStats.at(joinNodeId).outputBytes;
        ASSERT_LT(outputBytes, ((40 + 50 + 30) / 3 + 8) * 1000 * 10 * 5);
        // Verify number of memory allocations. Should not be too high if
        // hash join is able to re-use output vectors that contain
        // build-side data.
        ASSERT_GT(40, task->pool()->stats().numAllocs);
      })
      .run();
}

/// Test an edge case in producing small output batches where the logic to
/// calculate the set of probe-side rows to load lazy vectors for was
/// triggering a crash.
TEST_F(HashJoinTest, smallOutputBatchSize) {
  // Setup probe data with 50 non-null matching keys followed by 50 null
  // keys: 1, 2, 1, 2,...null, null.
  auto probeVectors = makeRowVector({
      makeFlatVector<int32_t>(
          100,
          [](auto row) { return 1 + row % 2; },
          [](auto row) { return row > 50; }),
      makeFlatVector<int32_t>(100, [](auto row) { return row * 10; }),
  });

  // Setup build side to match non-null probe side keys.
  auto buildVectors = makeRowVector(
      {"u_c0", "u_c1"},
      {
          makeFlatVector<int32_t>({1, 2}),
          makeFlatVector<int32_t>({100, 200}),
      });

  createDuckDbTable("t", {probeVectors});
  createDuckDbTable("u", {buildVectors});

  // Plan hash inner join with a filter.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values({probeVectors})
                  .hashJoin(
                      {"c0"},
                      {"u_c0"},
                      PlanBuilder(planNodeIdGenerator)
                          .values({buildVectors})
                          .planNode(),
                      "c1 < u_c1",
                      {"c0", "u_c1"})
                  .planNode();

  // Use small output batch size to trigger logic for calculating set of
  // probe-side rows to load lazy vectors for.
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(std::move(plan))
      .config(core::QueryConfig::kPreferredOutputBatchRows, std::to_string(10))
      .referenceQuery("SELECT c0, u_c1 FROM t, u WHERE c0 = u_c0 AND c1 < u_c1")
      .injectSpill(false)
      .run();
}

TEST_F(HashJoinTest, spillFileSize) {
  const std::vector<uint64_t> maxSpillFileSizes({0, 1, 1'000'000'000});
  for (const auto spillFileSize : maxSpillFileSizes) {
    SCOPED_TRACE(fmt::format("spillFileSize: {}", spillFileSize));
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .keyTypes({BIGINT()})
        .probeVectors(100, 3)
        .buildVectors(100, 3)
        .referenceQuery(
            "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t.t_k0 = u.u_k0")
        .config(core::QueryConfig::kSpillStartPartitionBit, "48")
        .config(core::QueryConfig::kJoinSpillPartitionBits, "3")
        .config(
            core::QueryConfig::kMaxSpillFileSize, std::to_string(spillFileSize))
        .checkSpillStats(false)
        .maxSpillLevel(0)
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          if (!hasSpill) {
            return;
          }
          const auto statsPair = taskSpilledStats(*task);
          const int32_t numPartitions = statsPair.first.spilledPartitions;
          ASSERT_EQ(statsPair.second.spilledPartitions, numPartitions);
          const auto fileSizes = numTaskSpillFiles(*task);
          if (spillFileSize != 1) {
            ASSERT_EQ(fileSizes.first, numPartitions);
          } else {
            ASSERT_GT(fileSizes.first, numPartitions);
          }
          verifyTaskSpilledRuntimeStats(*task, true);
        })
        .run();
  }
}

// The test is to verify if the hash build reservation has been released on
// task error.
DEBUG_ONLY_TEST_F(HashJoinTest, buildReservationReleaseCheck) {
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(1, [&](int32_t /*unused*/) {
        return std::dynamic_pointer_cast<RowVector>(
            BatchMaker::createBatch(probeType_, 1000, *pool_));
      });
  std::vector<RowVectorPtr> buildVectors = makeBatches(10, [&](int32_t index) {
    return std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(buildType_, 5000 * (1 + index), *pool_));
  });

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  CursorParameters params;
  params.planNode = PlanBuilder(planNodeIdGenerator)
                        .values(probeVectors, true)
                        .hashJoin(
                            {"t_k1"},
                            {"u_k1"},
                            PlanBuilder(planNodeIdGenerator)
                                .values(buildVectors, true)
                                .planNode(),
                            "",
                            concat(probeType_->names(), buildType_->names()))
                        .planNode();
  params.queryCtx = std::make_shared<core::QueryCtx>(driverExecutor_.get());
  // NOTE: the spilling setup is to trigger memory reservation code path which
  // only gets executed when spilling is enabled. We don't care about if
  // spilling is really triggered in test or not.
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  params.spillDirectory = spillDirectory->path;
  params.queryCtx->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kSpillEnabled, "true"},
       {core::QueryConfig::kMaxSpillLevel, "0"},
       {core::QueryConfig::kJoinSpillEnabled, "true"}});
  params.maxDrivers = 1;

  auto cursor = TaskCursor::create(params);
  auto* task = cursor->task().get();

  // Set up a testvalue to trigger task abort when hash build tries to reserve
  // memory.
  SCOPED_TESTVALUE_SET(
      "facebook::velox::common::memory::MemoryPoolImpl::maybeReserve",
      std::function<void(memory::MemoryPool*)>(
          [&](memory::MemoryPool* /*unused*/) { task->requestAbort(); }));
  auto runTask = [&]() {
    while (cursor->moveNext()) {
    }
  };
  VELOX_ASSERT_THROW(runTask(), "");
  ASSERT_TRUE(waitForTaskAborted(task, 5'000'000));
}

TEST_F(HashJoinTest, dynamicFilterOnPartitionKey) {
  vector_size_t size = 10;
  auto filePaths = makeFilePaths(1);
  auto rowVector = makeRowVector(
      {makeFlatVector<int64_t>(size, [&](auto row) { return row; })});
  createDuckDbTable("u", {rowVector});
  writeToFile(filePaths[0]->path, rowVector);
  std::vector<RowVectorPtr> buildVectors{
      makeRowVector({"c0"}, {makeFlatVector<int64_t>({0, 1, 2})})};
  createDuckDbTable("t", buildVectors);
  auto split =
      facebook::velox::exec::test::HiveConnectorSplitBuilder(filePaths[0]->path)
          .partitionKey("k", "0")
          .build();
  auto outputType = ROW({"n1_0", "n1_1"}, {BIGINT(), BIGINT()});
  ColumnHandleMap assignments = {
      {"n1_0", regularColumn("c0", BIGINT())},
      {"n1_1", partitionKey("k", BIGINT())}};

  core::PlanNodeId probeScanId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op =
      PlanBuilder(planNodeIdGenerator)
          .startTableScan()
          .outputType(outputType)
          .assignments(assignments)
          .endTableScan()
          .capturePlanNodeId(probeScanId)
          .hashJoin(
              {"n1_1"},
              {"c0"},
              PlanBuilder(planNodeIdGenerator).values(buildVectors).planNode(),
              "",
              {"c0"},
              core::JoinType::kInner)
          .project({"c0"})
          .planNode();
  SplitInput splits = {{probeScanId, {exec::Split(split)}}};

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(std::move(op))
      .inputSplits(splits)
      .referenceQuery("select t.c0 from t, u where t.c0 = 0")
      .checkSpillStats(false)
      .run();
}

DEBUG_ONLY_TEST_F(HashJoinTest, reclaimDuringInputProcessing) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBuildVectors = 10;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }
  const int32_t numProbeVectors = 5;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  struct {
    // 0: trigger reclaim with some input processed.
    // 1: trigger reclaim after all the inputs processed.
    int triggerCondition;
    bool spillEnabled;
    bool expectedReclaimable;

    std::string debugString() const {
      return fmt::format(
          "triggerCondition {}, spillEnabled {}, expectedReclaimable {}",
          triggerCondition,
          spillEnabled,
          expectedReclaimable);
    }
  } testSettings[] = {
      {0, true, true}, {0, true, true}, {0, false, false}, {0, false, false}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto tempDirectory = exec::test::TempDirectoryPath::create();
    auto queryPool = memory::memoryManager()->addRootPool(
        "", kMaxBytes, memory::MemoryReclaimer::create());

    core::PlanNodeId probeScanId;
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors, false)
                    .hashJoin(
                        {"t_k1"},
                        {"u_k1"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors, false)
                            .planNode(),
                        "",
                        concat(probeType_->names(), buildType_->names()))
                    .planNode();

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    std::atomic<int> numInputs{0};
    Operator* op;
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "HashBuild") {
            //ASSERT_FALSE(testOp->canReclaim());
            return;
          }
          op = testOp;
          ++numInputs;
          if (testData.triggerCondition == 0) {
            if (numInputs != 2) {
              return;
            }
          }
          if (testData.triggerCondition == 1) {
            if (numInputs != numBuildVectors) {
              return;
            }
          }
          ASSERT_EQ(op->canReclaim(), testData.expectedReclaimable);
          uint64_t reclaimableBytes{0};
          const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
          ASSERT_EQ(reclaimable, testData.expectedReclaimable);
          if (testData.expectedReclaimable) {
            ASSERT_GT(reclaimableBytes, 0);
          } else {
            ASSERT_EQ(reclaimableBytes, 0);
          }
          testWait.notify();
          driverWait.wait(driverWaitKey);
        })));

    std::thread taskThread([&]() {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .numDrivers(numDrivers_)
          .planNode(plan)
          .queryPool(std::move(queryPool))
          .injectSpill(false)
          .spillDirectory(testData.spillEnabled ? tempDirectory->path : "")
          .referenceQuery(
              "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
          .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
            const auto statsPair = taskSpilledStats(*task);
            if (testData.expectedReclaimable) {
              ASSERT_GT(statsPair.first.spilledBytes, 0);
              ASSERT_EQ(statsPair.first.spilledPartitions, 4);
              ASSERT_GT(statsPair.second.spilledBytes, 0);
              ASSERT_EQ(statsPair.second.spilledPartitions, 4);
              verifyTaskSpilledRuntimeStats(*task, true);
            } else {
              ASSERT_EQ(statsPair.first.spilledBytes, 0);
              ASSERT_EQ(statsPair.first.spilledPartitions, 0);
              ASSERT_EQ(statsPair.second.spilledBytes, 0);
              ASSERT_EQ(statsPair.second.spilledPartitions, 0);
              verifyTaskSpilledRuntimeStats(*task, false);
            }
          })
          .run();
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->testingOperatorCtx()->task();
    auto taskPauseWait = task->requestPause();
    driverWait.notify();
    taskPauseWait.wait();

    uint64_t reclaimableBytes{0};
    const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
    ASSERT_EQ(op->canReclaim(), testData.expectedReclaimable);
    ASSERT_EQ(reclaimable, testData.expectedReclaimable);
    if (testData.expectedReclaimable) {
      ASSERT_GT(reclaimableBytes, 0);
    } else {
      ASSERT_EQ(reclaimableBytes, 0);
    }

    if (testData.expectedReclaimable) {
      reclaimAndRestoreCapacity(
          op,
          folly::Random::oneIn(2) ? 0 : folly::Random::rand32(),
          reclaimerStats_);
      ASSERT_GT(reclaimerStats_.reclaimExecTimeUs, 0);
      ASSERT_GT(reclaimerStats_.reclaimedBytes, 0);
      reclaimerStats_.reset();
      ASSERT_EQ(op->pool()->currentBytes(), 0);
    } else {
      VELOX_ASSERT_THROW(
          op->reclaim(
              folly::Random::oneIn(2) ? 0 : folly::Random::rand32(),
              reclaimerStats_),
          "");
    }

    Task::resume(task);
    task.reset();

    taskThread.join();
  }
  ASSERT_EQ(reclaimerStats_, memory::MemoryReclaimer::Stats{});
}

DEBUG_ONLY_TEST_F(HashJoinTest, reclaimDuringReserve) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  const int32_t numBuildVectors = 3;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    const size_t size = i == 0 ? 1 : 1'000;
    VectorFuzzer fuzzer({.vectorSize = size}, pool());
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }

  const int32_t numProbeVectors = 3;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    VectorFuzzer fuzzer({.vectorSize = 1'000}, pool());
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto tempDirectory = exec::test::TempDirectoryPath::create();
  auto queryPool = memory::memoryManager()->addRootPool(
      "", kMaxBytes, memory::MemoryReclaimer::create());

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors, false)
                  .hashJoin(
                      {"t_k1"},
                      {"u_k1"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors, false)
                          .planNode(),
                      "",
                      concat(probeType_->names(), buildType_->names()))
                  .planNode();

  folly::EventCount driverWait;
  std::atomic_bool driverWaitFlag{true};
  folly::EventCount testWait;
  std::atomic_bool testWaitFlag{true};

  Operator* op{nullptr};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::addInput",
      std::function<void(Operator*)>(([&](Operator* testOp) {
        if (testOp->operatorType() != "HashBuild") {
          //ASSERT_FALSE(testOp->canReclaim());
          return;
        }
        op = testOp;
      })));

  std::atomic<bool> injectOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::common::memory::MemoryPoolImpl::maybeReserve",
      std::function<void(memory::MemoryPoolImpl*)>(
          ([&](memory::MemoryPoolImpl* pool) {
            ASSERT_TRUE(op != nullptr);
            if (!isHashBuildMemoryPool(*pool)) {
              return;
            }
            ASSERT_TRUE(op->canReclaim());
            if (op->pool()->currentBytes() == 0) {
              // We skip trigger memory reclaim when the hash table is empty on
              // memory reservation.
              return;
            }
            if (!injectOnce.exchange(false)) {
              return;
            }
            uint64_t reclaimableBytes{0};
            const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
            ASSERT_TRUE(reclaimable);
            ASSERT_GT(reclaimableBytes, 0);
            auto* driver = op->testingOperatorCtx()->driver();
            SuspendedSection suspendedSection(driver);
            testWaitFlag = false;
            testWait.notifyAll();
            driverWait.await([&]() { return !driverWaitFlag.load(); });
          })));

  std::thread taskThread([&]() {
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .planNode(plan)
        .queryPool(std::move(queryPool))
        .injectSpill(false)
        .spillDirectory(tempDirectory->path)
        .referenceQuery(
            "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_GT(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 4);
          ASSERT_GT(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 4);
          verifyTaskSpilledRuntimeStats(*task, true);
        })
        .run();
  });

  testWait.await([&]() { return !testWaitFlag.load(); });
  ASSERT_TRUE(op != nullptr);
  auto task = op->testingOperatorCtx()->task();
  task->requestPause().wait();

  uint64_t reclaimableBytes{0};
  const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
  ASSERT_TRUE(op->canReclaim());
  ASSERT_TRUE(reclaimable);
  ASSERT_GT(reclaimableBytes, 0);

  reclaimAndRestoreCapacity(
      op,
      folly::Random::oneIn(2) ? 0 : folly::Random::rand32(),
      reclaimerStats_);
  ASSERT_GT(reclaimerStats_.reclaimedBytes, 0);
  ASSERT_GT(reclaimerStats_.reclaimExecTimeUs, 0);
  ASSERT_EQ(op->pool()->currentBytes(), 0);

  driverWaitFlag = false;
  driverWait.notifyAll();
  Task::resume(task);
  task.reset();

  taskThread.join();
}

DEBUG_ONLY_TEST_F(HashJoinTest, reclaimDuringAllocation) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBuildVectors = 10;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }
  const int32_t numProbeVectors = 5;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  const std::vector<bool> enableSpillings = {false, true};
  for (const auto enableSpilling : enableSpillings) {
    SCOPED_TRACE(fmt::format("enableSpilling {}", enableSpilling));

    auto tempDirectory = exec::test::TempDirectoryPath::create();
    auto queryPool = memory::memoryManager()->addRootPool("", kMaxBytes);

    core::PlanNodeId probeScanId;
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors, false)
                    .hashJoin(
                        {"t_k1"},
                        {"u_k1"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors, false)
                            .planNode(),
                        "",
                        concat(probeType_->names(), buildType_->names()))
                    .planNode();

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    Operator* op;
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "HashBuild") {
          //  ASSERT_FALSE(testOp->canReclaim());
            return;
          }
          op = testOp;
        })));

    std::atomic<bool> injectOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::common::memory::MemoryPoolImpl::allocateNonContiguous",
        std::function<void(memory::MemoryPoolImpl*)>(
            ([&](memory::MemoryPoolImpl* pool) {
              ASSERT_TRUE(op != nullptr);
              const std::string re(".*HashBuild");
              if (!RE2::FullMatch(pool->name(), re)) {
                return;
              }
              if (!injectOnce.exchange(false)) {
                return;
              }
              ASSERT_EQ(op->canReclaim(), enableSpilling);
              uint64_t reclaimableBytes{0};
              const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
              ASSERT_EQ(reclaimable, enableSpilling);
              if (enableSpilling) {
                ASSERT_GE(reclaimableBytes, 0);
              } else {
                ASSERT_EQ(reclaimableBytes, 0);
              }
              auto* driver = op->testingOperatorCtx()->driver();
              SuspendedSection suspendedSection(driver);
              testWait.notify();
              driverWait.wait(driverWaitKey);
            })));

    std::thread taskThread([&]() {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .numDrivers(numDrivers_)
          .planNode(plan)
          .queryPool(std::move(queryPool))
          .injectSpill(false)
          .spillDirectory(enableSpilling ? tempDirectory->path : "")
          .referenceQuery(
              "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
          .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
            const auto statsPair = taskSpilledStats(*task);
            ASSERT_EQ(statsPair.first.spilledBytes, 0);
            ASSERT_EQ(statsPair.first.spilledPartitions, 0);
            ASSERT_EQ(statsPair.second.spilledBytes, 0);
            ASSERT_EQ(statsPair.second.spilledPartitions, 0);
            verifyTaskSpilledRuntimeStats(*task, false);
          })
          .run();
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->testingOperatorCtx()->task();
    auto taskPauseWait = task->requestPause();
    taskPauseWait.wait();

    uint64_t reclaimableBytes{0};
    const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
    ASSERT_EQ(op->canReclaim(), enableSpilling);
    ASSERT_EQ(reclaimable, enableSpilling);
    if (enableSpilling) {
      ASSERT_GE(reclaimableBytes, 0);
    } else {
      ASSERT_EQ(reclaimableBytes, 0);
    }
    VELOX_ASSERT_THROW(
        op->reclaim(
            folly::Random::oneIn(2) ? 0 : folly::Random::rand32(),
            reclaimerStats_),
        "");

    driverWait.notify();
    Task::resume(task);
    task.reset();

    taskThread.join();
  }
  ASSERT_EQ(reclaimerStats_, memory::MemoryReclaimer::Stats{0});
}

DEBUG_ONLY_TEST_F(HashJoinTest, reclaimDuringOutputProcessing) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBuildVectors = 10;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }
  const int32_t numProbeVectors = 5;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  const std::vector<bool> enableSpillings = {false, true};
  for (const auto enableSpilling : enableSpillings) {
    SCOPED_TRACE(fmt::format("enableSpilling {}", enableSpilling));
    auto tempDirectory = exec::test::TempDirectoryPath::create();
    auto queryPool = memory::memoryManager()->addRootPool(
        "", kMaxBytes, memory::MemoryReclaimer::create());

    core::PlanNodeId probeScanId;
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors, false)
                    .hashJoin(
                        {"t_k1"},
                        {"u_k1"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors, false)
                            .planNode(),
                        "",
                        concat(probeType_->names(), buildType_->names()))
                    .planNode();

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    std::atomic<bool> injectOnce{true};
    Operator* op;
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::noMoreInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "HashBuild") {
            //ASSERT_FALSE(testOp->canReclaim());
            return;
          }
          op = testOp;
          if (!injectOnce.exchange(false)) {
            return;
          }
          ASSERT_EQ(op->canReclaim(), enableSpilling);
          uint64_t reclaimableBytes{0};
          const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
          ASSERT_EQ(reclaimable, enableSpilling);
          if (enableSpilling) {
            ASSERT_GT(reclaimableBytes, 0);
          } else {
            ASSERT_EQ(reclaimableBytes, 0);
          }
          testWait.notify();
          driverWait.wait(driverWaitKey);
        })));

    std::thread taskThread([&]() {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .numDrivers(numDrivers_)
          .planNode(plan)
          .queryPool(std::move(queryPool))
          .injectSpill(false)
          .spillDirectory(enableSpilling ? tempDirectory->path : "")
          .referenceQuery(
              "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
          .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
            const auto statsPair = taskSpilledStats(*task);
            ASSERT_EQ(statsPair.first.spilledBytes, 0);
            ASSERT_EQ(statsPair.first.spilledPartitions, 0);
            ASSERT_EQ(statsPair.second.spilledBytes, 0);
            ASSERT_EQ(statsPair.second.spilledPartitions, 0);
            verifyTaskSpilledRuntimeStats(*task, false);
          })
          .run();
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->testingOperatorCtx()->task();
    auto taskPauseWait = task->requestPause();
    driverWait.notify();
    taskPauseWait.wait();

    uint64_t reclaimableBytes{0};
    const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
    ASSERT_EQ(op->canReclaim(), enableSpilling);
    ASSERT_EQ(reclaimable, enableSpilling);

    if (enableSpilling) {
      ASSERT_GT(reclaimableBytes, 0);
      const auto usedMemoryBytes = op->pool()->currentBytes();
      reclaimAndRestoreCapacity(
          op,
          folly::Random::oneIn(2) ? 0 : folly::Random::rand32(),
          reclaimerStats_);
      ASSERT_GT(reclaimerStats_.reclaimedBytes, 0);
      ASSERT_GT(reclaimerStats_.reclaimExecTimeUs, 0);
      // No reclaim as the operator has started output processing.
      ASSERT_EQ(usedMemoryBytes, op->pool()->currentBytes());
    } else {
      ASSERT_EQ(reclaimableBytes, 0);
      VELOX_ASSERT_THROW(
          op->reclaim(
              folly::Random::oneIn(2) ? 0 : folly::Random::rand32(),
              reclaimerStats_),
          "");
    }

    Task::resume(task);
    task.reset();

    taskThread.join();
  }
  ASSERT_EQ(reclaimerStats_.numNonReclaimableAttempts, 1);
}

DEBUG_ONLY_TEST_F(HashJoinTest, reclaimDuringWaitForProbe) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBuildVectors = 10;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }
  const int32_t numProbeVectors = 5;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto tempDirectory = exec::test::TempDirectoryPath::create();
  auto queryPool = memory::memoryManager()->addRootPool(
      "", kMaxBytes, memory::MemoryReclaimer::create());

  core::PlanNodeId probeScanId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors, false)
                  .hashJoin(
                      {"t_k1"},
                      {"u_k1"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors, false)
                          .planNode(),
                      "",
                      concat(probeType_->names(), buildType_->names()))
                  .planNode();

  folly::EventCount driverWait;
  auto driverWaitKey = driverWait.prepareWait();
  folly::EventCount testWait;
  auto testWaitKey = testWait.prepareWait();

  Operator* op;
  std::atomic<bool> injectSpillOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::addInput",
      std::function<void(Operator*)>(([&](Operator* testOp) {
        if (testOp->operatorType() != "HashBuild") {
          //ASSERT_FALSE(testOp->canReclaim());
          return;
        }
        op = testOp;
        if (!injectSpillOnce.exchange(false)) {
          return;
        }
        auto* driver = op->testingOperatorCtx()->driver();
        auto task = driver->task();
        SuspendedSection suspendedSection(driver);
        auto taskPauseWait = task->requestPause();
        taskPauseWait.wait();
        op->reclaim(0, reclaimerStats_);
        Task::resume(task);
      })));

  std::atomic<bool> injectOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::noMoreInput",
      std::function<void(Operator*)>(([&](Operator* testOp) {
        if (testOp->operatorType() != "HashProbe") {
          return;
        }
        //ASSERT_FALSE(testOp->canReclaim());
        if (!injectOnce.exchange(false)) {
          return;
        }
        ASSERT_TRUE(op != nullptr);
        ASSERT_TRUE(op->canReclaim());
        uint64_t reclaimableBytes{0};
        const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
        ASSERT_TRUE(reclaimable);
        ASSERT_GT(reclaimableBytes, 0);
        testWait.notify();
        auto* driver = testOp->testingOperatorCtx()->driver();
        auto task = driver->task();
        SuspendedSection suspendedSection(driver);
        driverWait.wait(driverWaitKey);
      })));

  std::thread taskThread([&]() {
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .planNode(plan)
        .queryPool(std::move(queryPool))
        .injectSpill(false)
        .spillDirectory(tempDirectory->path)
        .referenceQuery(
            "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_GT(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 4);
          ASSERT_GT(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 4);
        })
        .run();
  });

  testWait.wait(testWaitKey);
  ASSERT_TRUE(op != nullptr);
  auto task = op->testingOperatorCtx()->task();
  auto taskPauseWait = task->requestPause();
  taskPauseWait.wait();

  uint64_t reclaimableBytes{0};
  const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
  ASSERT_TRUE(op->canReclaim());
  ASSERT_TRUE(reclaimable);
  ASSERT_GT(reclaimableBytes, 0);

  const auto usedMemoryBytes = op->pool()->currentBytes();
  reclaimerStats_.reset();
  reclaimAndRestoreCapacity(
      op,
      folly::Random::oneIn(2) ? 0 : folly::Random::rand32(),
      reclaimerStats_);
  ASSERT_GT(reclaimerStats_.reclaimedBytes, 0);
  ASSERT_GT(reclaimerStats_.reclaimExecTimeUs, 0);
  // No reclaim as the build operator is not in building table state.
  ASSERT_EQ(usedMemoryBytes, op->pool()->currentBytes());

  driverWait.notify();
  Task::resume(task);
  task.reset();

  taskThread.join();
  ASSERT_EQ(reclaimerStats_.numNonReclaimableAttempts, 1);
}

DEBUG_ONLY_TEST_F(HashJoinTest, hashBuildAbortDuringOutputProcessing) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBuildVectors = 10;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }
  const int32_t numProbeVectors = 5;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  struct {
    bool abortFromRootMemoryPool;
    int numDrivers;

    std::string debugString() const {
      return fmt::format(
          "abortFromRootMemoryPool {} numDrivers {}",
          abortFromRootMemoryPool,
          numDrivers);
    }
  } testSettings[] = {{true, 1}, {false, 1}, {true, 4}, {false, 4}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto queryPool = memory::memoryManager()->addRootPool(
        "", kMaxBytes, memory::MemoryReclaimer::create());

    core::PlanNodeId probeScanId;
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors, true)
                    .hashJoin(
                        {"t_k1"},
                        {"u_k1"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors, true)
                            .planNode(),
                        "",
                        concat(probeType_->names(), buildType_->names()))
                    .planNode();

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    std::atomic<bool> injectOnce{true};
    Operator* op;
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::noMoreInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "HashBuild") {
            return;
          }
          op = testOp;
          if (!injectOnce.exchange(false)) {
            return;
          }
          auto* driver = op->testingOperatorCtx()->driver();
          ASSERT_EQ(
              driver->task()->enterSuspended(driver->state()),
              StopReason::kNone);
          testWait.notify();
          driverWait.wait(driverWaitKey);
          ASSERT_EQ(
              driver->task()->leaveSuspended(driver->state()),
              StopReason::kAlreadyTerminated);
          VELOX_MEM_POOL_ABORTED("Memory pool aborted");
        })));

    std::thread taskThread([&]() {
      VELOX_ASSERT_THROW(
          HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
              .numDrivers(numDrivers_)
              .planNode(plan)
              .queryPool(std::move(queryPool))
              .injectSpill(false)
              .referenceQuery(
                  "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
              .run(),
          "");
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->testingOperatorCtx()->task();
    testData.abortFromRootMemoryPool ? abortPool(queryPool.get())
                                     : abortPool(op->pool());
    ASSERT_TRUE(op->pool()->aborted());
    ASSERT_TRUE(queryPool->aborted());
    ASSERT_EQ(queryPool->currentBytes(), 0);
    driverWait.notify();
    taskThread.join();
    task.reset();
    waitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(HashJoinTest, hashBuildAbortDuringInputgProcessing) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBuildVectors = 10;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }
  const int32_t numProbeVectors = 5;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  struct {
    bool abortFromRootMemoryPool;
    int numDrivers;

    std::string debugString() const {
      return fmt::format(
          "abortFromRootMemoryPool {} numDrivers {}",
          abortFromRootMemoryPool,
          numDrivers);
    }
  } testSettings[] = {{true, 1}, {false, 1}, {true, 4}, {false, 4}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto queryPool = memory::memoryManager()->addRootPool(
        "", kMaxBytes, memory::MemoryReclaimer::create());

    core::PlanNodeId probeScanId;
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors, true)
                    .hashJoin(
                        {"t_k1"},
                        {"u_k1"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors, true)
                            .planNode(),
                        "",
                        concat(probeType_->names(), buildType_->names()))
                    .planNode();

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    std::atomic<int> numInputs{0};
    Operator* op;
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "HashBuild") {
            return;
          }
          op = testOp;
          ++numInputs;
          if (numInputs != 2) {
            return;
          }
          auto* driver = op->testingOperatorCtx()->driver();
          ASSERT_EQ(
              driver->task()->enterSuspended(driver->state()),
              StopReason::kNone);
          testWait.notify();
          driverWait.wait(driverWaitKey);
          ASSERT_EQ(
              driver->task()->leaveSuspended(driver->state()),
              StopReason::kAlreadyTerminated);
          VELOX_MEM_POOL_ABORTED("Memory pool aborted");
        })));

    std::thread taskThread([&]() {
      VELOX_ASSERT_THROW(
          HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
              .numDrivers(numDrivers_)
              .planNode(plan)
              .queryPool(std::move(queryPool))
              .injectSpill(false)
              .referenceQuery(
                  "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
              .run(),
          "");
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->testingOperatorCtx()->task();
    testData.abortFromRootMemoryPool ? abortPool(queryPool.get())
                                     : abortPool(op->pool());
    ASSERT_TRUE(op->pool()->aborted());
    ASSERT_TRUE(queryPool->aborted());
    ASSERT_EQ(queryPool->currentBytes(), 0);
    driverWait.notify();
    taskThread.join();
    task.reset();
    waitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(HashJoinTest, hashProbeAbortDuringInputProcessing) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBuildVectors = 10;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }
  const int32_t numProbeVectors = 5;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  struct {
    bool abortFromRootMemoryPool;
    int numDrivers;

    std::string debugString() const {
      return fmt::format(
          "abortFromRootMemoryPool {} numDrivers {}",
          abortFromRootMemoryPool,
          numDrivers);
    }
  } testSettings[] = {{true, 1}, {false, 1}, {true, 4}, {false, 4}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto queryPool = memory::memoryManager()->addRootPool(
        "", kMaxBytes, memory::MemoryReclaimer::create());

    core::PlanNodeId probeScanId;
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors, true)
                    .hashJoin(
                        {"t_k1"},
                        {"u_k1"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors, true)
                            .planNode(),
                        "",
                        concat(probeType_->names(), buildType_->names()))
                    .planNode();

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    std::atomic<int> numInputs{0};
    Operator* op;
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "HashProbe") {
            return;
          }
          op = testOp;
          ++numInputs;
          if (numInputs != 2) {
            return;
          }
          auto* driver = op->testingOperatorCtx()->driver();
          ASSERT_EQ(
              driver->task()->enterSuspended(driver->state()),
              StopReason::kNone);
          testWait.notify();
          driverWait.wait(driverWaitKey);
          ASSERT_EQ(
              driver->task()->leaveSuspended(driver->state()),
              StopReason::kAlreadyTerminated);
          VELOX_MEM_POOL_ABORTED("Memory pool aborted");
        })));

    std::thread taskThread([&]() {
      VELOX_ASSERT_THROW(
          HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
              .numDrivers(numDrivers_)
              .planNode(plan)
              .queryPool(std::move(queryPool))
              .injectSpill(false)
              .referenceQuery(
                  "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
              .run(),
          "");
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->testingOperatorCtx()->task();
    testData.abortFromRootMemoryPool ? abortPool(queryPool.get())
                                     : abortPool(op->pool());
    ASSERT_TRUE(op->pool()->aborted());
    ASSERT_TRUE(queryPool->aborted());
    ASSERT_EQ(queryPool->currentBytes(), 0);
    driverWait.notify();
    taskThread.join();
    task.reset();
    waitForAllTasksToBeDeleted();
  }
}

TEST_F(HashJoinTest, leftJoinWithMissAtEndOfBatch) {
  // Tests some cases where the row at the end of an output batch fails the
  // filter.
  auto probeVectors = std::vector<RowVectorPtr>{makeRowVector(
      {"t_k1", "t_k2"},
      {makeFlatVector<int32_t>(20, [](auto row) { return 1 + row % 2; }),
       makeFlatVector<int32_t>(20, [](auto row) { return row; })})};
  auto buildVectors = std::vector<RowVectorPtr>{
      makeRowVector({"u_k1"}, {makeFlatVector<int32_t>({1, 2})})};
  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", {buildVectors});
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto test = [&](const std::string& filter) {
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors, true)
                    .hashJoin(
                        {"t_k1"},
                        {"u_k1"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors, true)
                            .planNode(),
                        filter,
                        {"t_k1", "u_k1"},
                        core::JoinType::kLeft)
                    .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(plan)
        .injectSpill(false)
        .checkSpillStats(false)
        .spillMemoryThreshold(1)
        .maxSpillLevel(0)
        .numDrivers(1)
        .config(
            core::QueryConfig::kPreferredOutputBatchRows, std::to_string(10))
        .referenceQuery(fmt::format(
            "SELECT t_k1, u_k1 from t left join u on t_k1 = u_k1 and {}",
            filter))
        .run();
  };

  // Alternate rows pass this filter and last row of a batch fails.
  test("t_k1=1");

  // All rows fail this filter.
  test("t_k1=5");

  // All rows in the second batch pass this filter.
  test("t_k2 > 9");
}

TEST_F(HashJoinTest, leftJoinWithMissAtEndOfBatchMultipleBuildMatches) {
  // Tests some cases where the row at the end of an output batch fails the
  // filter and there are multiple matches with the build side..
  auto probeVectors = std::vector<RowVectorPtr>{makeRowVector(
      {"t_k1", "t_k2"},
      {makeFlatVector<int32_t>(10, [](auto row) { return 1 + row % 2; }),
       makeFlatVector<int32_t>(10, [](auto row) { return row; })})};
  auto buildVectors = std::vector<RowVectorPtr>{
      makeRowVector({"u_k1"}, {makeFlatVector<int32_t>({1, 2, 1, 2})})};
  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", {buildVectors});
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto test = [&](const std::string& filter) {
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors, true)
                    .hashJoin(
                        {"t_k1"},
                        {"u_k1"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors, true)
                            .planNode(),
                        filter,
                        {"t_k1", "u_k1"},
                        core::JoinType::kLeft)
                    .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(plan)
        .injectSpill(false)
        .checkSpillStats(false)
        .spillMemoryThreshold(1)
        .maxSpillLevel(0)
        .numDrivers(1)
        .config(
            core::QueryConfig::kPreferredOutputBatchRows, std::to_string(10))
        .referenceQuery(fmt::format(
            "SELECT t_k1, u_k1 from t left join u on t_k1 = u_k1 and {}",
            filter))
        .run();
  };

  // In this case the rows with t_k2 = 4 appear at the end of the first batch,
  // meaning the last rows in that output batch are misses, and don't get added.
  // The rows with t_k2 = 8 appear in the second batch so only one row is
  // written, meaning there is space in the second output batch for the miss
  // with tk_2 = 4 to get written.
  test("t_k2 != 4 and t_k2 != 8");
}

DEBUG_ONLY_TEST_F(HashJoinTest, minSpillableMemoryReservation) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBuildVectors = 10;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    buildVectors.push_back(fuzzer.fuzzInputRow(buildType_));
  }
  const int32_t numProbeVectors = 5;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    probeVectors.push_back(fuzzer.fuzzInputRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  core::PlanNodeId probeScanId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors, false)
                  .hashJoin(
                      {"t_k1"},
                      {"u_k1"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors, false)
                          .planNode(),
                      "",
                      concat(probeType_->names(), buildType_->names()))
                  .planNode();

  for (int32_t minSpillableReservationPct : {5, 50, 100}) {
    SCOPED_TRACE(fmt::format(
        "minSpillableReservationPct: {}", minSpillableReservationPct));

    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::HashBuild::addInput",
        std::function<void(exec::HashBuild*)>(([&](exec::HashBuild* hashBuild) {
          memory::MemoryPool* pool = hashBuild->pool();
          const auto availableReservationBytes = pool->availableReservation();
          const auto currentUsedBytes = pool->currentBytes();
          // Verifies we always have min reservation after ensuring the input.
          ASSERT_GE(
              availableReservationBytes,
              currentUsedBytes * minSpillableReservationPct / 100);
        })));

    auto tempDirectory = exec::test::TempDirectoryPath::create();
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .planNode(plan)
        .injectSpill(false)
        .spillDirectory(tempDirectory->path)
        .referenceQuery(
            "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
        .run();
  }
}

TEST_F(HashJoinTest, exceededMaxSpillLevel) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBuildVectors = 10;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }
  const int32_t numProbeVectors = 5;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  core::PlanNodeId probeScanId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors, false)
                  .hashJoin(
                      {"t_k1"},
                      {"u_k1"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors, false)
                          .planNode(),
                      "",
                      concat(probeType_->names(), buildType_->names()))
                  .planNode();

  auto tempDirectory = exec::test::TempDirectoryPath::create();
  const int exceededMaxSpillLevelCount =
      common::globalSpillStats().spillMaxLevelExceededCount;
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(1)
      .planNode(plan)
      .injectSpill(false)
      // Always trigger spilling.
      .spillMemoryThreshold(1)
      .maxSpillLevel(0)
      .spillDirectory(tempDirectory->path)
      .referenceQuery(
          "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
      .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
        auto joinStats = task->taskStats()
                             .pipelineStats.back()
                             .operatorStats.back()
                             .runtimeStats;
        ASSERT_EQ(joinStats["exceededMaxSpillLevel"].sum, 4);
        ASSERT_EQ(joinStats["exceededMaxSpillLevel"].count, 4);
      })
      .run();
  ASSERT_EQ(
      common::globalSpillStats().spillMaxLevelExceededCount,
      exceededMaxSpillLevelCount + 4);
}

TEST_F(HashJoinTest, maxSpillBytes) {
  const auto rowType =
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), VARCHAR()});
  const auto probeVectors = createVectors(rowType, 1024, 10 << 20);
  const auto buildVectors = createVectors(rowType, 1024, 10 << 20);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors, true)
                  .project({"c0", "c1", "c2"})
                  .hashJoin(
                      {"c0"},
                      {"u1"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors, true)
                          .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                          .planNode(),
                      "",
                      {"c0", "c1", "c2"},
                      core::JoinType::kInner)
                  .planNode();

  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto queryCtx = std::make_shared<core::QueryCtx>(executor_.get());

  struct {
    int32_t maxSpilledBytes;
    bool expectedExceedLimit;
    std::string debugString() const {
      return fmt::format("maxSpilledBytes {}", maxSpilledBytes);
    }
  } testSettings[] = {{1 << 30, false}, {16 << 20, true}, {0, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    try {
      AssertQueryBuilder(plan)
          .spillDirectory(spillDirectory->path)
          .queryCtx(queryCtx)
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kJoinSpillEnabled, true)
          // Set a small capacity to trigger threshold based spilling
          .config(core::QueryConfig::kJoinSpillMemoryThreshold, 5 << 20)
          .config(core::QueryConfig::kMaxSpillBytes, testData.maxSpilledBytes)
          .copyResults(pool_.get());
      ASSERT_FALSE(testData.expectedExceedLimit);
    } catch (const VeloxRuntimeError& e) {
      ASSERT_TRUE(testData.expectedExceedLimit);
      ASSERT_NE(
          e.message().find(
              "Query exceeded per-query local spill limit of 16.00MB"),
          std::string::npos);
      ASSERT_EQ(
          e.errorCode(), facebook::velox::error_code::kSpillLimitExceeded);
    }
  }
}

TEST_F(HashJoinTest, onlyHashBuildMaxSpillBytes) {
  const auto rowType =
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), VARCHAR()});
  const auto probeVectors = createVectors(rowType, 32, 128);
  const auto buildVectors = createVectors(rowType, 1024, 10 << 20);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors, true)
                  .hashJoin(
                      {"c0"},
                      {"u1"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors, true)
                          .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                          .planNode(),
                      "",
                      {"c0", "c1", "c2"},
                      core::JoinType::kInner)
                  .planNode();

  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto queryCtx = std::make_shared<core::QueryCtx>(executor_.get());

  struct {
    int32_t maxSpilledBytes;
    bool expectedExceedLimit;
    std::string debugString() const {
      return fmt::format("maxSpilledBytes {}", maxSpilledBytes);
    }
  } testSettings[] = {{1 << 30, false}, {16 << 20, true}, {0, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    try {
      AssertQueryBuilder(plan)
          .spillDirectory(spillDirectory->path)
          .queryCtx(queryCtx)
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kJoinSpillEnabled, true)
          // Set a small capacity to trigger threshold based spilling
          .config(core::QueryConfig::kJoinSpillMemoryThreshold, 5 << 20)
          .config(core::QueryConfig::kMaxSpillBytes, testData.maxSpilledBytes)
          .copyResults(pool_.get());
      ASSERT_FALSE(testData.expectedExceedLimit);
    } catch (const VeloxRuntimeError& e) {
      ASSERT_TRUE(testData.expectedExceedLimit);
      ASSERT_NE(
          e.message().find(
              "Query exceeded per-query local spill limit of 16.00MB"),
          std::string::npos);
      ASSERT_EQ(
          e.errorCode(), facebook::velox::error_code::kSpillLimitExceeded);
    }
  }
}

DEBUG_ONLY_TEST_F(HashJoinTest, reclaimFromJoinBuild) {
  std::vector<RowVectorPtr> vectors = createVectors(8, buildType_, fuzzerOpts_);
  createDuckDbTable(vectors);
  std::unique_ptr<memory::MemoryManager> memoryManager = createMemoryManager();
  std::vector<bool> sameQueries = {false, true};
  for (bool sameQuery : sameQueries) {
    SCOPED_TRACE(fmt::format("sameQuery {}", sameQuery));
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    std::shared_ptr<core::QueryCtx> fakeQueryCtx =
        newQueryCtx(memoryManager, executor_, kMemoryCapacity * 2);
    std::shared_ptr<core::QueryCtx> joinQueryCtx;
    if (sameQuery) {
      joinQueryCtx = fakeQueryCtx;
    } else {
      joinQueryCtx = newQueryCtx(memoryManager, executor_, kMemoryCapacity * 2);
    }

    folly::EventCount arbitrationWait;
    std::atomic_bool arbitrationWaitFlag{true};
    folly::EventCount taskPauseWait;
    std::atomic_bool taskPauseWaitFlag{true};

    std::atomic_int numInputs{0};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "HashBuild") {
            return;
          }
          if (++numInputs != 5) {
            return;
          }
          arbitrationWaitFlag = false;
          arbitrationWait.notifyAll();

          // Wait for task pause to be triggered.
          taskPauseWait.await([&] { return !taskPauseWaitFlag.load(); });
        })));

    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Task::requestPauseLocked",
        std::function<void(Task*)>(([&](Task* /*unused*/) {
          taskPauseWaitFlag = false;
          taskPauseWait.notifyAll();
        })));

    std::unordered_map<std::string, std::string> config{
        {core::QueryConfig::kSpillEnabled, "true"},
        {core::QueryConfig::kJoinSpillEnabled, "true"},
        {core::QueryConfig::kJoinSpillPartitionBits, "2"},
    };
    joinQueryCtx->testingOverrideConfigUnsafe(std::move(config));

    std::thread joinThread([&]() {
      auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
      auto task =
          AssertQueryBuilder(duckDbQueryRunner_)
              .spillDirectory(spillDirectory->path)
              .queryCtx(joinQueryCtx)
              .plan(PlanBuilder(planNodeIdGenerator)
                        .values(vectors)
                        .project({"u_k1 AS t0", "u_k2 AS t1", "u_v1 AS t2"})
                        .hashJoin(
                            {"t0"},
                            {"u0"},
                            PlanBuilder(planNodeIdGenerator)
                                .values(vectors)
                                .project(
                                    {"u_k1 AS u0", "u_k2 AS u1", "u_v1 AS u2"})
                                .planNode(),
                            "",
                            {"t1"},
                            core::JoinType::kAnti)
                        .planNode())
              .assertResults(
                  "SELECT u_k2 FROM tmp WHERE u_k1 NOT IN (SELECT u_k1 FROM tmp)");
      auto stats = task->taskStats().pipelineStats;
      ASSERT_GT(stats[1].operatorStats[2].spilledBytes, 0);
    });

    arbitrationWait.await([&] { return !arbitrationWaitFlag.load(); });

    auto fakePool = fakeQueryCtx->pool()->addLeafChild(
        "fakePool", true, FakeMemoryReclaimer::create());

    memoryManager->testingGrowPool(
        fakePool.get(), memoryManager->arbitrator()->capacity());

    joinThread.join();

    waitForAllTasksToBeDeleted();
  }
}

TEST_F(HashJoinTest, reclaimFromCompletedJoinBuilder) {
  std::vector<RowVectorPtr> vectors = createVectors(8, buildType_, fuzzerOpts_);
  createDuckDbTable(vectors);
  std::unique_ptr<memory::MemoryManager> memoryManager = createMemoryManager();
  std::vector<bool> sameQueries = {false, true};
  for (bool sameQuery : sameQueries) {
    SCOPED_TRACE(fmt::format("sameQuery {}", sameQuery));
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    std::shared_ptr<core::QueryCtx> fakeQueryCtx =
        newQueryCtx(memoryManager, executor_, kMemoryCapacity * 2);
    std::shared_ptr<core::QueryCtx> joinQueryCtx;
    if (sameQuery) {
      joinQueryCtx = fakeQueryCtx;
    } else {
      joinQueryCtx = newQueryCtx(memoryManager, executor_, kMemoryCapacity * 2);
    }

    folly::EventCount arbitrationWait;
    std::atomic_bool arbitrationWaitFlag{true};

    std::thread joinThread([&]() {
      auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
      auto task =
          AssertQueryBuilder(duckDbQueryRunner_)
              .spillDirectory(spillDirectory->path)
              .queryCtx(joinQueryCtx)
              .plan(PlanBuilder(planNodeIdGenerator)
                        .values(vectors)
                        .project({"u_k1 AS t0", "u_k2 AS t1", "u_v1 AS t2"})
                        .hashJoin(
                            {"t0"},
                            {"u0"},
                            PlanBuilder(planNodeIdGenerator)
                                .values(vectors)
                                .project(
                                    {"u_k1 AS u0", "u_k2 AS u1", "u_v1 AS u2"})
                                .planNode(),
                            "",
                            {"t1"},
                            core::JoinType::kAnti)
                        .planNode())
              .assertResults(
                  "SELECT u_k2 FROM tmp WHERE u_k1 NOT IN (SELECT u_k1 FROM tmp)");
      waitForTaskCompletion(task.get());
      arbitrationWaitFlag = false;
      arbitrationWait.notifyAll();
      auto stats = task->taskStats().pipelineStats;
      ASSERT_EQ(stats[1].operatorStats[2].spilledBytes, 0);
    });

    arbitrationWait.await([&] { return !arbitrationWaitFlag.load(); });

    auto fakePool = fakeQueryCtx->pool()->addLeafChild(
        "fakePool", true, FakeMemoryReclaimer::create());

    memoryManager->testingGrowPool(
        fakePool.get(), memoryManager->arbitrator()->capacity());

    joinThread.join();

    waitForAllTasksToBeDeleted();
  }
}

TEST_F(HashJoinTest, reclaimFromJoinBuilderWithMultiDrivers) {
  std::unique_ptr<memory::MemoryManager> memoryManager = createMemoryManager();
  const auto& arbitrator = memoryManager->arbitrator();
  auto rowType = ROW({
      {"c0", INTEGER()},
      {"c1", INTEGER()},
      {"c2", VARCHAR()},
  });
  const auto vectors = createVectors(rowType, 64 << 20, fuzzerOpts_);
  const int numDrivers = 4;
  const auto expectedResult =
      runHashJoinTask(vectors, nullptr, numDrivers, pool(), false).data;
  // Create a query ctx with a small capacity to trigger spilling.
  std::shared_ptr<core::QueryCtx> queryCtx =
      newQueryCtx(memoryManager, executor_, 128 << 20);
  auto result = runHashJoinTask(
      vectors, queryCtx, numDrivers, pool(), true, expectedResult);
  auto taskStats = exec::toPlanStats(result.task->taskStats());
  auto& planStats = taskStats.at(result.planNodeId);
  ASSERT_GT(planStats.spilledBytes, 0);
  result.task.reset();
  waitForAllTasksToBeDeleted();
  ASSERT_GT(arbitrator->stats().numRequests, 0);
  ASSERT_GT(arbitrator->stats().numReclaimedBytes, 0);
}

DEBUG_ONLY_TEST_F(
    HashJoinTest,
    failedToReclaimFromHashJoinBuildersInNonReclaimableSection) {
  std::unique_ptr<memory::MemoryManager> memoryManager = createMemoryManager();
  const auto& arbitrator = memoryManager->arbitrator();
  auto rowType = ROW({
      {"c0", INTEGER()},
      {"c1", INTEGER()},
      {"c2", VARCHAR()},
  });
  const auto vectors = createVectors(rowType, 64 << 20, fuzzerOpts_);
  const int numDrivers = 1;
  std::shared_ptr<core::QueryCtx> queryCtx =
      newQueryCtx(memoryManager, executor_, kMemoryCapacity);
  const auto expectedResult =
      runHashJoinTask(vectors, queryCtx, numDrivers, pool(), false).data;

  std::atomic_bool nonReclaimableSectionWaitFlag{true};
  folly::EventCount nonReclaimableSectionWait;
  std::atomic_bool memoryArbitrationWaitFlag{true};
  folly::EventCount memoryArbitrationWait;

  std::atomic<bool> injectNonReclaimableSectionOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::common::memory::MemoryPoolImpl::allocateNonContiguous",
      std::function<void(memory::MemoryPoolImpl*)>(
          ([&](memory::MemoryPoolImpl* pool) {
            if (!isHashBuildMemoryPool(*pool)) {
              return;
            }
            if (!injectNonReclaimableSectionOnce.exchange(false)) {
              return;
            }

            // Signal the test control that one of the hash build operator has
            // entered into non-reclaimable section.
            nonReclaimableSectionWaitFlag = false;
            nonReclaimableSectionWait.notifyAll();

            // Suspend the driver to simulate the arbitration.
            pool->reclaimer()->enterArbitration();
            // Wait for the memory arbitration to complete.
            memoryArbitrationWait.await(
                [&]() { return !memoryArbitrationWaitFlag.load(); });
            pool->reclaimer()->leaveArbitration();
          })));

  std::thread joinThread([&]() {
    const auto result = runHashJoinTask(
        vectors, queryCtx, numDrivers, pool(), true, expectedResult);
    auto taskStats = exec::toPlanStats(result.task->taskStats());
    auto& planStats = taskStats.at(result.planNodeId);
    ASSERT_EQ(planStats.spilledBytes, 0);
  });

  auto fakePool = queryCtx->pool()->addLeafChild(
      "fakePool", true, FakeMemoryReclaimer::create());
  // Wait for the hash build operators to enter into non-reclaimable section.
  nonReclaimableSectionWait.await(
      [&]() { return !nonReclaimableSectionWaitFlag.load(); });

  // We expect capacity grow fails as we can't reclaim from hash join operators.
  ASSERT_FALSE(memoryManager->testingGrowPool(fakePool.get(), kMemoryCapacity));

  // Notify the hash build operator that memory arbitration has been done.
  memoryArbitrationWaitFlag = false;
  memoryArbitrationWait.notifyAll();

  joinThread.join();
  waitForAllTasksToBeDeleted();
  ASSERT_EQ(arbitrator->stats().numNonReclaimableAttempts, 1);
}

DEBUG_ONLY_TEST_F(HashJoinTest, reclaimFromHashJoinBuildInWaitForTableBuild) {
  std::unique_ptr<memory::MemoryManager> memoryManager = createMemoryManager();
  const auto& arbitrator = memoryManager->arbitrator();
  auto rowType = ROW({
      {"c0", INTEGER()},
      {"c1", INTEGER()},
      {"c2", VARCHAR()},
  });
  const auto vectors = createVectors(rowType, 32 << 20, fuzzerOpts_);
  const int numDrivers = 4;
  const auto expectedResult =
      runHashJoinTask(vectors, nullptr, numDrivers, pool(), false).data;
  std::shared_ptr<core::QueryCtx> queryCtx =
      newQueryCtx(memoryManager, executor_, kMemoryCapacity);

  folly::EventCount arbitrationWait;
  std::atomic_bool arbitrationWaitFlag{true};
  folly::EventCount taskPauseWait;
  std::atomic_bool taskPauseWaitFlag{true};

  std::atomic_int blockedBuildOperators{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal",
      std::function<void(Driver*)>(([&](Driver* driver) {
        // Check if the driver is from hash join build.
        if (driver->driverCtx()->pipelineId != 1) {
          return;
        }

        if (++blockedBuildOperators > numDrivers - 1) {
          return;
        }

        taskPauseWait.await([&]() { return !taskPauseWaitFlag.load(); });
      })));

  std::atomic_bool injectNoMoreInputOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::noMoreInput",
      std::function<void(Operator*)>(([&](Operator* op) {
        if (op->operatorType() != "HashBuild") {
          return;
        }

        if (!injectNoMoreInputOnce.exchange(false)) {
          return;
        }

        arbitrationWaitFlag = false;
        arbitrationWait.notifyAll();
        taskPauseWait.await([&]() { return !taskPauseWaitFlag.load(); });
      })));

  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Task::requestPauseLocked",
      std::function<void(Task*)>([&](Task* /*unused*/) {
        taskPauseWaitFlag = false;
        taskPauseWait.notifyAll();
      }));

  std::thread joinThread([&]() {
    VELOX_ASSERT_THROW(
        runHashJoinTask(
            vectors, queryCtx, numDrivers, pool(), true, expectedResult),
        "Exceeded memory pool cap of");
  });

  arbitrationWait.await([&] { return !arbitrationWaitFlag.load(); });
  auto fakePool = queryCtx->pool()->addLeafChild(
      "fakePool", true, FakeMemoryReclaimer::create());
  void* fakeBuffer{nullptr};
  arbitrationWait.await([&]() { return !arbitrationWaitFlag.load(); });
  // Let the first hash build operator reaches to wait for table build state.
  std::this_thread::sleep_for(std::chrono::seconds(1));
  fakeBuffer = fakePool->allocate(kMemoryCapacity);

  joinThread.join();

  // We expect the reclaimed bytes from hash build.
  ASSERT_GT(arbitrator->stats().numReclaimedBytes, 0);
  waitForAllTasksToBeDeleted();
  ASSERT_TRUE(fakeBuffer != nullptr);
  fakePool->free(fakeBuffer, kMemoryCapacity);
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(HashJoinTest, arbitrationTriggeredDuringParallelJoinBuild) {
  std::unique_ptr<memory::MemoryManager> memoryManager = createMemoryManager();
  const auto& arbitrator = memoryManager->arbitrator();
  auto rowType = ROW({
      {"c0", INTEGER()},
      {"c1", INTEGER()},
      {"c2", VARCHAR()},
  });
  // Build a large vector to trigger memory arbitration.
  fuzzerOpts_.vectorSize = 10'000;
  std::vector<RowVectorPtr> vectors = createVectors(2, rowType, fuzzerOpts_);
  createDuckDbTable(vectors);

  const int numDrivers = 4;
  std::shared_ptr<core::QueryCtx> joinQueryCtx =
      newQueryCtx(memoryManager, executor_, kMemoryCapacity);
  // Make sure the parallel build has been triggered.
  std::atomic<bool> parallelBuildTriggered{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashTable::parallelJoinBuild",
      std::function<void(void*)>(
          [&](void*) { parallelBuildTriggered = true; }));

  // TODO: add driver context to test if the memory allocation is triggered in
  // driver context or not.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  AssertQueryBuilder(duckDbQueryRunner_)
      // Set very low table size threshold to trigger parallel build.
      .config(core::QueryConfig::kMinTableRowsForParallelJoinBuild, 0)
      // Set multiple hash build drivers to trigger parallel build.
      .maxDrivers(4)
      .queryCtx(joinQueryCtx)
      .plan(PlanBuilder(planNodeIdGenerator)
                .values(vectors, true)
                .project({"c0 AS t0", "c1 AS t1", "c2 AS t2"})
                .hashJoin(
                    {"t0", "t1"},
                    {"u1", "u0"},
                    PlanBuilder(planNodeIdGenerator)
                        .values(vectors, true)
                        .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                        .planNode(),
                    "",
                    {"t1"},
                    core::JoinType::kInner)
                .planNode())
      .assertResults(
          "SELECT t.c1 FROM tmp as t, tmp AS u WHERE t.c0 == u.c1 AND t.c1 == u.c0");
  ASSERT_TRUE(parallelBuildTriggered);
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(HashJoinTest, arbitrationTriggeredByEnsureJoinTableFit) {
  std::unique_ptr<memory::MemoryManager> memoryManager = createMemoryManager();
  const auto& arbitrator = memoryManager->arbitrator();
  auto rowType = ROW({
      {"c0", INTEGER()},
      {"c1", INTEGER()},
      {"c2", VARCHAR()},
  });
  // Build a large vector to trigger memory arbitration.
  fuzzerOpts_.vectorSize = 10'000;
  std::vector<RowVectorPtr> vectors = createVectors(2, rowType, fuzzerOpts_);
  createDuckDbTable(vectors);

  std::shared_ptr<core::QueryCtx> joinQueryCtx =
      newQueryCtx(memoryManager, executor_, kMemoryCapacity);
  std::shared_ptr<core::QueryCtx> fakeCtx =
      newQueryCtx(memoryManager, executor_, kMemoryCapacity);

  auto fakePool = fakeCtx->pool()->addLeafChild(
      "fakePool", true, FakeMemoryReclaimer::create());
  std::vector<std::unique_ptr<TestAllocation>> injectAllocations;
  std::atomic<bool> injectAllocationOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashBuild::ensureTableFits",
      std::function<void(HashBuild*)>([&](HashBuild* buildOp) {
        // Inject the allocation once to ensure the merged table allocation will
        // trigger memory arbitration.
        if (!injectAllocationOnce.exchange(false)) {
          return;
        }
        auto* buildPool = buildOp->pool();
        // Free up available reservation from the leaf build memory pool.
        uint64_t injectAllocationSize = buildPool->availableReservation();
        injectAllocations.emplace_back(new TestAllocation{
            buildPool,
            buildPool->allocate(injectAllocationSize),
            injectAllocationSize});
        // Free up available memory from the system.
        injectAllocationSize = arbitrator->stats().freeCapacityBytes +
            joinQueryCtx->pool()->freeBytes();
        injectAllocations.emplace_back(new TestAllocation{
            fakePool.get(),
            fakePool->allocate(injectAllocationSize),
            injectAllocationSize});
      }));

  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashBuild::reclaim",
      std::function<void(Operator*)>([&](Operator* /*unused*/) {
        ASSERT_EQ(injectAllocations.size(), 2);
        for (auto& injectAllocation : injectAllocations) {
          injectAllocation->free();
        }
      }));

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto task =
      AssertQueryBuilder(duckDbQueryRunner_)
          .spillDirectory(spillDirectory->path)
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kJoinSpillEnabled, true)
          .config(core::QueryConfig::kJoinSpillPartitionBits, 2)
          // Set multiple hash build drivers to trigger parallel build.
          .maxDrivers(4)
          .queryCtx(joinQueryCtx)
          .plan(PlanBuilder(planNodeIdGenerator)
                    .values(vectors, true)
                    .project({"c0 AS t0", "c1 AS t1", "c2 AS t2"})
                    .hashJoin(
                        {"t0", "t1"},
                        {"u1", "u0"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(vectors, true)
                            .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                            .planNode(),
                        "",
                        {"t1"},
                        core::JoinType::kInner)
                    .planNode())
          .assertResults(
              "SELECT t.c1 FROM tmp as t, tmp AS u WHERE t.c0 == u.c1 AND t.c1 == u.c0");
  task.reset();
  waitForAllTasksToBeDeleted();
  ASSERT_EQ(injectAllocations.size(), 2);
}

DEBUG_ONLY_TEST_F(HashJoinTest, reclaimDuringJoinTableBuild) {
  std::unique_ptr<memory::MemoryManager> memoryManager = createMemoryManager();
  const auto& arbitrator = memoryManager->arbitrator();
  auto rowType = ROW({
      {"c0", INTEGER()},
      {"c1", INTEGER()},
      {"c2", VARCHAR()},
  });
  // Build a large vector to trigger memory arbitration.
  fuzzerOpts_.vectorSize = 10'000;
  std::vector<RowVectorPtr> vectors = createVectors(2, rowType, fuzzerOpts_);
  createDuckDbTable(vectors);

  std::shared_ptr<core::QueryCtx> joinQueryCtx =
      newQueryCtx(memoryManager, executor_, kMemoryCapacity);

  std::atomic<bool> blockTableBuildOpOnce{true};
  std::atomic<bool> tableBuildBlocked{false};
  folly::EventCount tableBuildBlockWait;
  std::atomic<bool> unblockTableBuild{false};
  folly::EventCount unblockTableBuildWait;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashTable::parallelJoinBuild",
      std::function<void(memory::MemoryPool*)>(([&](memory::MemoryPool* pool) {
        if (!blockTableBuildOpOnce.exchange(false)) {
          return;
        }
        tableBuildBlocked = true;
        tableBuildBlockWait.notifyAll();
        unblockTableBuildWait.await([&]() { return unblockTableBuild.load(); });
        void* buffer = pool->allocate(kMemoryCapacity / 4);
        pool->free(buffer, kMemoryCapacity / 4);
      })));

  std::thread joinThread([&]() {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto task =
        AssertQueryBuilder(duckDbQueryRunner_)
            .spillDirectory(spillDirectory->path)
            .config(core::QueryConfig::kSpillEnabled, true)
            .config(core::QueryConfig::kJoinSpillEnabled, true)
            .config(core::QueryConfig::kJoinSpillPartitionBits, 2)
            // Set multiple hash build drivers to trigger parallel build.
            .maxDrivers(4)
            .queryCtx(joinQueryCtx)
            .plan(PlanBuilder(planNodeIdGenerator)
                      .values(vectors, true)
                      .project({"c0 AS t0", "c1 AS t1", "c2 AS t2"})
                      .hashJoin(
                          {"t0", "t1"},
                          {"u1", "u0"},
                          PlanBuilder(planNodeIdGenerator)
                              .values(vectors, true)
                              .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                              .planNode(),
                          "",
                          {"t1"},
                          core::JoinType::kInner)
                      .planNode())
            .assertResults(
                "SELECT t.c1 FROM tmp as t, tmp AS u WHERE t.c0 == u.c1 AND t.c1 == u.c0");
  });

  tableBuildBlockWait.await([&]() { return tableBuildBlocked.load(); });

  folly::EventCount taskPauseWait;
  std::atomic<bool> taskPaused{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Task::requestPauseLocked",
      std::function<void(Task*)>(([&](Task* /*unused*/) {
        taskPaused = true;
        taskPauseWait.notifyAll();
      })));

  std::thread memThread([&]() {
    std::shared_ptr<core::QueryCtx> fakeCtx =
        newQueryCtx(memoryManager, executor_, kMemoryCapacity);
    auto fakePool = fakeCtx->pool()->addLeafChild("fakePool");
    ASSERT_FALSE(memoryManager->testingGrowPool(
        fakePool.get(), memoryManager->arbitrator()->capacity()));
  });

  taskPauseWait.await([&]() { return taskPaused.load(); });

  unblockTableBuild = true;
  unblockTableBuildWait.notifyAll();

  joinThread.join();
  memThread.join();
  waitForAllTasksToBeDeleted();
}

// This test is to reproduce a race condition that memory arbitrator tries to
// reclaim from a set of hash build operators in which the last hash build
// operator has finished.
DEBUG_ONLY_TEST_F(HashJoinTest, raceBetweenRaclaimAndJoinFinish) {
  std::unique_ptr<memory::MemoryManager> memoryManager = createMemoryManager();
  const auto& arbitrator = memoryManager->arbitrator();
  auto rowType = ROW({
      {"c0", INTEGER()},
      {"c1", INTEGER()},
      {"c2", VARCHAR()},
  });
  // Build a large vector to trigger memory arbitration.
  fuzzerOpts_.vectorSize = 10'000;
  std::vector<RowVectorPtr> vectors = createVectors(2, rowType, fuzzerOpts_);
  createDuckDbTable(vectors);

  std::shared_ptr<core::QueryCtx> joinQueryCtx =
      newQueryCtx(memoryManager, executor_, kMemoryCapacity);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId planNodeId;
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(vectors, false)
                  .project({"c0 AS t0", "c1 AS t1", "c2 AS t2"})
                  .hashJoin(
                      {"t0"},
                      {"u0"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(vectors, true)
                          .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                          .planNode(),
                      "",
                      {"t1"},
                      core::JoinType::kAnti)
                  .capturePlanNodeId(planNodeId)
                  .planNode();

  std::atomic<bool> waitForBuildFinishFlag{true};
  folly::EventCount waitForBuildFinishEvent;
  std::atomic<Driver*> lastBuildDriver{nullptr};
  std::atomic<Task*> task{nullptr};
  std::atomic<bool> isLastBuildFirstChildPool{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashBuild::finishHashBuild",
      std::function<void(exec::HashBuild*)>([&](exec::HashBuild* buildOp) {
        lastBuildDriver = buildOp->testingOperatorCtx()->driver();
        // Checks if the last build memory pool is the first build pool in its
        // parent node pool. It is used to check the test result.
        int buildPoolIndex{0};
        buildOp->pool()->parent()->visitChildren([&](memory::MemoryPool* pool) {
          if (pool == buildOp->pool()) {
            return false;
          }
          if (isHashBuildMemoryPool(*pool)) {
            ++buildPoolIndex;
          }
          return true;
        });
        isLastBuildFirstChildPool = (buildPoolIndex == 0);
        task = lastBuildDriver.load()->task().get();
        waitForBuildFinishFlag = false;
        waitForBuildFinishEvent.notifyAll();
      }));

  std::atomic<bool> waitForReclaimFlag{true};
  folly::EventCount waitForReclaimEvent;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal",
      std::function<void(Driver*)>([&](Driver* driver) {
        auto* op = driver->findOperator(planNodeId);
        if (op->operatorType() != "HashBuild" &&
            op->operatorType() != "HashProbe") {
          return;
        }

        // Suspend hash probe driver to wait for the test triggered reclaim to
        // finish.
        if (op->operatorType() == "HashProbe") {
          op->pool()->reclaimer()->enterArbitration();
          waitForReclaimEvent.await(
              [&]() { return !waitForReclaimFlag.load(); });
          op->pool()->reclaimer()->leaveArbitration();
        }

        // Check if we have reached to the last hash build operator or not. The
        // testvalue callback will set the last build driver.
        if (lastBuildDriver == nullptr) {
          return;
        }

        // Suspend all the remaining hash build drivers until the test triggered
        // reclaim finish.
        op->pool()->reclaimer()->enterArbitration();
        waitForReclaimEvent.await([&]() { return !waitForReclaimFlag.load(); });
        op->pool()->reclaimer()->leaveArbitration();
      }));

  const int numDrivers = 4;
  std::thread queryThread([&]() {
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .maxDrivers(numDrivers)
        .queryCtx(joinQueryCtx)
        .spillDirectory(spillDirectory->path)
        .config(core::QueryConfig::kSpillEnabled, true)
        .config(core::QueryConfig::kJoinSpillEnabled, true)
        .assertResults(
            "SELECT c1 FROM tmp WHERE c0 NOT IN (SELECT c0 FROM tmp)");
  });

  // Wait for the last hash build operator to start building the hash table.
  waitForBuildFinishEvent.await([&] { return !waitForBuildFinishFlag.load(); });
  ASSERT_TRUE(lastBuildDriver != nullptr);
  ASSERT_TRUE(task != nullptr);

  // Wait until the last build driver gets removed from the task after finishes.
  while (task.load()->numFinishedDrivers() != 1) {
    bool foundLastBuildDriver{false};
    task.load()->testingVisitDrivers([&](Driver* driver) {
      if (driver == lastBuildDriver) {
        foundLastBuildDriver = true;
      }
    });
    if (!foundLastBuildDriver) {
      break;
    }
  }

  // Reclaim from the task, and we can't reclaim anything as we don't support
  // spill after hash table built.
  memory::MemoryReclaimer::Stats stats;
  const uint64_t oldCapacity = joinQueryCtx->pool()->capacity();
  task.load()->pool()->shrink();
  task.load()->pool()->reclaim(1'000, 0, stats);
  // If the last build memory pool is first child of its parent memory pool,
  // then memory arbitration (or join node memory pool) will reclaim from the
  // last build operator first which simply quits as the driver has gone. If
  // not, we expect to get numNonReclaimableAttempts from any one of the
  // remaining hash build operator.
  if (isLastBuildFirstChildPool) {
    ASSERT_EQ(stats.numNonReclaimableAttempts, 0);
  } else {
    ASSERT_EQ(stats.numNonReclaimableAttempts, 1);
  }
  // Make sure we don't leak memory capacity since we reclaim from task pool
  // directly.
  static_cast<memory::MemoryPoolImpl*>(task.load()->pool())
      ->testingSetCapacity(oldCapacity);
  waitForReclaimFlag = false;
  waitForReclaimEvent.notifyAll();

  queryThread.join();

  waitForAllTasksToBeDeleted();
  ASSERT_EQ(arbitrator->stats().numFailures, 0);
  ASSERT_EQ(arbitrator->stats().numReclaimedBytes, 0);
  ASSERT_EQ(arbitrator->stats().numReserves, 1);
}

DEBUG_ONLY_TEST_F(HashJoinTest, joinBuildSpillError) {
  const int kMemoryCapacity = 32 << 20;
  // Set a small memory capacity to trigger spill.
  std::unique_ptr<memory::MemoryManager> memoryManager =
      createMemoryManager(kMemoryCapacity, 0);
  const auto& arbitrator = memoryManager->arbitrator();
  auto rowType = ROW(
      {{"c0", INTEGER()},
       {"c1", INTEGER()},
       {"c2", VARCHAR()},
       {"c3", VARCHAR()}});

  std::vector<RowVectorPtr> vectors = createVectors(16, rowType, fuzzerOpts_);
  createDuckDbTable(vectors);

  std::shared_ptr<core::QueryCtx> joinQueryCtx =
      newQueryCtx(memoryManager, executor_, kMemoryCapacity);

  const int numDrivers = 4;
  std::atomic<int> numAppends{0};
  const std::string injectedErrorMsg("injected spillError");
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::SpillState::appendToPartition",
      std::function<void(exec::SpillState*)>([&](exec::SpillState* state) {
        if (++numAppends != numDrivers) {
          return;
        }
        VELOX_FAIL(injectedErrorMsg);
      }));

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(vectors)
                  .project({"c0 AS t0", "c1 AS t1", "c2 AS t2"})
                  .hashJoin(
                      {"t0"},
                      {"u0"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(vectors)
                          .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                          .planNode(),
                      "",
                      {"t1"},
                      core::JoinType::kAnti)
                  .planNode();
  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .queryCtx(joinQueryCtx)
          .spillDirectory(spillDirectory->path)
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kJoinSpillEnabled, true)
          .copyResults(pool()),
      injectedErrorMsg);

  waitForAllTasksToBeDeleted();
  ASSERT_EQ(arbitrator->stats().numFailures, 1);
  ASSERT_EQ(arbitrator->stats().numReserves, 1);
}

DEBUG_ONLY_TEST_F(HashJoinTest, DISABLED_taskWaitTimeout) {
  const int queryMemoryCapacity = 128 << 20;
  // Creates a large number of vectors based on the query capacity to trigger
  // memory arbitration.
  fuzzerOpts_.vectorSize = 10'000;
  auto rowType = ROW(
      {{"c0", INTEGER()},
       {"c1", INTEGER()},
       {"c2", VARCHAR()},
       {"c3", VARCHAR()}});
  const auto vectors =
      createVectors(rowType, queryMemoryCapacity / 2, fuzzerOpts_);
  const int numDrivers = 4;
  const auto expectedResult =
      runHashJoinTask(vectors, nullptr, numDrivers, pool(), false).data;

  for (uint64_t timeoutMs : {0, 1'000, 30'000}) {
    SCOPED_TRACE(fmt::format("timeout {}", succinctMillis(timeoutMs)));
    auto memoryManager = createMemoryManager(512 << 20, 0, 0, timeoutMs);
    auto queryCtx = newQueryCtx(memoryManager, executor_, queryMemoryCapacity);

    // Set test injection to block one hash build operator to inject delay when
    // memory reclaim waits for task to pause.
    folly::EventCount buildBlockWait;
    std::atomic<bool> buildBlockWaitFlag{true};
    std::atomic<bool> blockOneBuild{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::common::memory::MemoryPoolImpl::maybeReserve",
        std::function<void(memory::MemoryPool*)>([&](memory::MemoryPool* pool) {
          const std::string re(".*HashBuild");
          if (!RE2::FullMatch(pool->name(), re)) {
            return;
          }
          if (!blockOneBuild.exchange(false)) {
            return;
          }
          buildBlockWait.await([&]() { return !buildBlockWaitFlag.load(); });
        }));

    folly::EventCount taskPauseWait;
    std::atomic<bool> taskPauseWaitFlag{false};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Task::requestPauseLocked",
        std::function<void(Task*)>(([&](Task* /*unused*/) {
          taskPauseWaitFlag = true;
          taskPauseWait.notifyAll();
        })));

    std::thread queryThread([&]() {
      // We expect failure on short time out.
      if (timeoutMs == 1'000) {
        VELOX_ASSERT_THROW(
            runHashJoinTask(
                vectors, queryCtx, numDrivers, pool(), true, expectedResult),
            "Memory reclaim failed to wait");
      } else {
        // We expect succeed on large time out or no timeout.
        const auto result = runHashJoinTask(
            vectors, queryCtx, numDrivers, pool(), true, expectedResult);
        auto taskStats = exec::toPlanStats(result.task->taskStats());
        auto& planStats = taskStats.at(result.planNodeId);
        ASSERT_GT(planStats.spilledBytes, 0);
      }
    });

    // Wait for task pause to reach, and then delay for a while before unblock
    // the blocked hash build operator.
    taskPauseWait.await([&]() { return taskPauseWaitFlag.load(); });
    // Wait for two seconds and expect the short reclaim wait timeout.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    // Unblock the blocked build operator to let memory reclaim proceed.
    buildBlockWaitFlag = false;
    buildBlockWait.notifyAll();

    queryThread.join();
    waitForAllTasksToBeDeleted();
  }
}
} // namespace
