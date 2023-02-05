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
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/AllocationPool.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/common/memory/MmapArena.h"
#include "velox/common/testutil/TestValue.h"

#include <thread>

#include <folly/Random.h>
#include <folly/Range.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

DECLARE_int32(velox_memory_pool_mb);

using namespace facebook::velox::common::testutil;

namespace facebook::velox::memory {

static constexpr uint64_t kMaxMemoryAllocator = 256UL * 1024 * 1024;
static constexpr MachinePageCount kCapacity =
    (kMaxMemoryAllocator / AllocationTraits::kPageSize);

class MemoryAllocatorTest : public testing::TestWithParam<bool> {
 protected:
  static void SetUpTestCase() {
    TestValue::enable();
  }

  void SetUp() override {
    setupAllocator();
  }

  void setupAllocator() {
    MemoryAllocator::testingDestroyInstance();
    useMmap_ = GetParam();
    if (useMmap_) {
      MmapAllocator::Options options;
      options.capacity = kMaxMemoryAllocator;
      allocator_ = std::make_shared<MmapAllocator>(options);
      MemoryAllocator::setDefaultInstance(allocator_.get());
    } else {
      allocator_ = MemoryAllocator::createDefaultInstance();
      MemoryAllocator::setDefaultInstance(allocator_.get());
    }
    instance_ = MemoryAllocator::getInstance();
    memoryManager_ = std::make_unique<MemoryManager>(IMemoryManager::Options{
        .capacity = kMaxMemory, .allocator = instance_});
    pool_ = memoryManager_->getChild();
    if (useMmap_) {
      ASSERT_EQ(instance_->kind(), MemoryAllocator::Kind::kMmap);
    } else {
      ASSERT_EQ(instance_->kind(), MemoryAllocator::Kind::kMalloc);
    }
    ASSERT_EQ(
        MemoryAllocator::kindString(static_cast<MemoryAllocator::Kind>(100)),
        "UNKNOWN: 100");
  }

  void TearDown() override {
    MemoryAllocator::testingDestroyInstance();
  }

  bool allocate(int32_t numPages, Allocation& result) {
    if (!result.empty()) {
      free(result);
    }
    try {
      if (!instance_->allocateNonContiguous(numPages, result)) {
        EXPECT_TRUE(result.empty());
        return false;
      }
    } catch (const VeloxException& e) {
      EXPECT_TRUE(result.empty());
      return false;
    }
    ++allocs_;
    EXPECT_GE(result.numPages(), numPages);
    initializeContents(result);
    return true;
  }

  void initializeContents(Allocation& alloc) {
    auto sequence = sequence_.fetch_add(1);
    bool first = true;
    for (int32_t i = 0; i < alloc.numRuns(); ++i) {
      Allocation::PageRun run = alloc.runAt(i);
      void** ptr = reinterpret_cast<void**>(run.data());
      int32_t numWords =
          run.numPages() * AllocationTraits::kPageSize / sizeof(void*);
      for (int32_t offset = 0; offset < numWords; offset++) {
        if (first) {
          ptr[offset] = reinterpret_cast<void*>(sequence);
          first = false;
        } else {
          ptr[offset] = ptr + offset + sequence;
        }
      }
    }
  }

  void checkContents(Allocation& alloc) {
    bool first = true;
    long sequence;
    for (int32_t i = 0; i < alloc.numRuns(); ++i) {
      Allocation::PageRun run = alloc.runAt(i);
      void** ptr = reinterpret_cast<void**>(run.data());
      int32_t numWords =
          run.numPages() * AllocationTraits::kPageSize / sizeof(void*);
      for (int32_t offset = 0; offset < numWords; offset++) {
        if (first) {
          sequence = reinterpret_cast<long>(ptr[offset]);
          first = false;
        } else {
          ASSERT_EQ(ptr[offset], ptr + offset + sequence);
        }
      }
    }
  }

  void initializeContents(ContiguousAllocation& alloc) {
    long sequence = sequence_.fetch_add(1);
    bool first = true;
    void** ptr = reinterpret_cast<void**>(alloc.data());
    int numWords = alloc.size() / sizeof(void*);
    for (int offset = 0; offset < numWords; offset++) {
      if (first) {
        ptr[offset] = reinterpret_cast<void*>(sequence);
        first = false;
      } else {
        ptr[offset] = ptr + offset + sequence;
      }
    }
  }

  void checkContents(ContiguousAllocation& alloc) {
    bool first = true;
    long sequence;
    void** ptr = reinterpret_cast<void**>(alloc.data());
    int numWords = alloc.size() / sizeof(void*);
    for (int offset = 0; offset < numWords; offset++) {
      if (first) {
        sequence = reinterpret_cast<long>(ptr[offset]);
        first = false;
      } else {
        ASSERT_EQ(ptr[offset], ptr + offset + sequence);
      }
    }
  }

  void free(Allocation& alloc) {
    assert(!alloc.empty());
    ++frees_;
    checkContents(alloc);
    instance_->freeNonContiguous(alloc);
  }

  void clearAllocations(std::vector<std::unique_ptr<Allocation>>& allocations) {
    for (auto& allocation : allocations) {
      instance_->freeNonContiguous(*allocation);
    }
    allocations.clear();
  }

  void clearAllocations(std::vector<std::vector<std::unique_ptr<Allocation>>>&
                            allocationsVector) {
    for (auto& allocations : allocationsVector) {
      for (auto& allocation : allocations) {
        if (!allocation->empty()) {
          free(*allocation);
        }
      }
    }
    allocationsVector.clear();
  }

  void shrinkAllocations(
      std::vector<std::unique_ptr<Allocation>>& allocations,
      int32_t reducedSize) {
    while (allocations.size() > reducedSize) {
      instance_->freeNonContiguous(*allocations.back());
      allocations.pop_back();
    }
    ASSERT_EQ(allocations.size(), reducedSize);
  }

  void clearContiguousAllocations(
      std::vector<ContiguousAllocation>& allocations) {
    for (auto& allocation : allocations) {
      instance_->freeContiguous(allocation);
    }
    allocations.clear();
  }

  void allocateMultiple(
      MachinePageCount numPages,
      int32_t numAllocs,
      std::vector<std::unique_ptr<Allocation>>& allocations) {
    clearAllocations(allocations);
    allocations.reserve(numAllocs);
    bool largeTested = false;
    for (int32_t i = 0; i < numAllocs; ++i) {
      auto allocation = std::make_unique<Allocation>();
      if (!allocate(numPages, *allocation)) {
        continue;
      }
      allocations.push_back(std::move(allocation));
      int available = kCapacity - instance_->numAllocated();

      // Try large allocations after half the capacity is used.
      if (available <= kCapacity / 2 && !largeTested) {
        largeTested = true;
        ContiguousAllocation large;
        if (!allocateContiguous(available / 2, nullptr, large)) {
          FAIL() << "Could not allocate half the available";
          return;
        }
        Allocation small;
        if (!instance_->allocateNonContiguous(available / 4, small)) {
          FAIL() << "Could not allocate 1/4 of available";
          return;
        }
        // Try to allocate more than available, and it should fail if we use
        // MmapAllocator which enforces the capacity check.
        if (useMmap_) {
          ASSERT_FALSE(
              instance_->allocateContiguous(available + 1, &small, large));
          ASSERT_TRUE(small.empty());
          ASSERT_TRUE(large.empty());
        } else {
          ASSERT_TRUE(
              instance_->allocateContiguous(available + 1, &small, large));
          ASSERT_TRUE(small.empty());
          ASSERT_FALSE(large.empty());
          instance_->freeContiguous(large);
        }

        // Check the failed allocation freed the collateral.
        ASSERT_EQ(small.numPages(), 0);
        ASSERT_EQ(large.numPages(), 0);
        if (!allocateContiguous(available, nullptr, large)) {
          FAIL() << "Could not allocate rest of capacity";
        }
        ASSERT_GE(large.numPages(), available);
        ASSERT_EQ(small.numPages(), 0);
        ASSERT_EQ(kCapacity, instance_->numAllocated());
        if (useMmap_) {
          // The allocator has everything allocated and half mapped, with the
          // other half mapped by the contiguous allocation. numMapped()
          // includes the contiguous allocation.
          ASSERT_EQ(kCapacity, instance_->numMapped());
        }
        if (!allocateContiguous(available / 2, nullptr, large)) {
          FAIL() << "Could not exchange all of available for half of available";
        }
        ASSERT_GE(large.numPages(), available / 2);
        instance_->freeContiguous(large);
      }
    }
  }

  bool allocateContiguous(
      int numPages,
      Allocation* FOLLY_NULLABLE collateral,
      ContiguousAllocation& allocation) {
    bool success =
        instance_->allocateContiguous(numPages, collateral, allocation);
    if (success) {
      initializeContents(allocation);
    }
    return success;
  }

  void free(ContiguousAllocation& allocation) {
    checkContents(allocation);
    instance_->freeContiguous(allocation);
  }

  void allocateIncreasing(
      MachinePageCount startSize,
      MachinePageCount endSize,
      int32_t repeat,
      std::vector<std::unique_ptr<Allocation>>& allocations) {
    int32_t hand = 0;
    for (int32_t count = 0; count < repeat;) {
      for (auto size = startSize; size < endSize;
           size += std::max<MachinePageCount>(1, size / 5)) {
        ++count;
        if (!allocate(size, *allocations[hand])) {
          if (!makeSpace(size, allocations, hand)) {
            // Stop early if other threads have consumed all capacity
            // and there is not enough here to free in to satisfy the
            // allocation.
            return;
          }
        }
        hand = (hand + 1) % allocations.size();
      }
    }
  }

  bool makeSpace(
      int32_t size,
      std::vector<std::unique_ptr<Allocation>>& allocations,
      int32_t& hand) {
    int numIterations = 0;
    while (kCapacity - instance_->numAllocated() < size) {
      if (allocations[hand]->numRuns()) {
        if (!allocations[hand]->empty()) {
          free(*allocations[hand].get());
        }
      }
      hand = (hand + 1) % allocations.size();
      if (++numIterations > allocations.size()) {
        // Looked at all of 'allocations' and could not free enough.
        return false;
      }
    }
    return true;
  }

  std::vector<std::unique_ptr<Allocation>> makeEmptyAllocations(int32_t size) {
    std::vector<std::unique_ptr<Allocation>> allocations;
    allocations.reserve(size);
    for (int32_t i = 0; i < size; i++) {
      allocations.push_back(std::make_unique<Allocation>());
    }
    return allocations;
  }

  bool useMmap_;
  std::shared_ptr<MemoryAllocator> allocator_;
  MemoryAllocator* instance_;
  std::unique_ptr<MemoryManager> memoryManager_;
  std::shared_ptr<MemoryPool> pool_;
  std::atomic<int32_t> sequence_ = {};
  std::atomic<int64_t> allocs_{0};
  std::atomic<int64_t> frees_{0};
};

TEST_P(MemoryAllocatorTest, increasingSizeWithThreadsTest) {
  LOG(INFO) << "start " << instance_->numAllocated();
  const int32_t numThreads = 20;
  std::vector<std::vector<std::unique_ptr<Allocation>>> allocations;
  allocations.reserve(numThreads);
  std::vector<std::thread> threads;
  threads.reserve(numThreads);
  for (int32_t i = 0; i < numThreads; ++i) {
    allocations.emplace_back(makeEmptyAllocations(500));
  }
  for (int32_t i = 0; i < numThreads; ++i) {
    threads.push_back(std::thread([this, &allocations, i]() {
      allocateIncreasing(10, 1000, 1000, allocations[i]);
    }));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_TRUE(instance_->checkConsistency());
  EXPECT_GT(instance_->numAllocated(), 0);

  clearAllocations(allocations);
  EXPECT_TRUE(instance_->checkConsistency());
  EXPECT_EQ(instance_->numAllocated(), 0);
  if (instance_->numAllocated() != 0) {
    LOG(INFO) << "bad allocs " << allocs_ << " frees " << frees_ << " " << instance_->numAllocated();
    instance_->toString();
  }
}
} // namespace facebook::velox::memory
