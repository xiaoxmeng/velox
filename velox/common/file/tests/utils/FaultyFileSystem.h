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
#pragma once

#include "velox/common/file/FileSystems.h"

#include <functional>
#include <memory>
#include <string_view>
#include "velox/common/file/tests/utils/FaultyFile.h"

namespace facebook::velox::tests::utils {

using namespace filesystems;

/// Implements fault filesystem which is a wrapper on top of a real file system.
/// It is used by unit test for io fault injection. By default, it delegates the
/// the file operation to the file system underneath.
class FaultyFileSystem : public FileSystem {
 public:
  explicit FaultyFileSystem(std::shared_ptr<const Config> config)
      : FileSystem(std::move(config)) {}

  ~FaultyFileSystem() override {}

  static inline std::string scheme() {
    return "faulty:";
  }

  std::string name() const override {
    return "Faulty FS";
  }

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view path,
      const FileOptions& options) override;

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view path,
      const FileOptions& options) override;

  void remove(std::string_view path) override;

  void rename(
      std::string_view oldPath,
      std::string_view newPath,
      bool overwrite) override;

  bool exists(std::string_view path) override;

  std::vector<std::string> list(std::string_view path) override;

  void mkdir(std::string_view path) override;

  void rmdir(std::string_view path) override;

  /// Setups hook for file fault injection.
  void setFileFaultInjection(FileFaultInjectionHook injectionHook);

  /// Clear hooks for file fault injections.
  void clearFileFaultInjection();

 private:
  mutable std::mutex mu_;
  FileFaultInjectionHook fileInjectionHook_;
};

/// Registers the faulty filesystem.
void registerFaultyFileSystem();

/// Gets the fault filesystem instance.
std::shared_ptr<FaultyFileSystem> faultyFileSystem();
} // namespace facebook::velox::tests::utils
