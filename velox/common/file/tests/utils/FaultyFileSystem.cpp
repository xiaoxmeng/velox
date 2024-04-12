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

#include "velox/common/file/tests/utils/FaultyFileSystem.h"
#include <folly/synchronization/CallOnce.h>

#include <filesystem>

namespace facebook::velox::tests::utils {
namespace {
inline std::string extractPath(std::string_view path) {
  VELOX_CHECK_EQ(path.find(FaultyFileSystem::scheme()), 0);
  return std::string(path.substr(FaultyFileSystem::scheme().length()));
}

std::function<bool(std::string_view)> schemeMatcher() {
  // Note: presto behavior is to prefix local paths with 'file:'.
  // Check for that prefix and prune to absolute regular paths as needed.
  return [](std::string_view filePath) {
    return filePath.find(FaultyFileSystem::scheme()) == 0;
  };
}

folly::once_flag faultFilesystemInitOnceFlag;

std::function<std::shared_ptr<
    FileSystem>(std::shared_ptr<const Config>, std::string_view)>
fileSystemGenerator() {
  return
      [](std::shared_ptr<const Config> properties, std::string_view filePath) {
        // One instance of faulty FileSystem is sufficient. Initializes on first
        // access and reuse after that.
        static std::shared_ptr<FileSystem> lfs;
        folly::call_once(faultFilesystemInitOnceFlag, [&properties]() {
          lfs = std::make_shared<FaultyFileSystem>(properties);
        });
        return lfs;
      };
}
} // namespace

std::unique_ptr<ReadFile> FaultyFileSystem::openFileForRead(
    std::string_view path,
    const FileOptions& options) {
  const std::string delegatedPath = extractPath(path);
  auto delegatedFile = getFileSystem(delegatedPath, config_)
                           ->openFileForRead(delegatedPath, options);
  return std::make_unique<FaultyReadFile>(
      std::move(delegatedFile), fileInjectionHook_);
}

std::unique_ptr<WriteFile> FaultyFileSystem::openFileForWrite(
    std::string_view path,
    const FileOptions& options) {
  const std::string delegatedPath = extractPath(path);
  auto delegatedFile = getFileSystem(delegatedPath, config_)
                           ->openFileForWrite(delegatedPath, options);
  return std::make_unique<FaultyWriteFile>(
      std::move(delegatedFile), fileInjectionHook_);
}

void FaultyFileSystem::remove(std::string_view path) {
  const std::string delegatedPath = extractPath(path);
  getFileSystem(delegatedPath, config_)->remove(path);
}

void FaultyFileSystem::rename(
    std::string_view oldPath,
    std::string_view newPath,
    bool overwrite) {
  const auto delegatedOldPath = extractPath(oldPath);
  const auto delegatedNewPath = extractPath(newPath);
  getFileSystem(delegatedOldPath, config_)
      ->rename(delegatedOldPath, delegatedNewPath);
}

bool FaultyFileSystem::exists(std::string_view path) {
  const auto delegatedPath = extractPath(path);
  return getFileSystem(delegatedPath, config_)->exists(delegatedPath);
}

std::vector<std::string> FaultyFileSystem::list(std::string_view path) {
  const auto delegatedDirPath = extractPath(path);
  return getFileSystem(delegatedDirPath, config_)->list(delegatedDirPath);
}

void FaultyFileSystem::mkdir(std::string_view path) {
  const auto delegatedDirPath = extractPath(path);
  getFileSystem(delegatedDirPath, config_)->mkdir(delegatedDirPath);
}

void FaultyFileSystem::rmdir(std::string_view path) {
  const auto delegatedDirPath = extractPath(path);
  getFileSystem(delegatedDirPath, config_)->rmdir(delegatedDirPath);
}

void FaultyFileSystem::setFileFaultInjection(
    FileFaultInjectionHook injectionHook) {
  std::lock_guard<std::mutex> l(mu_);
  fileInjectionHook_ = std::move(injectionHook);
}

void FaultyFileSystem::clearFileFaultInjection() {
  std::lock_guard<std::mutex> l(mu_);
  fileInjectionHook_ = nullptr;
}

void registerFaultyFileSystem() {
  registerFileSystem(schemeMatcher(), fileSystemGenerator());
}

std::shared_ptr<FaultyFileSystem> faultyFileSystem() {
  return std::dynamic_pointer_cast<FaultyFileSystem>(
      getFileSystem(FaultyFileSystem::scheme(), {}));
}
} // namespace facebook::velox::exec::test
