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

#include <unistd.h>
#include <cstdlib>
#include <memory>
#include <string>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::exec::test {

/// Manages the lifetime of a temporary directory.
class TempDirectoryPath {
 public:
  static std::shared_ptr<TempDirectoryPath> create(
      bool enableFaultInjection = false);

  virtual ~TempDirectoryPath();

  TempDirectoryPath(const TempDirectoryPath&) = delete;
  TempDirectoryPath& operator=(const TempDirectoryPath&) = delete;

  std::string path() const {
    return enableFaultInjection_ ? fmt::format("faulty:{}", path_) : path_;
  }

 private:
  static std::string createTempDirectory();

  explicit TempDirectoryPath(bool enableFaultInjection)
      : path_(createTempDirectory()),
        enableFaultInjection_(enableFaultInjection) {}

  const std::string path_;

  bool enableFaultInjection_{false};
};
} // namespace facebook::velox::exec::test
