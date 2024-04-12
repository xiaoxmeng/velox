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

#include <sys/stat.h>
#include <unistd.h>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::exec::test {

/// Manages the lifetime of a temporary file.
class TempFilePath {
 public:
  static std::shared_ptr<TempFilePath> create(
      bool faultInjectionEnable = false);

  virtual ~TempFilePath() {
    ::unlink(path_.c_str());
    ::close(fd_);
  }

  TempFilePath(const TempFilePath&) = delete;
  TempFilePath& operator=(const TempFilePath&) = delete;

  void append(std::string data) {
    std::ofstream file(path_, std::ios_base::app);
    file << data;
    file.flush();
    file.close();
  }

  const int64_t fileSize() {
    struct stat st;
    stat(path_.data(), &st);
    return st.st_size;
  }

  const int64_t fileModifiedTime() {
    struct stat st;
    ::stat(path_.data(), &st);
    return st.st_mtime;
  }

 private:
  static std::string createTempFile(TempFilePath* tempFilePath);

  TempFilePath() : path_(createTempFile(this)) {
    VELOX_CHECK_NE(fd_, -1);
  }

  const std::string path_;
  int fd_;
};

} // namespace facebook::velox::exec::test
