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

#include "velox/common/file/tests/utils/FaultyFile.h"

namespace facebook::velox::tests::utils {

FaultyReadFile::FaultyReadFile(
    std::shared_ptr<ReadFile> delegatedFile,
    FileFaultInjectionHook injectionHook)
    : delegatedFile_(std::move(delegatedFile)),
      injectionHook_(std::move(injectionHook)) {
  VELOX_CHECK_NOT_NULL(delegatedFile_);
}

std::string_view
FaultyReadFile::pread(uint64_t offset, uint64_t length, void* buf) const {
  if (injectionHook_ != nullptr) {
    FileReadFaultInjection injection(offset, length, buf);
    injectionHook_(&injection);
    if (!injection.delegate) {
      return injection.injectedReadBuf;
    }
  }
  return delegatedFile_->pread(offset, length, buf);
}

uint64_t FaultyReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers) const {
  if (injectionHook_ != nullptr) {
    FileReadvFaultInjection injection(offset, buffers);
    injectionHook_(&injection);
    if (!injection.delegate) {
      return injection.readBytes;
    }
  }
  return delegatedFile_->preadv(offset, buffers);
}

FaultyWriteFile::FaultyWriteFile(
    std::shared_ptr<WriteFile> delegatedFile,
    FileFaultInjectionHook injectionHook)
    : delegatedFile_(std::move(delegatedFile)),
      injectionHook_(std::move(injectionHook)) {
  VELOX_CHECK_NOT_NULL(delegatedFile_);
}
void FaultyWriteFile::append(std::string_view data) {
  delegatedFile_->append(data);
}

void FaultyWriteFile::append(std::unique_ptr<folly::IOBuf> data) {
  delegatedFile_->append(std::move(data));
}

void FaultyWriteFile::flush() {
  delegatedFile_->flush();
}

void FaultyWriteFile::close() {
  delegatedFile_->close();
}
} // namespace facebook::velox::exec::test
