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

#include "velox/common/file/File.h"

namespace facebook::velox::tests::utils {

    /// Defines the per-file operation fault injection.
struct FileFaultInjectionBase {
  enum class Type {
    /// Injects fault for file read operations.
    kRead,
    kReadv,
    /// TODO: add to support fault injections for the other operator types.
  };

  const Type type;

  /// Indicates to forward this operation to the delegate file or not. If not,
  /// then the file fault injection hook must have processed the request. For
  /// instance, if this is a file read injection, then the hook must have filled
  /// the fake data for read.
  bool delegate{true};

  explicit FileFaultInjectionBase(Type _type) : type(_type) {}
};

/// Defines fault injection parameters for file read.
struct FileReadFaultInjection : FileFaultInjectionBase {
  const uint64_t offset;
  const uint64_t length;
  void* const buf;
  std::string_view injectedReadBuf;

  FileReadFaultInjection(uint64_t _offset, uint64_t _length, void* _buf)
      : FileFaultInjectionBase(FileFaultInjectionBase::Type::kRead),
        offset(_offset),
        length(_length),
        buf(_buf) {}
};

/// Defines fault injection parameters for file readv.
struct FileReadvFaultInjection : FileFaultInjectionBase {
  const uint64_t offset;
  const std::vector<folly::Range<char*>>& buffers;
  uint64_t readBytes{0};

  FileReadvFaultInjection(
      uint64_t _offset,
      const std::vector<folly::Range<char*>>& _buffers)
      : FileFaultInjectionBase(FileFaultInjectionBase::Type::kReadv),
        offset(_offset),
        buffers(_buffers) {}
};

using FileFaultInjectionHook = std::function<void(FileFaultInjectionBase*)>;

class FaultyReadFile : public ReadFile {
 public:
  FaultyReadFile(
      std::shared_ptr<ReadFile> delegatedFile,
      FileFaultInjectionHook injectionHook);

  ~FaultyReadFile() override{};

  uint64_t size() const override {
    return delegatedFile_->size();
  }

  std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const override;

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const override;

  uint64_t memoryUsage() const override {
    return delegatedFile_->memoryUsage();
  }

  bool shouldCoalesce() const override {
    return delegatedFile_->shouldCoalesce();
  }

  std::string getName() const override {
    return delegatedFile_->getName();
  }

  uint64_t getNaturalReadSize() const override {
    return delegatedFile_->getNaturalReadSize();
  }

 private:
  const std::shared_ptr<ReadFile> delegatedFile_;
  const FileFaultInjectionHook injectionHook_;
};

class FaultyWriteFile : public WriteFile {
 public:
  FaultyWriteFile(
      std::shared_ptr<WriteFile> delegatedFile,
      FileFaultInjectionHook injectionHook);

  ~FaultyWriteFile() override{};

  void append(std::string_view data) override;

  void append(std::unique_ptr<folly::IOBuf> data) override;

  void flush() override;

  void close() override;

  uint64_t size() const final {
    return delegatedFile_->size();
  }

 private:
  const std::shared_ptr<WriteFile> delegatedFile_;
  const FileFaultInjectionHook injectionHook_;
};

} // namespace facebook::velox::exec::test
