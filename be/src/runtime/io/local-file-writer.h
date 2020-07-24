// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "runtime/io/file-writer.h"
#include "runtime/tmp-file-mgr-internal.h"

namespace impala {
namespace io {

/// File reader class for the local file system.
/// It uses the standard C APIs from stdio.h
class LocalFileWriter : public FileWriter {
 public:
  LocalFileWriter(
      DiskIoMgr* io_mgr, TmpFile* tmp_file, impala::LocalFileMode mode, int64_t file_size)
    : FileWriter(io_mgr, tmp_file, file_size), mode_(mode) {
    bool is_buff_mode = mode_ == impala::LocalFileMode::BUFFER;
    file_path_ =
        is_buff_mode ? tmp_file_->LocalBuffPath().c_str() : tmp_file_->path().c_str();
  }
  ~LocalFileWriter() {}

  virtual Status Open() override;
  virtual Status Write(WriteRange* range, bool* is_ready) override;
  virtual Status Close() override;

 private:
  /// Points to a C FILE object between calls to Open() and Close(), otherwise nullptr.
  FILE* file_ = nullptr;
  // TODO: yidawu do with the mode
  impala::LocalFileMode mode_;
  const char* file_path_;
};
}
}
