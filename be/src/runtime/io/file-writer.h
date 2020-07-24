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

#include <string>

#include "common/status.h"

namespace impala {
class TmpFile;
namespace io {
class DiskIoMgr;
class WriteRange;

/// Abstract class that provides interface for file operations
/// Child classes implement these operations for the local file system
/// and for HDFS.
/// A FileReader object is owned by a single ScanRange object, and
/// a ScanRange object only has a single FileReader object.
class FileWriter {
 public:
  FileWriter(DiskIoMgr* io_mgr, TmpFile* tmp_file, const int64_t file_size)
    : io_mgr_(io_mgr), tmp_file_(tmp_file), file_size_(file_size) {}
  virtual ~FileWriter() {}

  /// Opens file that is associated with 'scan_range_'.
  virtual Status Open() = 0;

  virtual Status Write(WriteRange*, bool*) = 0;

  /// Closes the file associated with 'scan_range_'. It doesn't have effect on other
  /// scan ranges.
  virtual Status Close() = 0;

  // The caller holds the lock to guarantee thread-safe
  bool UpdateWrittenSize(int64_t val) {
    written_bytes_ += val;
    DCHECK(written_bytes_ <= file_size_);
    return written_bytes_ == file_size_;
  }

 protected:
  DiskIoMgr* io_mgr_ = nullptr;
  TmpFile* tmp_file_ = nullptr;
  std::mutex lock_;
  int64_t written_bytes_ = 0;
  const int64_t file_size_;
};
}
}
