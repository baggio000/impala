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

#include "common/hdfs.h"
#include "runtime/io/file-writer.h"

namespace impala {
namespace io {

/// File writer class for HDFS.
class HdfsFileWriter : public FileWriter {
 public:
  HdfsFileWriter(DiskIoMgr* io_mgr, TmpFile* tmp_file, hdfsFS hdfs_conn,
      int64_t file_size, bool expected_local)
    : FileWriter(io_mgr, tmp_file, file_size),
      hdfs_conn_(hdfs_conn),
      expected_local_(expected_local),
      block_size_(io_mgr->GetRemoteTmpBlockSize()) {}

  ~HdfsFileWriter();

  virtual Status Open() override;
  virtual Status Write(WriteRange* range = nullptr, bool* is_ready = nullptr) override;
  virtual Status Close() override;

 private:
  hdfsFS const hdfs_conn_;

  hdfsFile hdfs_file_ = nullptr;

  const int64_t block_size_;

  /// The hdfs file handle is stored here in three cases:
  /// 1. The file handle cache is off (max_cached_file_handles == 0).
  /// 2. The scan range is using hdfs caching.
  /// -OR-
  /// 3. The hdfs file is expected to be remote (expected_local_ == false)
  /// In each case, the scan range gets a new ExclusiveHdfsFileHandle at Open(),
  /// owns it exclusively, and destroys it in Close().
  std::unique_ptr<ExclusiveHdfsFileHandle> exclusive_hdfs_fh_;

  /// If true, we expect the reads to be a local read. Note that if this is false,
  /// it does not necessarily mean we expect the read to be remote, and that we never
  /// create scan ranges where some of the range is expected to be remote and some of it
  /// local.
  /// TODO: we can do more with this
  const bool expected_local_;

  /// Total number of bytes read remotely. This is necessary to maintain a count of
  /// the number of remote scan ranges. Since IO statistics can be collected multiple
  /// times for a scan range, it is necessary to keep some state about whether this
  /// scan range has already been counted as remote. There is also a requirement to
  /// log the number of unexpected remote bytes for a scan range. To solve both
  /// requirements, maintain num_remote_bytes_ on the ScanRange and push it to the
  /// reader_ once at the close of the scan range.
  // int64_t num_remote_bytes_ = 0;
};
}
}
