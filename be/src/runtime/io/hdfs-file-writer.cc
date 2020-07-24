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

#include <stdio.h>
#include <algorithm>

#include "runtime/hdfs-fs-cache.h"
#include "runtime/io/disk-io-mgr-internal.h"
#include "runtime/io/hdfs-file-writer.h"
#include "runtime/io/request-ranges.h"
#include "runtime/tmp-file-mgr-internal.h"
#include "util/histogram-metric.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"

#include "common/names.h"

namespace impala {
namespace io {

HdfsFileWriter::~HdfsFileWriter() {
  DCHECK(exclusive_hdfs_fh_ == nullptr) << "File was not closed.";
  if (expected_local_) {
    // TODO: yidawu delete, just for compile, not used
  }
}

Status HdfsFileWriter::Open() {
  //  RETURN_IF_ERROR(write_range_->cancel_status_);
  // TODO: yidawu add Metric
  DCHECK(hdfs_conn_ != nullptr);
  if (hdfs_file_ == nullptr) {
    std::lock_guard<mutex> l(lock_);
    if (hdfs_file_ != nullptr) return Status::OK();
    hdfs_file_ =
        hdfsOpenFile(hdfs_conn_, tmp_file_->path().c_str(), O_WRONLY, 0, 0, block_size_);
    ImpaladMetrics::IO_MGR_NUM_OPEN_FILES->Increment(1L);
  }
  return Status::OK();
}

Status HdfsFileWriter::Write(WriteRange* range, bool* is_ready) {
  DCHECK(hdfs_file_ != nullptr);
  std::lock_guard<mutex> l(lock_);
  // hdfsFile hdfs_file = exclusive_hdfs_fh_->file();
  int ret = hdfsWrite(hdfs_conn_, hdfs_file_, range->data(), range->len());
  LOG(INFO) << range->file() << " written";
  if (ret == -1) {
    stringstream msg;
    msg << "Failed to write data (length: " << range->len() << ") to Hdfs file: ";
    return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
        GetHdfsErrorMsg(msg.str(), range->file()));
  } else {
    range->ResetOffset(written_bytes_);
    if (UpdateWrittenSize(range->len())) {
      *is_ready = true;
    }
  }
  return Status::OK();
}
Status HdfsFileWriter::Close() {
  DCHECK(hdfs_conn_ != nullptr);
  DCHECK(hdfs_file_ != nullptr);
  int ret = hdfsCloseFile(hdfs_conn_, hdfs_file_);
  if (ret != 0) {
    return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
        GetHdfsErrorMsg("Failed to close HDFS file: ", tmp_file_->path()));
  }
  ImpaladMetrics::IO_MGR_NUM_OPEN_FILES->Increment(-1L);
  return Status::OK();
}
}
}
