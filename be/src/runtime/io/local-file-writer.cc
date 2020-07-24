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

#include "runtime/io/disk-io-mgr-internal.h"
#include "runtime/io/local-file-writer.h"
#include "runtime/io/request-ranges.h"
#include "util/histogram-metric.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"

#include "common/names.h"

namespace impala {
namespace io {

Status LocalFileWriter::Open() {
  if (file_ == nullptr) {
    {
      lock_guard<mutex> lock(lock_);
      if (file_ != nullptr) return Status::OK();
      return io_mgr_->local_file_system_->OpenForWrite(
          file_path_, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR, &file_);
    }
  }
  return Status::OK();
}

Status LocalFileWriter::Write(WriteRange* range, bool* is_ready) {
  lock_guard<mutex> lock(lock_);
  Status status;
  if (mode_ == LocalFileMode::BUFFER) {
    status = io_mgr_->local_file_system_->Fwrite(file_, range);
    // TODO: yidawu seems worse than  fwrite
    /*
    int written = write(fileno(file_), range->data(), range->len());
    DCHECK(written == range->len());
    status = Status::OK();*/
  } else {
    status = io_mgr_->WriteRangeHelper(file_, range);
  }
  if (status.ok()) {
    ImpaladMetrics::IO_MGR_BYTES_WRITTEN->Increment(range->len());
    if (mode_ == LocalFileMode::BUFFER) {
      range->ResetOffset(written_bytes_);
      if (UpdateWrittenSize(range->len())) {
        *is_ready = true;
      }
      LOG(WARNING) << "file written: " << range->tmp_file_->LocalBuffPath()
                   << " offset:" << range->offset_ << " len:" << range->len()
                   << " written_bytes:" << written_bytes_;
    } else {
      *is_ready = true;
    }
  }

  // TODO: yidawu error handle

  return status;
}

Status LocalFileWriter::Close() {
  // It is supposed only one thread and it is the last thread to  be able
  // to close the file. So no need for locking.
  Status status = Status::OK();
  if (file_ == nullptr) {
    lock_guard<mutex> lock(lock_);
    if (file_ == nullptr) return status;
    status = io_mgr_->local_file_system_->Fclose(file_, file_path_);
    file_ = nullptr;
  }
  return status;
}
}
}
