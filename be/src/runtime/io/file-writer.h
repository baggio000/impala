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
namespace io {

class WriteRange;

/// Abstract class that provides interface for file operations
/// Child classes implement these operations for the local file system
/// and for HDFS.
/// A FileReader object is owned by a single ScanRange object, and
/// a ScanRange object only has a single FileReader object.
class FileWriter {
 public:
  FileWriter() {}
  virtual ~FileWriter() {}

  /// Opens file that is associated with 'scan_range_'.
  virtual Status Open() = 0;

  virtual Status Write() = 0;

  /// Closes the file associated with 'scan_range_'. It doesn't have effect on other
  /// scan ranges.
  virtual Status Close() = 0;

  // TODO: yidawu comment
  virtual void Reset(WriteRange* range) { write_range_ = range; }

 protected:
  WriteRange* write_range_ = nullptr;
};
}
}
