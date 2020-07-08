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

#ifndef IMPALA_RUNTIME_TMP_FILE_MGR_INTERNAL_H
#define IMPALA_RUNTIME_TMP_FILE_MGR_INTERNAL_H

#include <string>

#include "common/atomic.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/io/file-writer.h"
#include "runtime/tmp-file-mgr.h"

namespace impala {
/// TmpFile is a handle to a physical file in a temporary directory. File space
/// can be allocated and files removed using AllocateSpace() and Remove(). Used
/// internally by TmpFileMgr.
///
/// Creation of the physical file in the file system is deferred until the file is
/// written by DiskIoMgr.
///
/// Methods of TmpFile are not thread-safe.
class TmpFile {
 public:
  TmpFile(TmpFileGroup* file_group, TmpFileMgr::DeviceId device_id,
      const std::string& path, bool expected_local, const char* url = nullptr);

  ~TmpFile();

  /// Allocates 'num_bytes' bytes in this file for a new block of data if there is
  /// free capacity in this temporary directory. If there is insufficient capacity,
  /// return false. Otherwise, update state and return true.
  /// This function does not actually perform any file operations.
  /// On success, sets 'offset' to the file offset of the first byte in the allocated
  /// range on success.
  bool AllocateSpace(int64_t num_bytes, int64_t* offset);

  /// Called when an IO error is encountered for this file. Logs the error and blacklists
  /// the file.
  void Blacklist(const ErrorMsg& msg);

  /// Delete the physical file on disk, if one was created.
  /// It is not valid to read or write to a file after calling Remove().
  Status Remove();

  /// Get the disk ID that should be used for IO mgr queueing.
  int AssignDiskQueue() const;

  /// Try to punch a hole in the file of size 'len' at 'offset'.
  Status PunchHole(int64_t offset, int64_t len);

  void Reset(io::WriteRange* range);

  void SetDumped(bool dumped = true) { dumped_ = dumped; }

  void SetRemote(bool remote = true) { remote_ = remote; }

  void SetInMemory(bool in_mem = true) { in_mem_ = in_mem; }

  const std::string& path() const { return path_; }

  /// Caller must hold TmpFileMgr::FileGroup::lock_.
  bool is_blacklisted() const { return blacklisted_; }

  bool is_done() const { return done_; }

  bool is_dumped() { return dumped_; }

  bool is_remote() { return remote_; }

  bool is_in_memory() { return in_mem_; }

  int64_t const len() { return allocation_offset_; }

  const uint8* buffer() const { return buffer_; }

  std::string DebugString();

 private:
  friend class io::WriteRange;
  friend class TmpFileGroup;
  friend class TmpFileRemote;
  friend class TmpFileMgrTest;

  /// The name of the sub-directory that Impala creates within each configured scratch
  /// directory.
  const static std::string TMP_SUB_DIR_NAME;

  /// Space (in MB) that must ideally be available for writing on a scratch
  /// directory. A warning is issued if available space is less than this threshold.
  const static uint64_t AVAILABLE_SPACE_THRESHOLD_MB;

  /// The TmpFileGroup this belongs to. Cannot be null.
  TmpFileGroup* const file_group_;

  /// Path of the physical file in the filesystem.
  const std::string path_;

  /// The temporary device this file is stored on.
  const TmpFileMgr::DeviceId device_id_;

  /// The id of the disk on which the physical file lies.
  const int disk_id_;

  // If the file is expected to be in the local file system.
  bool expected_local_;

  /// Total bytes of the file that have been given out by AllocateSpace(). Note that
  /// these bytes may not be actually using space on the filesystem, either because the
  /// data hasn't been written or a hole has been punched. Modified by AllocateSpace().
  int64_t allocation_offset_ = 0;

  /// Bytes reclaimed through hole punching.
  AtomicInt64 bytes_reclaimed_{0};

  /// Set to true to indicate that we shouldn't allocate any more space in this file.
  /// Protected by TmpFileMgr::FileGroup::lock_.
  bool blacklisted_;

  // TODO: yidawu set below as status
  /// Done_ will be set to true if the file is ready to be written
  bool done_ = false;

  bool dumped_ = false;

  bool remote_ = false;

  bool in_mem_ = false;

  io::WriteRange* write_range_ = nullptr;

  std::unique_ptr<io::FileWriter> file_writer_;

  const uint8* buffer_;

  hdfsFS hdfs_conn_;

  string hdfs_url_;

  /// Helper to get the TmpDir that this file is associated with.
  TmpFileMgr::TmpDir* GetDir();
};

class TmpFileRemote : public TmpFile {
 public:
  using TmpFile::TmpFile;
  // TODO: yidawu set it by a property
  // default: 16M
  const static int64_t buffer_size_ = 16 * 1024 * 1024;
  // Protect the write buffer
  SpinLock write_buffer_lock_;
  string path_local_;

  // TODO: ydiawu will be replaced by dynamic allocation from pool
  uint8 write_buffer_[buffer_size_];
  bool AllocateSpace(const MemRange& page, int64_t* offset);
  SpinLock* BufferLock() { return &write_buffer_lock_; }
  uint8* const WriteBuffer() { return write_buffer_; }
  // TODO: yidawu should be set in the contruct function
  void SetLocalPath(string path) { path_local_ = path; }
  string LocalPath() { return path_local_; }
  void FetchFromRemote();
};

/*
class TmpFileRemoteConnections {
  public:
    void* GetConnection(string path);
  private:
    map<string, void*> connections_;
}*/

} // namespace impala

#endif
