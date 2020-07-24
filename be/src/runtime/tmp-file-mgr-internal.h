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

#include <boost/thread/shared_mutex.hpp>
#include "common/atomic.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/io/file-writer.h"
#include "runtime/tmp-file-mgr.h"

namespace impala {
namespace io {
class LocalFileWriter;
}

enum class LocalFileMode { BUFFER, FILE };

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
      const std::string& path, bool expected_local, const char* url = nullptr,
      const std::string* local_buffer_path = nullptr,
      TmpFileMgr::DeviceId* local_device_id = nullptr);

  ~TmpFile();

  /// Allocates 'num_bytes' bytes in this file for a new block of data if there is
  /// free capacity in this temporary directory. If there is insufficient capacity,
  /// return false. Otherwise, update state and return true.
  /// This function does not actually perform any file operations.
  /// On success, sets 'offset' to the file offset of the first byte in the allocated
  /// range on success.
  virtual bool AllocateSpace(int64_t num_bytes, int64_t* offset) = 0;

  /// Called when an IO error is encountered for this file. Logs the error and blacklists
  /// the file.
  void Blacklist(const ErrorMsg& msg);

  /// Delete the physical file on disk, if one was created.
  /// It is not valid to read or write to a file after calling Remove().
  virtual Status Remove() = 0;

  /// Caller should hold the lock.
  virtual Status RemoveLocalBuff() = 0;

  /// Get the disk ID that should be used for IO mgr queueing.
  int AssignDiskQueue() const;

  /// Try to punch a hole in the file of size 'len' at 'offset'.
  Status PunchHole(int64_t offset, int64_t len);

  void ResetLocalBuffPath(const string& path, TmpFileMgr::DeviceId device_id);
  const string& LocalBuffPath() { return local_buffer_path_; }

  // If the local file is in buffer mode, writer will wait for the whole file finished,
  // and
  // write once. If it is file mode, after each write range will close the file handle.
  void SetDumped(bool dumped = true) { dumped_ = dumped; }

  void SetInDumping(bool is_dumping = true) { in_dumping_ = is_dumping; }

  void SetInWriting(bool in_writing = true) { in_writing_ = in_writing; }

  void SetRemote(bool remote = true) { remote_ = remote; }

  // Should call with lock;
  void UpdateUnpinCnt() { unpinned_page_cnt_.Add(1); }
  int64_t GetUnpinCnt() { return unpinned_page_cnt_.Load(); }

  // Should call with lock;
  void UpdatePinCnt() { pinned_page_cnt_.Add(1); }
  int64_t GetPinCnt() { return pinned_page_cnt_.Load(); }

  bool AllPinned();

  virtual io::FileWriter* GetFileWriter(bool write_to_buff = false) = 0;

  const std::string& path() const { return path_; }

  int64_t file_size() const { return file_size_; }

  LocalFileMode mode() const { return mode_; }

  /// Caller must hold TmpFileMgr::FileGroup::lock_.
  bool is_blacklisted() const { return blacklisted_; }

  bool is_done() const { return done_; }

  bool is_dumped() { return dumped_; }

  bool is_dumping() { return in_dumping_; }

  bool is_writing() { return in_writing_; }

  bool is_remote() { return remote_; }

  int64_t const len() { return allocation_offset_; }

  std::string DebugString();

 private:
  friend class io::RemoteOperRange;
  friend class io::ScanRange;
  friend class io::WriteRange;
  friend class io::HdfsFileWriter;
  friend class io::LocalFileWriter;
  friend class TmpFileGroup;
  friend class TmpFileRemote;
  friend class TmpFileLocal;
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

  /// done_ is set to true if all the space in the file has been assigned
  volatile bool done_ = false;

  /// dumped is set to true if the file exists locally.
  volatile bool dumped_ = false;

  /// in_dumping_ is set to true if the file is dumping locally.
  volatile bool in_dumping_ = false;

  /// in_writing_ is set to true if the file is in writing, it is the initial status.
  volatile bool in_writing_ = true;

  /// remote_ is set to true if the file is uploaded to remote.
  volatile bool remote_ = false;

  std::unique_ptr<io::FileWriter> file_writer_;

  std::unique_ptr<io::FileWriter> local_buffer_writer_;

  std::mutex dump_lock_;
  ConditionVariable dump_done_;

  hdfsFS hdfs_conn_;

  int64_t file_size_ = 0;

  AtomicInt64 unpinned_page_cnt_{0};

  AtomicInt64 pinned_page_cnt_{0};

  LocalFileMode mode_;

  string local_buffer_path_;

  TmpFileMgr::DeviceId local_buffer_device_id_;

  int local_buffer_disk_id_;

  // Every time to check/modify the status, or need to gurrantee working under
  // certain status, the caller should own the lock
  SpinLock status_lock_;

  // Protect the physical file from renaming or deleting while using
  boost::shared_mutex lock_;

  /// Helper to get the TmpDir that this file is associated with.
  TmpFileMgr::TmpDir* GetDir();
  TmpFileMgr::TmpDir* GetLocalBuffDir();
};

class TmpFileLocal : public TmpFile {
 public:
  using TmpFile::TmpFile;

  bool AllocateSpace(int64_t num_bytes, int64_t* offset);
  io::FileWriter* GetFileWriter(bool write_to_buff = false);
  Status Remove();
  Status RemoveLocalBuff() { return Status::OK(); };
};

class TmpFileRemote : public TmpFile {
 public:
  using TmpFile::TmpFile;

  bool AllocateSpace(int64_t num_bytes, int64_t* offset);
  io::FileWriter* GetFileWriter(bool write_to_buff = false);
  Status Remove();
  Status RemoveLocalBuff();

 private:
  friend class io::RemoteOperRange;
  friend class io::ScanRange;
  friend class io::WriteRange;

  std::unique_ptr<io::RemoteOperRange> upload_range_;

  std::unique_ptr<io::RemoteOperRange> evict_range_;
};

} // namespace impala

#endif
