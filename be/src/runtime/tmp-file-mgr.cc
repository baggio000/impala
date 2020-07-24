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

#include "runtime/tmp-file-mgr.h"

#include <limits>
#include <mutex>
#include <linux/falloc.h>

#include <zstd.h> // for ZSTD_CLEVEL_DEFAULT
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <gutil/strings/join.h>
#include <gutil/strings/substitute.h>

#include "kudu/util/env.h"
#include "runtime/bufferpool/buffer-pool-counters.h"
#include "runtime/exec-env.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/io/hdfs-file-writer.h"
#include "runtime/io/local-file-writer.h"
#include "runtime/io/request-context.h"
#include "runtime/mem-tracker.h"
#include "runtime/runtime-state.h"
#include "runtime/tmp-file-mgr-internal.h"
#include "util/bit-util.h"
#include "util/codec.h"
#include "util/collection-metrics.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/filesystem-util.h"
#include "util/hdfs-util.h"
#include "util/histogram-metric.h"
#include "util/kudu-status-util.h"
#include "util/os-util.h"
#include "util/parse-util.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"
#include "util/scope-exit-trigger.h"
#include "util/string-parser.h"

#include "common/names.h"

DEFINE_bool(disk_spill_encryption, true,
    "Set this to encrypt and perform an integrity "
    "check on all data spilled to disk during a query");
DEFINE_string(disk_spill_compression_codec, "",
    "(Advanced) If set, data will be compressed using the specified compression codec "
    "before spilling to disk. This can substantially reduce scratch disk usage, at the "
    "cost of requiring more CPU and memory resources to compress the data. Uses the same "
    "syntax as the COMPRESSION_CODEC query option, e.g. 'lz4', 'zstd', 'zstd:6'. If "
    "this is set, then --disk_spill_punch_holes must be enabled.");
DEFINE_int64(disk_spill_compression_buffer_limit_bytes, 512L * 1024L * 1024L,
    "(Advanced) Limit on the total bytes of compression buffers that will be used for "
    "spill-to-disk compression across all queries. If this limit is exceeded, some data "
    "may be spilled to disk in uncompressed form.");
DEFINE_bool(disk_spill_punch_holes, false,
    "(Advanced) changes the free space management strategy for files created in "
    "--scratch_dirs to punch holes in the file when space is unused. This can reduce "
    "the amount of scratch space used by queries, particularly in conjunction with "
    "disk spill compression. This option requires the filesystems of the directories "
    "in --scratch_dirs to support hole punching.");
DEFINE_string(scratch_dirs, "/tmp",
    "Writable scratch directories. "
    "This is a comma-separated list of directories. Each directory is "
    "specified as the directory path, an optional limit on the bytes that will "
    "be allocated in that directory, and an optional priority for the directory. "
    "If the optional limit is provided, the path and "
    "the limit are separated by a colon. E.g. '/dir1:10G,/dir2:5GB,/dir3' will allow "
    "allocating up to 10GB of scratch in /dir1, 5GB of scratch in /dir2 and an "
    "unlimited amount in /dir3. "
    "If the optional priority is provided, the path and the limit and priority are "
    "separated by colon. Priority based spilling will result in directories getting "
    "selected as a spill target based on their priority. The lower the numerical value "
    "the higher the priority. E.g. '/dir1:10G:0,/dir2:5GB:1,/dir3::1', will cause "
    "spilling to first fill up '/dir1' followed by using '/dir2' and '/dir3' in a "
    "round robin manner.");
DEFINE_bool(allow_multiple_scratch_dirs_per_device, true,
    "If false and --scratch_dirs contains multiple directories on the same device, "
    "then only the first writable directory is used");
// TODO: yidawu desc
DEFINE_string(remote_tmp_file_size, "16M",
    "(Advanced) Limit on the total bytes of compression buffers that will be used for "
    "spill-to-disk compression across all queries. If this limit is exceeded, some data "
    "may be spilled to disk in uncompressed form.");
DEFINE_string(remote_tmp_file_block_size, "1M",
    "(Advanced) Limit on the total bytes of compression buffers that will be used for "
    "spill-to-disk compression across all queries. If this limit is exceeded, some data "
    "may be spilled to disk in uncompressed form.");
// TODO: yidawu for test
DEFINE_bool(remote_tmp_file_local_buff_mode, true,
    "If false and --scratch_dirs contains multiple directories on the same device, "
    "then only the first writable directory is used");
DEFINE_bool(remote_tmp_files_avail_pool_lifo, false,
    "If false and --scratch_dirs contains multiple directories on the same device, "
    "then only the first writable directory is used");

using boost::algorithm::is_any_of;
using boost::algorithm::join;
using boost::algorithm::split;
using boost::algorithm::token_compress_off;
using boost::algorithm::token_compress_on;
using boost::filesystem::absolute;
using boost::filesystem::path;
using boost::uuids::random_generator;
using namespace impala::io;
using kudu::Env;
using kudu::RWFile;
using kudu::RWFileOptions;
using namespace strings;

namespace impala {

constexpr int64_t TmpFileMgr::HOLE_PUNCH_BLOCK_SIZE_BYTES;

const string TMP_SUB_DIR_NAME = "impala-scratch";
const uint64_t AVAILABLE_SPACE_THRESHOLD_MB = 1024;

// Metric keys
const string TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS = "tmp-file-mgr.active-scratch-dirs";
const string TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS_LIST =
    "tmp-file-mgr.active-scratch-dirs.list";
const string TMP_FILE_MGR_SCRATCH_SPACE_BYTES_USED_HIGH_WATER_MARK =
    "tmp-file-mgr.scratch-space-bytes-used-high-water-mark";
const string TMP_FILE_MGR_SCRATCH_SPACE_BYTES_USED =
    "tmp-file-mgr.scratch-space-bytes-used";
const string SCRATCH_DIR_BYTES_USED_FORMAT =
    "tmp-file-mgr.scratch-space-bytes-used.dir-$0";
const string LOCAL_BUFF_BYTES_USED_FORMAT = "tmp-file-mgr.local-buff-bytes-used.dir-$0";

using DeviceId = TmpFileMgr::DeviceId;
using TmpDir = TmpFileMgr::TmpDir;
using WriteDoneCallback = TmpFileMgr::WriteDoneCallback;

hdfsFS hdfs_connection_ = nullptr;

TmpFileMgr::TmpFileMgr() {}

TmpFileMgr::~TmpFileMgr() {}

Status TmpFileMgr::Init(MetricGroup* metrics) {
  return InitCustom(FLAGS_scratch_dirs, !FLAGS_allow_multiple_scratch_dirs_per_device,
      FLAGS_disk_spill_compression_codec, FLAGS_disk_spill_punch_holes, metrics);
}

Status TmpFileMgr::InitCustom(const string& tmp_dirs_spec, bool one_dir_per_device,
    const string& compression_codec, bool punch_holes, MetricGroup* metrics) {
  vector<string> all_tmp_dirs;
  // Empty string should be interpreted as no scratch
  if (!tmp_dirs_spec.empty()) {
    split(all_tmp_dirs, tmp_dirs_spec, is_any_of(","), token_compress_on);
  }
  return InitCustom(
      all_tmp_dirs, one_dir_per_device, compression_codec, punch_holes, metrics);
}

Status TmpFileMgr::InitCustom(const vector<string>& tmp_dir_specifiers,
    bool one_dir_per_device, const string& compression_codec, bool punch_holes,
    MetricGroup* metrics) {
  DCHECK(!initialized_);
  punch_holes_ = punch_holes;
  if (tmp_dir_specifiers.empty()) {
    LOG(WARNING) << "Running without spill to disk: no scratch directories provided.";
  }
  if (!compression_codec.empty()) {
    if (!punch_holes) {
      return Status("--disk_spill_punch_holes must be true if disk spill compression "
                    "is enabled");
    }
    Status codec_parse_status = ParseUtil::ParseCompressionCodec(
        compression_codec, &compression_codec_, &compression_level_);
    if (!codec_parse_status.ok()) {
      return Status(
          Substitute("Could not parse --disk_spill_compression_codec value '$0': $1",
              compression_codec, codec_parse_status.GetDetail()));
    }
    if (compression_enabled()) {
      compressed_buffer_tracker_.reset(
          new MemTracker(FLAGS_disk_spill_compression_buffer_limit_bytes,
              "Spill-to-disk temporary compression buffers",
              ExecEnv::GetInstance()->process_mem_tracker()));
    }
  }

  bool is_percent;
  remote_tmp_file_size_ =
      ParseUtil::ParseMemSpec(FLAGS_remote_tmp_file_size, &is_percent, 0);
  if (remote_tmp_file_size_ <= 0) {
    remote_tmp_file_size_ = 16 * 1024 * 1024;
  }
  remote_tmp_block_size_ = ParseUtil::ParseMemSpec(
      FLAGS_remote_tmp_file_block_size, &is_percent, remote_tmp_file_size_);
  if (remote_tmp_block_size_ <= 0) {
    remote_tmp_block_size_ = 1 * 1024 * 1024;
  }
  // TODO: yidawu for test only.
  remote_tmp_file_local_buff_mode_ = FLAGS_remote_tmp_file_local_buff_mode;
  remote_tmp_files_avail_pool_lifo_ = FLAGS_remote_tmp_files_avail_pool_lifo;
  LOG(WARNING) << " tmp_remote_file_size is '" << FLAGS_remote_tmp_file_size
               << "' value is " << std::to_string(remote_tmp_file_size_);
  LOG(WARNING) << " tmp_remote_file_size is '" << FLAGS_remote_tmp_file_block_size
               << "' value is " << std::to_string(remote_tmp_block_size_);
  LOG(WARNING) << " remote_tmp_file_local_buff_mode is '"
               << remote_tmp_file_local_buff_mode_;
  LOG(WARNING) << " remote_tmp_files_avail_pool_lifo is '"
               << remote_tmp_files_avail_pool_lifo_;

  vector<std::unique_ptr<TmpDir>> tmp_dirs;

  // Parse the directory specifiers. Don't return an error on parse errors, just log a
  // warning - we don't want to abort process startup because of misconfigured scratch,
  // since queries will generally still be runnable.
  for (const string& tmp_dir_spec : tmp_dir_specifiers) {
    string tmp_dirs_without_prefix, prefix;
    string tmp_dir_spec_trimed(boost::algorithm::trim_left_copy(tmp_dir_spec));

    if (IsHdfsPath(tmp_dir_spec_trimed.c_str(), false)) {
      prefix = FILESYS_PREFIX_HDFS;
      tmp_dirs_without_prefix = tmp_dir_spec_trimed.substr(strlen(FILESYS_PREFIX_HDFS));
    } else if (IsS3APath(tmp_dir_spec.c_str(), false)) {
      prefix = FILESYS_PREFIX_S3;
      tmp_dirs_without_prefix = tmp_dir_spec_trimed.substr(strlen(FILESYS_PREFIX_S3));
    } else {
      prefix = "";
      tmp_dirs_without_prefix = tmp_dir_spec_trimed.substr(0);
    }

    vector<string> toks;
    split(toks, tmp_dirs_without_prefix, is_any_of(":"), token_compress_off);
    if (toks.size() > 3) {
      LOG(ERROR) << "Could not parse temporary dir specifier, too many colons: '"
                 << tmp_dir_spec << "'";
      continue;
    }
    int64_t bytes_limit = numeric_limits<int64_t>::max();
    if (toks.size() >= 2) {
      bool is_percent;
      bytes_limit = ParseUtil::ParseMemSpec(toks[1], &is_percent, 0);
      if (bytes_limit < 0 || is_percent) {
        LOG(ERROR) << "Malformed scratch directory capacity configuration '"
                   << tmp_dirs_without_prefix << "'";
        continue;
      } else if (bytes_limit == 0) {
        // Interpret -1, 0 or empty string as no limit.
        bytes_limit = numeric_limits<int64_t>::max();
      }
    }
    int priority = numeric_limits<int>::max();
    if (toks.size() == 3 && !toks[2].empty()) {
      StringParser::ParseResult result;
      priority = StringParser::StringToInt<int>(toks[2].data(), toks[2].size(), &result);
      if (result != StringParser::PARSE_SUCCESS) {
        LOG(ERROR) << "Malformed scratch directory priority configuration '"
                   << tmp_dir_spec << "'";
        continue;
      }
    }
    IntGauge* bytes_used_metric = metrics->AddGauge(
        SCRATCH_DIR_BYTES_USED_FORMAT, 0, Substitute("$0", tmp_dirs.size()));
    tmp_dirs.emplace_back(
        new TmpDir(prefix.append(toks[0]), bytes_limit, priority, bytes_used_metric));
  }

  // Sort the tmp directories by priority.
  std::sort(tmp_dirs.begin(), tmp_dirs.end(),
      [](const std::unique_ptr<TmpDir>& a, const std::unique_ptr<TmpDir>& b) {
        return a->priority < b->priority;
      });

  vector<bool> is_tmp_dir_on_disk(DiskInfo::num_disks(), false);
  // For each tmp directory, find the disk it is on,
  // so additional tmp directories on the same disk can be skipped.
  for (int i = 0; i < tmp_dirs.size(); ++i) {
    Status status;
    path tmp_path(trim_right_copy_if(tmp_dirs[i]->path, is_any_of("/")));
    if (IsHdfsPath(tmp_path.c_str(), false)) {
      tmp_dirs_remote_.emplace_back(tmp_path.string(), tmp_dirs[i]->bytes_limit,
          tmp_dirs[i]->priority, tmp_dirs[i]->bytes_used_metric, false);
    } else if (IsS3APath(tmp_path.c_str(), false)) {
      tmp_dirs_remote_.emplace_back(tmp_path.string(), tmp_dirs[i]->bytes_limit,
          tmp_dirs[i]->priority, tmp_dirs[i]->bytes_used_metric, false);
    } else {
      tmp_path = absolute(tmp_path);
      path scratch_subdir_path(tmp_path / TMP_SUB_DIR_NAME);
      // tmp_path must be a writable directory.
      status = FileSystemUtil::VerifyIsDirectory(tmp_path.string());
      if (!status.ok()) {
        LOG(WARNING) << "Cannot use directory " << tmp_path.string()
                     << " for scratch: " << status.msg().msg();
        continue;
      }

      // Find the disk id of tmp_path. Add the scratch directory if there isn't another
      // directory on the same disk (or if we don't know which disk it is on).
      int disk_id = DiskInfo::disk_id(tmp_path.c_str());
      if (!one_dir_per_device || disk_id < 0 || !is_tmp_dir_on_disk[disk_id]) {
        uint64_t available_space;
        RETURN_IF_ERROR(
            FileSystemUtil::GetSpaceAvailable(tmp_path.string(), &available_space));
        if (available_space < AVAILABLE_SPACE_THRESHOLD_MB * 1024 * 1024) {
          LOG(WARNING) << "Filesystem containing scratch directory " << tmp_path
                       << " has less than " << AVAILABLE_SPACE_THRESHOLD_MB
                       << "MB available.";
        }
        // Create the directory, destroying if already present. If this succeeds, we will
        // have an empty writable scratch directory.
        status = FileSystemUtil::RemoveAndCreateDirectory(scratch_subdir_path.string());
        if (status.ok()) {
          if (disk_id >= 0) is_tmp_dir_on_disk[disk_id] = true;
          LOG(INFO) << "Using scratch directory " << scratch_subdir_path.string()
                    << " on "
                    << "disk " << disk_id
                    << " limit: " << PrettyPrinter::PrintBytes(tmp_dirs[i]->bytes_limit);
          tmp_dirs_.emplace_back(scratch_subdir_path.string(), tmp_dirs[i]->bytes_limit,
              tmp_dirs[i]->priority, tmp_dirs[i]->bytes_used_metric);
        } else {
          LOG(WARNING) << "Could not remove and recreate directory "
                       << scratch_subdir_path.string() << ": cannot use it for scratch. "
                       << "Error was: " << status.msg().msg();
        }
        if (punch_holes_) {
          // Make sure hole punching is supported for the directory.
          // IMPALA-9798: this file should *not* be created inside impala-scratch
          // subdirectory to avoid races with multiple impalads starting up.
          RETURN_IF_ERROR(FileSystemUtil::CheckHolePunch(tmp_path.string()));
        }
      }
    }
  }

  DCHECK(metrics != nullptr);
  num_active_scratch_dirs_metric_ =
      metrics->AddGauge(TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS, 0);
  active_scratch_dirs_metric_ = SetMetric<string>::CreateAndRegister(
      metrics, TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS_LIST, set<string>());
  num_active_scratch_dirs_metric_->SetValue(tmp_dirs_.size());
  for (int i = 0; i < tmp_dirs_.size(); ++i) {
    active_scratch_dirs_metric_->Add(tmp_dirs_[i].path);
  }
  scratch_bytes_used_metric_ =
      metrics->AddHWMGauge(TMP_FILE_MGR_SCRATCH_SPACE_BYTES_USED_HIGH_WATER_MARK,
          TMP_FILE_MGR_SCRATCH_SPACE_BYTES_USED, 0);

  if (tmp_dirs_remote_.size() > 0) {
    // Add the second dir as local buffer
    if (tmp_dirs_.size() == 0) {
      int64_t local_buff_bytes_limit = 512 * 1024 * 1024;
      // TODO: create dir and check availability
      string default_local_buff_dir = "/tmp";
      IntGauge* local_buff_bytes_used_metric =
          metrics->AddGauge(LOCAL_BUFF_BYTES_USED_FORMAT, 0, Substitute("$0", 0));
      local_buff_dir_ = std::make_unique<TmpDir>(default_local_buff_dir,
          local_buff_bytes_limit, 0, local_buff_bytes_used_metric);
    } else {
      TmpDir& dir = tmp_dirs_[0];
      local_buff_dir_ = std::make_unique<TmpDir>(
          dir.path, dir.bytes_limit, dir.priority, dir.bytes_used_metric);
      tmp_dirs_.erase(tmp_dirs_.begin());
    }
  }

  initialized_ = true;

  if (tmp_dirs_.empty() && !tmp_dirs.empty()) {
    LOG(ERROR) << "Running without spill to disk: could not use any scratch "
               << "directories in list: " << join(tmp_dir_specifiers, ",")
               << ". See previous warnings for information on causes.";
  }
  return Status::OK();
}

void TmpFileMgr::NewFile(
    TmpFileGroup* file_group, DeviceId device_id, unique_ptr<TmpFile>* new_file) {
  DCHECK(initialized_);
  DCHECK_GE(device_id, 0);
  DCHECK_LT(device_id, tmp_dirs_.size());
  DCHECK(file_group != nullptr);
  // Generate the full file path.
  string unique_name = lexical_cast<string>(random_generator()());
  stringstream file_name;
  file_name << PrintId(file_group->unique_id()) << "_" << unique_name;
  path new_file_path(tmp_dirs_[device_id].path);
  new_file_path /= file_name.str();

  new_file->reset(new TmpFileLocal(file_group, device_id, new_file_path.string(),
      tmp_dirs_[device_id].expected_local));
}

string TmpFileMgr::GetTmpDirPath(DeviceId device_id) const {
  DCHECK(initialized_);
  DCHECK_GE(device_id, 0);
  DCHECK_LT(device_id, tmp_dirs_.size());
  return tmp_dirs_[device_id].path;
}

int64_t TmpFileMgr::GetRemoteTmpFileSize() const {
  return remote_tmp_file_size_;
}

int64_t TmpFileMgr::GetRemoteTmpBlockSize() const {
  return remote_tmp_block_size_;
}

int TmpFileMgr::NumActiveTmpDevices() {
  DCHECK(initialized_);
  return tmp_dirs_.size();
}

vector<DeviceId> TmpFileMgr::ActiveTmpDevices() {
  vector<DeviceId> devices;
  for (DeviceId device_id = 0; device_id < tmp_dirs_.size(); ++device_id) {
    devices.push_back(device_id);
  }
  return devices;
}

TmpFile::TmpFile(TmpFileGroup* file_group, DeviceId device_id, const string& path,
    bool expected_local, const char* hdfs_url, const string* local_buff_path,
    DeviceId* local_device_id)
  : file_group_(file_group),
    path_(path),
    device_id_(device_id),
    disk_id_(DiskInfo::disk_id(path.c_str())),
    expected_local_(expected_local),
    blacklisted_(false) {
  DCHECK(file_group != nullptr);
  file_size_ = file_group_->tmp_file_mgr_->GetRemoteTmpFileSize();
  if (hdfs_url == nullptr) {
    mode_ = LocalFileMode::FILE;
    file_writer_ = make_unique<LocalFileWriter>(file_group->io_mgr_, this, mode_, 0);
  } else {
    DCHECK(local_buff_path != nullptr);
    DCHECK(local_device_id != nullptr);
    local_buffer_path_ = *local_buff_path;
    local_buffer_device_id_ = *local_device_id;
    local_buffer_disk_id_ = DiskInfo::disk_id(local_buffer_path_.c_str());
    hdfs_conn_ = nullptr;
    mode_ = LocalFileMode::BUFFER;
    // TODO: yidawu should open the connection somewhere else
    if (IsHdfsPath(hdfs_url, false)) {
      hdfs_conn_ = hdfsConnect("default", 0);
    } else if (IsS3APath(hdfs_url, false)) {
      string s3a_url = hdfs_url;
      string s3a_access_key;
      string s3a_secret_key;
      string s3a_session_token;
      RunShellProcess("echo $S3A_ACCESS_KEY", &s3a_access_key);
      RunShellProcess("echo $S3A_SECRET_KEY", &s3a_secret_key);
      RunShellProcess("echo $S3A_SESSION_TOKEN", &s3a_session_token);
      LOG(INFO) << "Open Connection with access key " << s3a_access_key;
      LOG(INFO) << "Open Connection with secret key " << s3a_secret_key;
      LOG(INFO) << "Open Connection with session token " << s3a_session_token;
      hdfsBuilder* hdfs_builder = hdfsNewBuilder();
      hdfsBuilderSetNameNode(hdfs_builder, s3a_url.c_str());
      hdfsBuilderSetForceNewInstance(hdfs_builder);
      hdfsBuilderConfSetStr(hdfs_builder, "fs.s3a.access.key", s3a_access_key.c_str());
      hdfsBuilderConfSetStr(hdfs_builder, "fs.s3a.secret.key", s3a_secret_key.c_str());
      hdfsBuilderConfSetStr(
          hdfs_builder, "fs.s3a.session.token", s3a_session_token.c_str());
      // TODO: yidawu s3 fast upload
      hdfsBuilderConfSetStr(hdfs_builder, "fs.s3a.fast.uploadr", "true");
      hdfsBuilderConfSetStr(hdfs_builder, "fs.s3a.fast.upload.buffer", "disk");
      hdfsBuilderConfSetStr(hdfs_builder, "fs.s3a.buffer.dir", "/tmp/disk-io-test");
      hdfs_conn_ = hdfsBuilderConnect(hdfs_builder);
      if (hdfs_conn_ == nullptr) {
        LOG(WARNING) << "Open Connection Failed " << s3a_url;
      }
    } else {
      // Do nothing.
    }
    file_writer_ = make_unique<HdfsFileWriter>(
        file_group->io_mgr_, this, hdfs_conn_, file_size_, expected_local);
    local_buffer_writer_ =
        make_unique<LocalFileWriter>(file_group->io_mgr_, this, mode_, file_size_);
  }
}

TmpFile::~TmpFile() {
  /* TODO: yidawu it will core here, later recheck
  if (hdfs_conn_ != nullptr) {
    int ret = hdfsDisconnect(hdfs_conn_);
    if (ret != 0) {
      LOG(WARNING) << "disconnect connection failed'";
    }
    LOG(WARNING) << "deconstruct tmp file:'" << path_;
  }*/
}

int TmpFile::AssignDiskQueue() const {
  return file_group_->io_mgr_->AssignQueue(path_.c_str(), disk_id_, expected_local_);
}

void TmpFile::Blacklist(const ErrorMsg& msg) {
  LOG(ERROR) << "Error for temporary file '" << path_ << "': " << msg.msg();
  blacklisted_ = true;
}

TmpFileMgr::TmpDir* TmpFile::GetDir() {
  return &file_group_->tmp_file_mgr_->tmp_dirs_[device_id_];
}

TmpFileMgr::TmpDir* TmpFile::GetLocalBuffDir() {
  return &file_group_->tmp_file_mgr_->tmp_dirs_[local_buffer_device_id_];
}

void TmpFile::ResetLocalBuffPath(const string& path, TmpFileMgr::DeviceId device_id) {
  LOG(WARNING) << "Reset path: " << path << " device id:" << device_id;
  local_buffer_path_ = path;
  local_buffer_device_id_ = device_id;
  local_buffer_disk_id_ = DiskInfo::disk_id(path.c_str());
}

Status TmpFile::PunchHole(int64_t offset, int64_t len) {
  DCHECK(file_group_->tmp_file_mgr_->punch_holes());
  // Because of RAII, the file is automatically closed when this function returns.
  RWFileOptions opts;
  opts.mode = Env::CREATE_OR_OPEN;
  unique_ptr<RWFile> file;
  KUDU_RETURN_IF_ERROR(Env::Default()->NewRWFile(opts, path_, &file),
      "Failed to open scratch file for hole punching");
  KUDU_RETURN_IF_ERROR(
      file->PunchHole(offset, len), "Failed to punch hole in scratch file");
  bytes_reclaimed_.Add(len);
  GetDir()->bytes_used_metric->Increment(-len);
  VLOG(3) << "Punched hole in " << path_ << " " << offset << " " << len;
  return Status::OK();
}

bool TmpFile::AllPinned() {
  int64_t pinned_cnt = GetPinCnt();
  return pinned_cnt == GetUnpinCnt() && pinned_cnt != 0;
}

string TmpFile::DebugString() {
  return Substitute(
      "File $0 path '$1' device id $2 disk id $3 allocation offset $4 blacklisted $5",
      this, path_, device_id_, disk_id_, allocation_offset_, blacklisted_);
}

bool TmpFileLocal::AllocateSpace(int64_t num_bytes, int64_t* offset) {
  DCHECK_GT(num_bytes, 0);
  TmpDir* dir = GetDir();
  // Increment optimistically and roll back if the limit is exceeded.
  if (dir->bytes_used_metric->Increment(num_bytes) > dir->bytes_limit) {
    dir->bytes_used_metric->Increment(-num_bytes);
    done_ = false;
    return false;
  }
  *offset = allocation_offset_;
  allocation_offset_ += num_bytes;
  done_ = true;
  return true;
}

io::FileWriter* TmpFileLocal::GetFileWriter(bool write_to_buff) {
  return file_writer_.get();
}

Status TmpFileLocal::Remove() {
  // Remove the file if present (it may not be present if no writes completed).
  Status status = FileSystemUtil::RemovePaths({path_});
  int64_t bytes_in_use = file_group_->tmp_file_mgr_->punch_holes() ?
      allocation_offset_ - bytes_reclaimed_.Load() :
      allocation_offset_;
  GetDir()->bytes_used_metric->Increment(-bytes_in_use);
  return status;
}

bool TmpFileRemote::AllocateSpace(int64_t num_bytes, int64_t* offset) {
  DCHECK_GT(num_bytes, 0);

  // TODO: yidawu add some used metircs
  if (allocation_offset_ + num_bytes > file_size_) {
    return false;
  }
  *offset = allocation_offset_;
  allocation_offset_ += num_bytes;
  // TODO: yidawu assume page size are the same when using one tmpfilegroup
  if (allocation_offset_ + num_bytes > file_size_) {
    done_ = true;
  }
  return true;
}

io::FileWriter* TmpFileRemote::GetFileWriter(bool write_to_buff) {
  if (write_to_buff) {
    return local_buffer_writer_.get();
  }
  return file_writer_.get();
}

Status TmpFileRemote::RemoveLocalBuff() {
  Status status = Status::OK();
  {
    // The file has been deleted already, return ok;
    if (!is_dumped()) return status;
    // All the pages has been read or the file has a backup remote.
    DCHECK(is_remote() || AllPinned());
    status = FileSystemUtil::RemovePaths({local_buffer_path_});
    if (status.ok()) {
      SetDumped(false);
      LOG(WARNING) << "local buffer:" << local_buffer_path_ << " has been deleted";
    } else {
      // TODO: yidawu error handle
      DCHECK(false);
    }
  }
  return status;
}

Status TmpFileRemote::Remove() {
  DCHECK(hdfs_conn_ != nullptr);
  Status status = Status::OK();
  // Remove the file if present (it may not be present if no writes completed).
  {
    lock_guard<shared_mutex> l(lock_);
    // status = FileSystemUtil::RemovePaths({local_buffer_path_});
  }
  // int64_t bytes_in_use = allocation_offset_;
  // TODO: yidawu review the metrics
  // GetDir()->bytes_used_metric->Increment(-bytes_in_use);

  // TODO: yidawu clear remote files async
  /*int ret = hdfsDelete(hdfs_conn_, path_.c_str(), 0);
  if (ret != 0) {
    LOG(WARNING) << "delete hdfs file failed:  " << path_;
  }*/
  // should be put into an array of tmp file mgr, delete them async
  /*
    string& path =
    RemoteOperRange::RemoteOperDoneCallback callback = [](const Status& status) {
      LOG(WARNING) <<
    };
    auto io_mgr =  file_group_->io_mgr_;
    auto io_ctx = file_group_->io_ctx_;
    auto range = new RemoteOperRange(,  file_group_->io_mgr_->RemoteFileOperDiskId(),
    RequestType::DELETE, file_group_->io_mgr_, callback);
    Status add_status = file_group_->io_ctx_->AddRemoteOperRange(range);*/
  return status;
}

TmpFileGroup::TmpFileGroup(TmpFileMgr* tmp_file_mgr, DiskIoMgr* io_mgr,
    RuntimeProfile* profile, const TUniqueId& unique_id, int64_t bytes_limit)
  : tmp_file_mgr_(tmp_file_mgr),
    io_mgr_(io_mgr),
    io_ctx_(nullptr),
    unique_id_(unique_id),
    bytes_limit_(bytes_limit),
    write_counter_(ADD_COUNTER(profile, "ScratchWrites", TUnit::UNIT)),
    bytes_written_counter_(ADD_COUNTER(profile, "ScratchBytesWritten", TUnit::BYTES)),
    uncompressed_bytes_written_counter_(
        ADD_COUNTER(profile, "UncompressedScratchBytesWritten", TUnit::BYTES)),
    read_counter_(ADD_COUNTER(profile, "ScratchReads", TUnit::UNIT)),
    bytes_read_counter_(ADD_COUNTER(profile, "ScratchBytesRead", TUnit::BYTES)),
    scratch_space_bytes_used_counter_(
        ADD_COUNTER(profile, "ScratchFileUsedBytes", TUnit::BYTES)),
    disk_read_timer_(ADD_TIMER(profile, "TotalReadBlockTime")),
    encryption_timer_(ADD_TIMER(profile, "TotalEncryptionTime")),
    compression_timer_(tmp_file_mgr->compression_enabled() ?
            ADD_TIMER(profile, "TotalCompressionTime") :
            nullptr),
    current_bytes_allocated_(0),
    next_allocation_index_(0),
    free_ranges_(64) {
  DCHECK(tmp_file_mgr != nullptr);
  io_ctx_ = io_mgr_->RegisterContext();
  // Populate the priority based index ranges.
  const std::vector<TmpDir>& tmp_dirs = tmp_file_mgr_->tmp_dirs_;
  if (tmp_dirs.size() > 0) {
    int start_index = 0;
    int priority = tmp_dirs[0].priority;
    for (int i = 0; i < tmp_dirs.size() - 1; ++i) {
      priority = tmp_dirs[i].priority;
      const int next_priority = tmp_dirs[i + 1].priority;
      if (next_priority != priority) {
        tmp_files_index_range_.emplace(priority, TmpFileIndexRange(start_index, i));
        start_index = i + 1;
        priority = next_priority;
      }
    }
    tmp_files_index_range_.emplace(
        priority, TmpFileIndexRange(start_index, tmp_dirs.size() - 1));
  }
}

TmpFileGroup::~TmpFileGroup() {
  DCHECK_EQ(tmp_files_.size(), 0);
  LOG(WARNING) << "deconstruct tmp file group'";
}

Status TmpFileGroup::CreateFiles() {
  lock_.DCheckLocked();
  DCHECK(tmp_files_.empty());
  vector<DeviceId> tmp_devices = tmp_file_mgr_->ActiveTmpDevices();
  int files_allocated = 0;
  // Initialize the tmp files and the initial file to use.
  for (int i = 0; i < tmp_devices.size(); ++i) {
    DeviceId device_id = tmp_devices[i];
    unique_ptr<TmpFile> tmp_file;
    tmp_file_mgr_->NewFile(this, device_id, &tmp_file);
    tmp_files_.emplace_back(std::move(tmp_file));
    ++files_allocated;
  }
  DCHECK_EQ(tmp_files_.size(), files_allocated);
  DCHECK_EQ(tmp_file_mgr_->tmp_dirs_.size(), tmp_files_.size());
  if (tmp_files_.size() == 0) return ScratchAllocationFailedStatus({});
  // Initialize the next allocation index for each priority.
  for (const auto& entry : tmp_files_index_range_) {
    const int priority = entry.first;
    const int start = entry.second.start;
    const int end = entry.second.end;
    // Start allocating on a random device to avoid overloading the first device.
    next_allocation_index_.emplace(priority, start + rand() % (end - start + 1));
  }
  return Status::OK();
}

template <typename T>
void TmpFileGroup::CloseInternal(vector<T>& tmp_files) {
  for (auto& file : tmp_files) {
    Status status = file->Remove();
    if (!status.ok()) {
      LOG(WARNING) << "Error removing scratch file '" << file->path()
                   << "': " << status.msg().msg();
    }
  }
  tmp_files.clear();
}

void TmpFileGroup::Close() {
  // Cancel writes before deleting the files, since in-flight writes could re-create
  // deleted files.
  if (io_ctx_ != nullptr) io_mgr_->UnregisterContext(io_ctx_.get());
  CloseInternal<std::unique_ptr<TmpFile>>(tmp_files_);
  CloseInternal<std::unique_ptr<TmpFile>>(tmp_files_remote_);
  tmp_file_mgr_->scratch_bytes_used_metric_->Increment(
      -1 * scratch_space_bytes_used_counter_->value());
}

// Rounds up to the smallest unit of allocation in a scratch file
// that will fit 'bytes'.
static int64_t RoundUpToScratchRangeSize(bool punch_holes, int64_t bytes) {
  if (punch_holes) {
    // Round up to a typical disk block size - 4KB that that hole punching can always
    // free the backing storage for the entire range.
    return BitUtil::RoundUpToPowerOf2(bytes, TmpFileMgr::HOLE_PUNCH_BLOCK_SIZE_BYTES);
  } else {
    // We recycle scratch ranges, which must be positive power-of-two sizes.
    return max<int64_t>(1L, BitUtil::RoundUpToPowerOfTwo(bytes));
  }
}

void TmpFileGroup::EnqueTmpFilesPool(TmpFile* tmp_file, bool front) {
  DCHECK(tmp_file != nullptr);
  {
    unique_lock<mutex> buffer_lock(tmp_files_uploaded_pool_lock_);
    if (front) {
      tmp_files_avail_pool_.push_front(tmp_file);
    } else {
      tmp_files_avail_pool_.push_back(tmp_file);
    }
    // tmp_files_avail_pool_.push(tmp_file);
    tmp_files_pool_cnt_.Add(1);
    LOG(WARNING) << tmp_file->local_buffer_path_
                 << " has been pushed. Total cnt:" << tmp_files_pool_cnt_.Load()
                 << " stack size:" << tmp_files_avail_pool_.size()
                 << " push front:" << (front ? "true" : "false");
  }
  // TODO: yidawu shouldn't it be notify one?
  uploaded_pool_available_.NotifyAll();
}

void TmpFileGroup::DequeTmpFilesPool(TmpFile** tmp_file) {
  DCHECK(tmp_file != nullptr);
  // TODO: yidawu shut down and review
  unique_lock<mutex> buffer_lock(tmp_files_uploaded_pool_lock_);
  LOG(WARNING) << "Total cnt:" << tmp_files_pool_cnt_.Load()
               << " stack size:" << tmp_files_avail_pool_.size();
  bool shut_down = false;
  while (!shut_down && tmp_files_avail_pool_.empty()) {
    // Wait if there are no readers on the queue.
    uploaded_pool_available_.Wait(buffer_lock);
  }
  if (shut_down) return;
  DCHECK(!tmp_files_avail_pool_.empty());
  *tmp_file = tmp_files_avail_pool_.front();
  tmp_files_avail_pool_.pop_front();
  DCHECK(*tmp_file != nullptr);
  tmp_files_pool_cnt_.Add(-1);
  LOG(WARNING) << (*tmp_file)->local_buffer_path_
               << " has been popped. Total cnt:" << tmp_files_pool_cnt_.Load()
               << " stack size:" << tmp_files_avail_pool_.size();
}

// Caller should hold the lock of the tmp_file.
Status TmpFileGroup::EvictFile(TmpFile* tmp_file, bool change_metrics) {
  DCHECK(tmp_file != nullptr);
  Status status = Status::OK();

  LOG(WARNING) << "In Evict file: " << tmp_file->LocalBuffPath();
  if (tmp_file->is_dumped()) {
    // TODO: yidawu might have some problem here,
    // if del in the same time
    // maybe del error return status error would be better
    status = tmp_file->RemoveLocalBuff();
    if (status.ok()) {
      if (change_metrics) {
        int64_t file_size = tmp_file_mgr_->GetRemoteTmpFileSize();
        TmpDir* dir = tmp_file->GetLocalBuffDir();
        dir->bytes_used_metric->Increment(-file_size);
        scratch_space_bytes_used_counter_->Add(-file_size);
        tmp_file_mgr_->scratch_bytes_used_metric_->Increment(-file_size);
        current_bytes_allocated_ -= file_size;
      }
      LOG(WARNING) << "File Evicted: " << tmp_file->LocalBuffPath();
    } else {
      // TODO: yidawu error handle
      DCHECK(false);
    }
  }
  LOG(WARNING) << "End evicting file: " << tmp_file->LocalBuffPath();
  return status;
}

Status TmpFileGroup::EvictFile(DeviceId* device_id) {
  DCHECK(device_id != nullptr);
  TmpFile* tmp_file = nullptr;
  Status status = Status::OK();
  LOG(WARNING) << "In evict file";
  while (true) {
    DequeTmpFilesPool(&tmp_file);
    // TODO: yidawu might have chance to be null during deconstruct
    DCHECK(tmp_file != nullptr);
    boost::unique_lock<boost::shared_mutex> l(tmp_file->lock_);
    lock_guard<SpinLock> sl(tmp_file->status_lock_);
    // Might have been deleted due to all pages pinned.
    if (!tmp_file->is_dumped()) {
      LOG(WARNING) << "Return the device, File deleted already: "
                   << tmp_file->LocalBuffPath();
    } else {
      LOG(WARNING) << "Going to evict file: " << tmp_file->LocalBuffPath();
      status = EvictFile(tmp_file, false);
      DCHECK(status.ok());
    }
    *device_id = tmp_file->local_buffer_device_id_;
    break;
  }
  LOG(WARNING) << "End evict file";
  return status;
}

// Caller hold the lock.
Status TmpFileGroup::AssignFreeDevice(DeviceId* device_id) {
  bool need_evict;
  LOG(WARNING) << "In AssignFreeDevice ";
  {
    int64_t need_bytes = tmp_file_mgr_->GetRemoteTmpFileSize();
    need_evict =
        bytes_limit_ != -1 && current_bytes_allocated_ + need_bytes > bytes_limit_;
    if (!need_evict) {
      tmp_file_mgr_->local_buff_dir_->bytes_used_metric->Increment(need_bytes);
      // TODO: yidawu assign the device id of local buffer
      *device_id = 0;
      scratch_space_bytes_used_counter_->Add(need_bytes);
      tmp_file_mgr_->scratch_bytes_used_metric_->Increment(need_bytes);
      current_bytes_allocated_ += need_bytes;
      LOG(WARNING) << "End AssignFreeDevice ";
      return Status::OK();
    }
  }
  return EvictFile(device_id);
}

string TmpFileGroup::GenerateNewPath(string& dir, string& unique_name) {
  stringstream file_name;
  file_name << TMP_SUB_DIR_NAME << "-" << unique_name;
  path new_file_path(dir);
  new_file_path /= file_name.str();
  return new_file_path.string();
}

Status TmpFileGroup::AllocateSpace(
    int64_t num_bytes, TmpFile** tmp_file, int64_t* file_offset) {
  // TODO: yidawu It would be a problem for the spin lock waiting for evict
  // result. Since it actually waits for the async upload task, if it reaches bytes
  // limit.
  lock_guard<SpinLock> lock(lock_);

  // TODO: yidawu only one dir supported, need multiple dirs support
  // need to consider tmp_dirs_remote
  if (!tmp_file_mgr_->tmp_dirs_remote_.empty()) {
    if (IsHdfsPath(tmp_file_mgr_->tmp_dirs_remote_[0].path.c_str(), false)
        || IsS3APath(tmp_file_mgr_->tmp_dirs_remote_[0].path.c_str(), false)) {
      if (!tmp_files_remote_.empty()) {
        auto tmp_file_cur = tmp_files_remote_.back().get();
        if (tmp_file_cur->AllocateSpace(num_bytes, file_offset)) {
          *tmp_file = tmp_file_cur;
          return Status::OK();
        }
      }

      string unique_name = lexical_cast<string>(random_generator()());
      stringstream file_name;
      string dir = tmp_file_mgr_->tmp_dirs_remote_[0].path;
      int disk_id = io_mgr_->RemoteS3DiskId();
      if (IsHdfsPath(tmp_file_mgr_->tmp_dirs_remote_[0].path.c_str(), false)) {
        // TODO: yidawu hdfs only support hdfs://localhost now
        dir.append(":20500/tmp");
        disk_id = io_mgr_->RemoteDfsDiskId();
      }
      string new_file_path = GenerateNewPath(dir, unique_name);
      // TODO: yidawu we are probably not using the dev id for local buffer.
      DeviceId local_dev_id;
      RETURN_IF_ERROR(AssignFreeDevice(&local_dev_id));
      LOG(WARNING) << "Device assigned: " << local_dev_id;
      string local_buffer_dir = tmp_file_mgr_->local_buff_dir_->path;
      string new_file_path_local = GenerateNewPath(local_buffer_dir, unique_name);
      LOG(WARNING) << "Generate new path: " << new_file_path;
      LOG(WARNING) << "Generate new path local: " << new_file_path_local
                   << " device id:" << local_dev_id;

      unique_ptr<TmpFile> tmp_file_remote(new TmpFileRemote(this, disk_id, new_file_path,
          false, dir.c_str(), &new_file_path_local, &local_dev_id));
      LOG(WARNING) << "tmp file dir:" << dir << " file path:" << new_file_path;

      // It should be successful.
      DCHECK(tmp_file_remote->AllocateSpace(num_bytes, file_offset));

      tmp_files_remote_.emplace_back(std::move(tmp_file_remote));
      *tmp_file = tmp_files_remote_.back().get();
      *file_offset = 0;
      return Status::OK();
    }
  }

  int64_t scratch_range_bytes =
      RoundUpToScratchRangeSize(tmp_file_mgr_->punch_holes(), num_bytes);
  int free_ranges_idx = BitUtil::Log2Ceiling64(scratch_range_bytes);
  if (!free_ranges_[free_ranges_idx].empty()) {
    DCHECK(!tmp_file_mgr_->punch_holes()) << "Ranges not recycled when punching holes";
    *tmp_file = free_ranges_[free_ranges_idx].back().first;
    *file_offset = free_ranges_[free_ranges_idx].back().second;
    free_ranges_[free_ranges_idx].pop_back();
    return Status::OK();
  }

  if (bytes_limit_ != -1
      && current_bytes_allocated_ + scratch_range_bytes > bytes_limit_) {
    return Status(TErrorCode::SCRATCH_LIMIT_EXCEEDED, bytes_limit_, GetBackendString());
  }

  // Lazily create the files on the first write.
  if (tmp_files_.empty()) RETURN_IF_ERROR(CreateFiles());

  // Track the indices of any directories where we failed due to capacity. This is
  // required for error reporting if we are totally out of capacity so that it's clear
  // that some disks were at capacity.
  vector<int> at_capacity_dirs;

  // Find the next physical file in priority based round-robin order and allocate a range
  // from it.
  for (const auto& entry : tmp_files_index_range_) {
    const int priority = entry.first;
    const int start = entry.second.start;
    const int end = entry.second.end;
    DCHECK(0 <= start && start <= end && end < tmp_files_.size())
        << "Invalid index range: [" << start << ", " << end << "] "
        << "tmp_files_.size(): " << tmp_files_.size();
    for (int index = start; index <= end; ++index) {
      const int idx = next_allocation_index_[priority];
      next_allocation_index_[priority] = start + (idx - start + 1) % (end - start + 1);
      *tmp_file = tmp_files_[idx].get();
      if ((*tmp_file)->is_blacklisted()) continue;

      // Check the per-directory limit.
      if (!(*tmp_file)->AllocateSpace(scratch_range_bytes, file_offset)) {
        at_capacity_dirs.push_back(idx);
        continue;
      }
      scratch_space_bytes_used_counter_->Add(scratch_range_bytes);
      tmp_file_mgr_->scratch_bytes_used_metric_->Increment(scratch_range_bytes);
      current_bytes_allocated_ += scratch_range_bytes;
      return Status::OK();
    }
  }

  // TODO: yidawu Find out some space in remote.

  return ScratchAllocationFailedStatus(at_capacity_dirs);
}

void TmpFileGroup::RecycleFileRange(unique_ptr<TmpWriteHandle> handle) {
  TmpFile* file = handle->file_;
  int64_t space_used_bytes =
      RoundUpToScratchRangeSize(tmp_file_mgr_->punch_holes(), handle->on_disk_len());
  if (tmp_file_mgr_->punch_holes()) {
    Status status = file->PunchHole(handle->write_range_->offset(), space_used_bytes);
    if (!status.ok()) {
      // Proceed even in the hole punching fails - we will use extra disk space but
      // functionally we can continue to spill.
      LOG_EVERY_N(WARNING, 100) << "Failed to punch hole in scratch file, couldn't "
                                << "reclaim space: " << status.GetDetail();
      return;
    }
    scratch_space_bytes_used_counter_->Add(-space_used_bytes);
    tmp_file_mgr_->scratch_bytes_used_metric_->Increment(-space_used_bytes);
    {
      lock_guard<SpinLock> lock(lock_);
      current_bytes_allocated_ -= space_used_bytes;
    }
  } else {
    int free_ranges_idx = BitUtil::Log2Ceiling64(space_used_bytes);
    lock_guard<SpinLock> lock(lock_);
    free_ranges_[free_ranges_idx].emplace_back(file, handle->write_range_->offset());
  }
}

Status TmpFileGroup::Write(MemRange buffer, WriteDoneCallback cb,
    unique_ptr<TmpWriteHandle>* handle, const BufferPoolClientCounters* counters) {
  DCHECK_GE(buffer.len(), 0);

  unique_ptr<TmpWriteHandle> tmp_handle(new TmpWriteHandle(this, cb));
  TmpWriteHandle* tmp_handle_ptr = tmp_handle.get(); // Pass ptr by value into lambda.
  WriteRange::WriteDoneCallback callback = [this, tmp_handle_ptr](
                                               const Status& write_status) {
    WriteComplete(tmp_handle_ptr, write_status);
  };
  RETURN_IF_ERROR(tmp_handle->Write(io_ctx_.get(), buffer, callback, counters));
  *handle = move(tmp_handle);
  return Status::OK();
}

Status TmpFileGroup::Read(TmpWriteHandle* handle, MemRange buffer) {
  RETURN_IF_ERROR(ReadAsync(handle, buffer));
  return WaitForAsyncRead(handle, buffer);
}

Status TmpFileGroup::ReadAsync(TmpWriteHandle* handle, MemRange buffer) {
  DCHECK(handle->write_range_ != nullptr);
  DCHECK(!handle->is_cancelled_);
  DCHECK_EQ(buffer.len(), handle->data_len());
  Status status;
  VLOG(3) << "ReadAsync " << handle->TmpFilePath() << " "
          << handle->write_range_->offset() << " " << handle->on_disk_len();
  // Don't grab 'write_state_lock_' in this method - it is not necessary because we
  // don't touch any members that it protects and could block other threads for the
  // duration of the synchronous read.
  DCHECK(!handle->write_in_flight_);
  DCHECK(handle->read_range_ == nullptr);
  DCHECK(handle->write_range_ != nullptr);

  MemRange read_buffer = buffer;
  if (handle->is_compressed()) {
    int64_t compressed_len = handle->compressed_len_;
    if (!handle->compressed_.TryAllocate(compressed_len)) {
      return tmp_file_mgr_->compressed_buffer_tracker()->MemLimitExceeded(
          nullptr, "Failed to decompress spilled data", compressed_len);
    }
    DCHECK_EQ(compressed_len, handle->write_range_->len());
    read_buffer = MemRange(handle->compressed_.buffer(), compressed_len);
  }

  // Don't grab handle->write_state_lock_, it is safe to touch all of handle's state
  // since the write is not in flight.
  handle->read_range_ = scan_range_pool_.Add(new ScanRange);

  if (handle->write_range_->disk_id() == io_mgr_->RemoteDfsDiskId()
      || handle->write_range_->disk_id() == io_mgr_->RemoteS3DiskId()) {
    // TODO: yidawu set mtime
    int mtime = 100000;
    // TODO: yidawu disk id/path/len
    handle->read_range_->Reset(handle->write_range_->tmpfile()->hdfs_conn_,
        handle->write_range_->file(), handle->write_range_->len(),
        handle->write_range_->offset(), 0, false, false, mtime,
        BufferOpts::ReadInto(
            read_buffer.data(), read_buffer.len(), BufferOpts::NO_CACHING),
        nullptr, handle->write_range_->tmpfile());
  } else {
    handle->read_range_->Reset(nullptr, handle->write_range_->file(),
        handle->write_range_->len(), handle->write_range_->offset(),
        handle->write_range_->disk_id(), false, false, ScanRange::INVALID_MTIME,
        BufferOpts::ReadInto(
            read_buffer.data(), read_buffer.len(), BufferOpts::NO_CACHING));
  }
  read_counter_->Add(1);
  bytes_read_counter_->Add(read_buffer.len());
  bool needs_buffers;
  RETURN_IF_ERROR(io_ctx_->StartScanRange(handle->read_range_, &needs_buffers));
  DCHECK(!needs_buffers) << "Already provided a buffer";
  return Status::OK();
}

Status TmpFileGroup::WaitForAsyncRead(
    TmpWriteHandle* handle, MemRange buffer, const BufferPoolClientCounters* counters) {
  DCHECK(handle->read_range_ != nullptr);
  // Don't grab handle->write_state_lock_, it is safe to touch all of handle's state
  // since the write is not in flight.
  SCOPED_TIMER(disk_read_timer_);
  MemRange read_buffer = handle->is_compressed() ?
      MemRange{handle->compressed_.buffer(), handle->compressed_.Size()} :
      buffer;
  DCHECK(read_buffer.data() != nullptr);
  unique_ptr<BufferDescriptor> io_mgr_buffer;
  Status status = handle->read_range_->GetNext(&io_mgr_buffer);
  if (!status.ok()) goto exit;
  DCHECK(io_mgr_buffer != NULL);
  DCHECK(io_mgr_buffer->eosr());
  DCHECK_LE(io_mgr_buffer->len(), read_buffer.len());
  if (io_mgr_buffer->len() < read_buffer.len()) {
    // The read was truncated - this is an error.
    status = Status(TErrorCode::SCRATCH_READ_TRUNCATED, read_buffer.len(),
        handle->write_range_->file(), GetBackendString(), handle->write_range_->offset(),
        io_mgr_buffer->len());
    goto exit;
  }
  DCHECK_EQ(io_mgr_buffer->buffer(),
      handle->is_compressed() ? handle->compressed_.buffer() : buffer.data());

  // Decrypt and decompress in the reverse order that we compressed then encrypted the
  // data originally.
  if (FLAGS_disk_spill_encryption) {
    status = handle->CheckHashAndDecrypt(read_buffer, counters);
    if (!status.ok()) goto exit;
  }

  if (handle->is_compressed()) {
    SCOPED_TIMER2(
        compression_timer_, counters == nullptr ? nullptr : counters->compression_time);
    scoped_ptr<Codec> decompressor;
    status = Codec::CreateDecompressor(
        nullptr, false, tmp_file_mgr_->compression_codec(), &decompressor);
    if (status.ok()) {
      int64_t decompressed_len = buffer.len();
      uint8_t* decompressed_buffer = buffer.data();
      status = decompressor->ProcessBlock(true, read_buffer.len(), read_buffer.data(),
          &decompressed_len, &decompressed_buffer);
    }
    // Free the compressed data regardless of whether the read was successful.
    handle->FreeCompressedBuffer();
    if (!status.ok()) goto exit;
  }
exit:
  // Always return the buffer before exiting to avoid leaking it.
  if (io_mgr_buffer != nullptr) handle->read_range_->ReturnBuffer(move(io_mgr_buffer));
  handle->read_range_ = nullptr;
  return status;
}

Status TmpFileGroup::RestoreData(unique_ptr<TmpWriteHandle> handle, MemRange buffer,
    const BufferPoolClientCounters* counters) {
  DCHECK_EQ(handle->data_len(), buffer.len());
  if (!handle->is_compressed()) DCHECK_EQ(handle->write_range_->data(), buffer.data());
  DCHECK(!handle->write_in_flight_);
  DCHECK(handle->read_range_ == nullptr);

  VLOG(3) << "Restore " << handle->TmpFilePath() << " " << handle->write_range_->offset()
          << " " << handle->data_len();
  Status status;
  if (handle->is_compressed()) {
    // 'buffer' already contains the data needed, because the compressed data was written
    // to 'compressed_' and (optionally) encrypted over there.
  } else if (FLAGS_disk_spill_encryption) {
    // Decrypt after the write is finished, so that we don't accidentally write decrypted
    // data to disk.
    status = handle->CheckHashAndDecrypt(buffer, counters);
  }
  RecycleFileRange(move(handle));
  return status;
}

void TmpFileGroup::DestroyWriteHandle(unique_ptr<TmpWriteHandle> handle) {
  handle->Cancel();
  handle->WaitForWrite();
  RecycleFileRange(move(handle));
}

void TmpFileGroup::WriteComplete(
    TmpWriteHandle* handle, const Status& write_status) {
  Status status;
  if (!write_status.ok()) {
    status = RecoverWriteError(handle, write_status);
    if (status.ok()) return;
  } else {
    status = write_status;
  }
  handle->WriteComplete(status);
}

Status TmpFileGroup::RecoverWriteError(
    TmpWriteHandle* handle, const Status& write_status) {
  DCHECK(!write_status.ok());
  DCHECK(handle->file_ != nullptr);

  // We can't recover from cancellation or memory limit exceeded.
  if (write_status.IsCancelled() || write_status.IsMemLimitExceeded()) {
    return write_status;
  }

  // Save and report the error before retrying so that the failure isn't silent.
  {
    lock_guard<SpinLock> lock(lock_);
    scratch_errors_.push_back(write_status);
    handle->file_->Blacklist(write_status.msg());
  }

  // Do not retry cancelled writes or propagate the error, simply return CANCELLED.
  if (handle->is_cancelled_) return Status::CancelledInternal("TmpFileMgr write");

  TmpFile* tmp_file;
  int64_t file_offset;
  // Discard the scratch file range - we will not reuse ranges from a bad file.
  // Choose another file to try. Blacklisting ensures we don't retry the same file.
  // If this fails, the status will include all the errors in 'scratch_errors_'.
  RETURN_IF_ERROR(AllocateSpace(handle->on_disk_len(), &tmp_file, &file_offset));
  return handle->RetryWrite(io_ctx_.get(), tmp_file, file_offset);
}

Status TmpFileGroup::ScratchAllocationFailedStatus(
    const vector<int>& at_capacity_dirs) {
  vector<string> tmp_dir_paths;
  for (TmpDir& tmp_dir : tmp_file_mgr_->tmp_dirs_) {
    tmp_dir_paths.push_back(tmp_dir.path);
  }
  vector<string> at_capacity_dir_paths;
  for (int dir_idx : at_capacity_dirs) {
    at_capacity_dir_paths.push_back(tmp_file_mgr_->tmp_dirs_[dir_idx].path);
  }
  Status status(TErrorCode::SCRATCH_ALLOCATION_FAILED, join(tmp_dir_paths, ","),
      GetBackendString(),
      PrettyPrinter::PrintBytes(
        tmp_file_mgr_->scratch_bytes_used_metric_->current_value()->GetValue()),
      PrettyPrinter::PrintBytes(current_bytes_allocated_),
      join(at_capacity_dir_paths, ","));
  // Include all previous errors that may have caused the failure.
  for (Status& err : scratch_errors_) status.MergeStatus(err);
  return status;
}

string TmpFileGroup::DebugString() {
  lock_guard<SpinLock> lock(lock_);
  stringstream ss;
  ss << "TmpFileGroup " << this << " bytes limit " << bytes_limit_
     << " current bytes allocated " << current_bytes_allocated_
     << " next allocation index [ ";
  // Get priority based allocation index.
  for (const auto& entry : next_allocation_index_) {
    ss << " (priority: " << entry.first << ", index: " << entry.second << "), ";
  }
  ss << "] writes " << write_counter_->value() << " bytes written "
     << bytes_written_counter_->value() << " uncompressed bytes written "
     << uncompressed_bytes_written_counter_->value() << " reads "
     << read_counter_->value() << " bytes read " << bytes_read_counter_->value()
     << " scratch bytes used " << scratch_space_bytes_used_counter_ << " dist read timer "
     << disk_read_timer_->value() << " encryption timer " << encryption_timer_->value()
     << endl
     << "  " << tmp_files_.size() << " files:" << endl;
  for (unique_ptr<TmpFile>& file : tmp_files_) {
    ss << "    " << file->DebugString() << endl;
  }
  return ss.str();
}

TmpWriteHandle::TmpWriteHandle(
    TmpFileGroup* const parent, WriteRange::WriteDoneCallback cb)
  : parent_(parent),
    cb_(cb),
    compressed_(parent_->tmp_file_mgr_->compressed_buffer_tracker()) {}

TmpWriteHandle::~TmpWriteHandle() {
  DCHECK(!write_in_flight_);
  DCHECK(read_range_ == nullptr);
  DCHECK(compressed_.buffer() == nullptr);
}

string TmpWriteHandle::TmpFilePath() const {
  if (file_ == nullptr) return "";
  return file_->path();
}

int64_t TmpWriteHandle::on_disk_len() const {
  return write_range_->len();
}

Status TmpWriteHandle::Write(RequestContext* io_ctx, MemRange buffer,
    WriteRange::WriteDoneCallback callback, const BufferPoolClientCounters* counters) {
  DCHECK(!write_in_flight_);
  MemRange buffer_to_write = buffer;
  if (parent_->tmp_file_mgr_->compression_enabled() && TryCompress(buffer, counters)) {
    buffer_to_write = MemRange(compressed_.buffer(), compressed_len_);
  }
  // Ensure that the compressed buffer is freed on all the code paths where we did not
  // start the write successfully.
  bool write_started = false;
  const auto free_compressed = MakeScopeExitTrigger([this, &write_started]() {
      if (!write_started) FreeCompressedBuffer();
  });

  // Allocate space after doing compression, to avoid overallocating space.
  TmpFile* tmp_file = write_range_.get() != nullptr ? write_range_->tmpfile() : nullptr;
  int64_t file_offset;

  // For the second unpin of a page, it will be written to a new file since the
  // content should be changed
  RETURN_IF_ERROR(parent_->AllocateSpace(buffer_to_write.len(), &tmp_file, &file_offset));

  if (FLAGS_disk_spill_encryption) {
    RETURN_IF_ERROR(EncryptAndHash(buffer_to_write, counters));
  }

  // Set all member variables before calling AddWriteRange(): after it succeeds,
  // WriteComplete() may be called concurrently with the remainder of this function.
  data_len_ = buffer.len();
  file_ = tmp_file;
  write_range_.reset(new WriteRange(
      tmp_file->path(), file_offset, tmp_file->AssignDiskQueue(), callback));
  write_range_->SetData(buffer_to_write.data(), buffer_to_write.len());
  write_range_->SetTmpFile(tmp_file);
  write_range_->SetRequestContext(io_ctx);
  VLOG(3) << "Write " << tmp_file->path() << " " << file_offset << " "
          << buffer_to_write.len();
  write_in_flight_ = true;
  Status status = io_ctx->AddWriteRange(write_range_.get());
  if (!status.ok()) {
    // The write will not be in flight if we returned with an error.
    write_in_flight_ = false;
    // We won't return this TmpWriteHandle to the client of TmpFileGroup, so it won't be
    // cancelled in the normal way. Mark the handle as cancelled so it can be
    // cleanly destroyed.
    is_cancelled_ = true;
    return status;
  }
  write_started = true;
  parent_->write_counter_->Add(1);
  parent_->uncompressed_bytes_written_counter_->Add(buffer.len());
  parent_->bytes_written_counter_->Add(buffer_to_write.len());
  return Status::OK();
}

bool TmpWriteHandle::TryCompress(
    MemRange buffer, const BufferPoolClientCounters* counters) {
  DCHECK(parent_->tmp_file_mgr_->compression_enabled());
  SCOPED_TIMER2(parent_->compression_timer_,
      counters == nullptr ? nullptr : counters->compression_time);
  DCHECK_LT(compressed_len_, 0);
  DCHECK(compressed_.buffer() == nullptr);
  scoped_ptr<Codec> compressor;
  Status status = Codec::CreateCompressor(nullptr, false,
      Codec::CodecInfo(parent_->tmp_file_mgr_->compression_codec(),
          parent_->tmp_file_mgr_->compression_level()),
      &compressor);
  if (!status.ok()) {
    LOG(WARNING) << "Failed to compress, couldn't create compressor: "
                 << status.GetDetail();
    return false;
  }
  int64_t compressed_buffer_len = compressor->MaxOutputLen(buffer.len());
  if (!compressed_.TryAllocate(compressed_buffer_len)) {
    LOG_EVERY_N(INFO, 100) << "Failed to compress: couldn't allocate "
                           << PrettyPrinter::PrintBytes(compressed_buffer_len);
    return false;
  }
  uint8_t* compressed_buffer = compressed_.buffer();
  int64_t compressed_len = compressed_buffer_len;
  status = compressor->ProcessBlock(
      true, buffer.len(), buffer.data(), &compressed_len, &compressed_buffer);
  if (!status.ok()) {
    compressed_.Release();
    return false;
  }
  compressed_len_ = compressed_len;
  VLOG(3) << "Buffer size: " << buffer.len() << " compressed size: " << compressed_len;
  return true;
}

Status TmpWriteHandle::RetryWrite(RequestContext* io_ctx, TmpFile* file, int64_t offset) {
  DCHECK(write_in_flight_);
  file_ = file;
  write_range_->SetRange(file->path(), offset, file->AssignDiskQueue());
  Status status = io_ctx->AddWriteRange(write_range_.get());
  if (!status.ok()) {
    // The write will not be in flight if we returned with an error.
    write_in_flight_ = false;
    return status;
  }
  return Status::OK();
}

void TmpWriteHandle::WriteComplete(const Status& write_status) {
  WriteDoneCallback cb;
  {
    lock_guard<mutex> lock(write_state_lock_);
    DCHECK(write_in_flight_);
    write_in_flight_ = false;
    // Need to extract 'cb_' because once 'write_in_flight_' is false and we release
    // 'write_state_lock_', 'this' may be destroyed.
    cb = move(cb_);

    if (is_compressed()) {
      DCHECK(compressed_.buffer() != nullptr);
      FreeCompressedBuffer();
    }

    // Notify before releasing the lock - after the lock is released 'this' may be
    // destroyed.
    write_complete_cv_.NotifyAll();
  }
  // Call 'cb' last - once 'cb' is called client code may call Read() or destroy this
  // handle.
  cb(write_status);
}

void TmpWriteHandle::Cancel() {
  CancelRead();
  {
    unique_lock<mutex> lock(write_state_lock_);
    is_cancelled_ = true;
    // TODO: in future, if DiskIoMgr supported write cancellation, we could cancel it
    // here.
  }
}

void TmpWriteHandle::CancelRead() {
  if (read_range_ != nullptr) {
    read_range_->Cancel(Status::CancelledInternal("TmpFileMgr read"));
    read_range_ = nullptr;
    FreeCompressedBuffer();
  }
}

void TmpWriteHandle::WaitForWrite() {
  unique_lock<mutex> lock(write_state_lock_);
  while (write_in_flight_) write_complete_cv_.Wait(lock);
}

Status TmpWriteHandle::EncryptAndHash(
    MemRange buffer, const BufferPoolClientCounters* counters) {
  DCHECK(FLAGS_disk_spill_encryption);
  SCOPED_TIMER2(parent_->encryption_timer_,
      counters == nullptr ? nullptr : counters->encryption_time);
  // Since we're using GCM/CTR/CFB mode, we must take care not to reuse a
  // key/IV pair. Regenerate a new key and IV for every data buffer we write.
  key_.InitializeRandom();
  RETURN_IF_ERROR(key_.Encrypt(buffer.data(), buffer.len(), buffer.data()));

  if (!key_.IsGcmMode()) {
    hash_.Compute(buffer.data(), buffer.len());
  }
  return Status::OK();
}

Status TmpWriteHandle::CheckHashAndDecrypt(
    MemRange buffer, const BufferPoolClientCounters* counters) {
  DCHECK(FLAGS_disk_spill_encryption);
  DCHECK(write_range_ != nullptr);
  SCOPED_TIMER2(parent_->encryption_timer_,
      counters == nullptr ? nullptr : counters->encryption_time);

  // GCM mode will verify the integrity by itself
  if (!key_.IsGcmMode()) {
    if (!hash_.Verify(buffer.data(), buffer.len())) {
      return Status(TErrorCode::SCRATCH_READ_VERIFY_FAILED, buffer.len(),
        write_range_->file(), GetBackendString(), write_range_->offset());
    }
  }
  Status decrypt_status = key_.Decrypt(buffer.data(), buffer.len(), buffer.data());
  if (!decrypt_status.ok()) {
    // Treat decryption failing as a verification failure, but include extra info from
    // the decryption status.
    Status result_status(TErrorCode::SCRATCH_READ_VERIFY_FAILED, buffer.len(),
          write_range_->file(), GetBackendString(), write_range_->offset());
    result_status.MergeStatus(decrypt_status);
    return result_status;
  }
  return Status::OK();
}

void TmpWriteHandle::FreeCompressedBuffer() {
  if (compressed_.buffer() == nullptr) return;
  DCHECK(is_compressed());
  compressed_.Release();
}

string TmpWriteHandle::DebugString() {
  unique_lock<mutex> lock(write_state_lock_);
  stringstream ss;
  ss << "Write handle " << this << " file '" << file_->path() << "'"
     << " is cancelled " << is_cancelled_ << " write in flight " << write_in_flight_;
  if (write_range_ != NULL) {
    ss << " data " << write_range_->data() << " disk range len " << write_range_->len()
       << " file offset " << write_range_->offset() << " disk id "
       << write_range_->disk_id();
  }
  return ss.str();
}
} // namespace impala
