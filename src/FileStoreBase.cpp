//  Copyright (c) 2007-2008 Facebook
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// See accompanying file LICENSE or visit the Scribe site at:
// http://developers.facebook.com/scribe/
//
// @author Bobby Johnson
// @author James Wang
// @author Jason Sobel
// @author Alex Moskalyuk
// @author Avinash Lakshman
// @author Anthony Giardullo
// @author Jan Oravec
// @author John Song

#include "Common.h"
#include "FileInterface.h"
#include "FileStoreBase.h"

static const unsigned long kDefaultFileStoreMaxSize          = 1000000000;
static const unsigned long kDefaultFileStoreMaxWriteSize     = 1000000;
static const unsigned long kDefaultFileStoreRollHour         = 1;
static const unsigned long kDefaultFileStoreRollMinute       = 15;

namespace scribe {

FileStoreBase::FileStoreBase(StoreQueue* storeq,
                             const string& category,
                             const string& type, bool multiCategory)
  : Store(storeq, category, type, multiCategory),
    baseFilePath_("/tmp"),
    subDirectory_(""),
    filePath_("/tmp"),
    baseFileName_(category),
    baseSymlinkName_(""),
    maxSize_(kDefaultFileStoreMaxSize),
    maxWriteSize_(kDefaultFileStoreMaxWriteSize),
    rollPeriod_(ROLL_NEVER),
    rollPeriodLength_(0),
    rollHour_(kDefaultFileStoreRollHour),
    rollMinute_(kDefaultFileStoreRollMinute),
    fsType_("std"),
    chunkSize_(0),
    writeFollowing_(false),
    writeCategory_(false),
    createSymlink_(true),
    writeStats_(false),
    rotateOnReopen_(false),
    currentSize_(0),
    lastRollTime_(0),
    eventsWritten_(0) {
}

FileStoreBase::~FileStoreBase() {
}

void FileStoreBase::configure(StoreConfPtr configuration, StoreConfPtr parent) {
  Store::configure(configuration, parent);

  // We can run using defaults for all of these, but there are
  // a couple of suspicious things we warn about.
  string tmp;
  configuration->getString("file_path", &baseFilePath_);
  configuration->getString("sub_directory", &subDirectory_);
  configuration->getString("use_hostname_sub_directory", &tmp);

  if (0 == tmp.compare("yes")) {
    setHostNameSubDir();
  }

  filePath_ = baseFilePath_;
  if (!subDirectory_.empty()) {
    filePath_ += "/" + subDirectory_;
  }


  if (!configuration->getString("base_filename", &baseFileName_)) {
    LOG_OPER(
        "[%s] WARNING: Bad config - no base_filename specified for file store",
        categoryHandled_.c_str());
  }

  // check if symlink name is optionally specified
  configuration->getString("base_symlink_name", &baseSymlinkName_);

  if (configuration->getString("rotate_period", &tmp)) {
    if (0 == tmp.compare("hourly")) {
      rollPeriod_ = ROLL_HOURLY;
    } else if (0 == tmp.compare("daily")) {
      rollPeriod_ = ROLL_DAILY;
    } else if (0 == tmp.compare("never")) {
      rollPeriod_ = ROLL_NEVER;
    } else {
      errno = 0;
      char* endptr;
      rollPeriod_ = ROLL_OTHER;
      rollPeriodLength_ = strtol(tmp.c_str(), &endptr, 10);

      bool ok = errno == 0 && rollPeriodLength_ > 0 && endptr != tmp.c_str() &&
                (*endptr == '\0' || endptr[1] == '\0');
      switch (*endptr) {
        case 'w':
          rollPeriodLength_ *= 60 * 60 * 24 * 7;
          break;
        case 'd':
          rollPeriodLength_ *= 60 * 60 * 24;
          break;
        case 'h':
          rollPeriodLength_ *= 60 * 60;
          break;
        case 'm':
          rollPeriodLength_ *= 60;
          break;
        case 's':
        case '\0':
          break;
        default:
          ok = false;
          break;
      }

      if (!ok) {
        rollPeriod_ = ROLL_NEVER;
        LOG_OPER("[%s] WARNING: Bad config - invalid format of rotate_period, "
                 "rotations disabled", categoryHandled_.c_str());
      }
    }
  }

  if (configuration->getString("write_meta", &tmp)) {
    if (0 == tmp.compare("yes")) {
      writeFollowing_ = true;
    }
  }
  if (configuration->getString("write_category", &tmp)) {
    if (0 == tmp.compare("yes")) {
      writeCategory_ = true;
    }
  }

  if (configuration->getString("create_symlink", &tmp)) {
    if (0 == tmp.compare("yes")) {
      createSymlink_ = true;
    } else {
      createSymlink_ = false;
    }
  }

  if (configuration->getString("write_stats", &tmp)) {
    if (0 == tmp.compare("yes")) {
      writeStats_ = true;
    } else {
      writeStats_ = false;
    }
  }

  configuration->getString("fs_type", &fsType_);
  configuration->getUnsigned("max_size",        &maxSize_);
  if(0 == maxSize_) {
    maxSize_ = ULONG_MAX;
  }
  configuration->getUnsigned("max_write_size",  &maxWriteSize_);
  configuration->getUnsigned("rotate_hour",     &rollHour_);
  configuration->getUnsigned("rotate_minute",   &rollMinute_);
  configuration->getUnsigned("chunk_size",      &chunkSize_);

  if (configuration->getString("rotate_on_reopen", &tmp)) {
    if (0 == tmp.compare("yes")) {
      rotateOnReopen_ = true;
    } else {
      rotateOnReopen_ = false;
    }
  }
}

void FileStoreBase::copyCommon(const FileStoreBase *base) {
  subDirectory_ = base->subDirectory_;
  chunkSize_ = base->chunkSize_;
  maxSize_ = base->maxSize_;
  maxWriteSize_ = base->maxWriteSize_;
  rollPeriod_ = base->rollPeriod_;
  rollPeriodLength_ = base->rollPeriodLength_;
  rollHour_ = base->rollHour_;
  rollMinute_ = base->rollMinute_;
  fsType_ = base->fsType_;
  writeFollowing_ = base->writeFollowing_;
  writeCategory_ = base->writeCategory_;
  createSymlink_ = base->createSymlink_;
  baseSymlinkName_ = base->baseSymlinkName_;
  writeStats_ = base->writeStats_;
  rotateOnReopen_ = base->rotateOnReopen_;

  /*
   * append the category name to the base file path and change the
   * baseFileName_ to the category name. these are arbitrary, could be anything
   * unique
   */
  baseFilePath_ = base->baseFilePath_ + string("/") + categoryHandled_;
  filePath_ = baseFilePath_;
  if (!subDirectory_.empty()) {
    filePath_ += "/" + subDirectory_;
  }

  baseFileName_ = categoryHandled_;
}

bool FileStoreBase::open() {
  return openInternal(rotateOnReopen_, NULL);
}

// Decides whether conditions are sufficient for us to roll files
void FileStoreBase::periodicCheck() {

  time_t rawTime = time(NULL);
  struct tm timeInfo;
  localtime_r(&rawTime, &timeInfo);

  // Roll the file if we're over max size, or an hour or day has passed
  bool rotate = (currentSize_ > maxSize_);
  if (!rotate) {
    switch (rollPeriod_) {
      case ROLL_DAILY:
        rotate = timeInfo.tm_mday != lastRollTime_ &&
                 static_cast<uint>(timeInfo.tm_hour) >= rollHour_ &&
                 static_cast<uint>(timeInfo.tm_min) >= rollMinute_;
        break;
      case ROLL_HOURLY:
        rotate = timeInfo.tm_hour != lastRollTime_ &&
                 static_cast<uint>(timeInfo.tm_min) >= rollMinute_;
        break;
      case ROLL_OTHER:
        rotate = rawTime >= lastRollTime_ + rollPeriodLength_;
        break;
      case ROLL_NEVER:
        break;
    }
  }

  if (rotate) {
    rotateFile(rawTime);
  }
}

void FileStoreBase::rotateFile(time_t currentTime) {
  struct tm timeInfo;

  currentTime = currentTime > 0 ? currentTime : time(NULL);
  localtime_r(&currentTime, &timeInfo);

  LOG_OPER("[%s] %d:%d rotating file <%s> old size <%lu> max size <%lu>",
           categoryHandled_.c_str(), timeInfo.tm_hour, timeInfo.tm_min,
           makeBaseFilename(&timeInfo).c_str(), currentSize_,
           maxSize_ == ULONG_MAX ? 0 : maxSize_);

  printStats();
  openInternal(true, &timeInfo);
}

string FileStoreBase::makeFullFilename(int suffix, struct tm* creationTime,
                                       bool useFullPath) {

  ostringstream filename;

  if (useFullPath) {
    filename << filePath_ << '/';
  }
  filename << makeBaseFilename(creationTime);
  filename << '_' << setw(5) << setfill('0') << suffix;

  return filename.str();
}

string FileStoreBase::makeBaseSymlink() {
  ostringstream base;
  if (!baseSymlinkName_.empty()) {
    base << baseSymlinkName_ << "_current";
  } else {
    base << baseFileName_ << "_current";
  }
  return base.str();
}

string FileStoreBase::makeFullSymlink() {
  ostringstream filename;
  filename << filePath_ << '/' << makeBaseSymlink();
  return filename.str();
}

string FileStoreBase::makeBaseFilename(struct tm* creationTime) {
  ostringstream filename;

  filename << baseFileName_;
  if (rollPeriod_ != ROLL_NEVER) {
    filename << '-' << creationTime->tm_year + 1900  << '-'
             << setw(2) << setfill('0') << creationTime->tm_mon + 1 << '-'
             << setw(2) << setfill('0')  << creationTime->tm_mday;

  }
  return filename.str();
}

// returns the suffix of the newest file matching baseFilename
int FileStoreBase::findNewestFile(const string& baseFilename) {

  vector<string> files = FileInterface::list(filePath_, fsType_);

  int maxSuffix = -1;
  string retVal;
  for (vector<string>::iterator iter = files.begin();
       iter != files.end();
       ++iter) {

    int suffix = getFileSuffix(*iter, baseFilename);
    if (suffix > maxSuffix) {
      maxSuffix = suffix;
    }
  }
  return maxSuffix;
}

int FileStoreBase::findOldestFile(const string& baseFilename) {

  vector<string> files = FileInterface::list(filePath_, fsType_);

  int minSuffix = -1;
  string retVal;
  for (vector<string>::iterator iter = files.begin();
       iter != files.end();
       ++iter) {

    int suffix = getFileSuffix(*iter, baseFilename);
    if (suffix >= 0 &&
        (minSuffix == -1 || suffix < minSuffix)) {
      minSuffix = suffix;
    }
  }
  return minSuffix;
}

int FileStoreBase::getFileSuffix(const string& filename,
                                const string& baseFilename) {
  int suffix = -1;
  string::size_type suffixPos = filename.rfind('_');

  bool retVal = (0 == filename.substr(0, suffixPos).compare(baseFilename));

  if (string::npos != suffixPos &&
      filename.length() > suffixPos &&
      retVal) {
    stringstream stream;
    stream << filename.substr(suffixPos + 1);
    stream >> suffix;
  }
  return suffix;
}

void FileStoreBase::printStats() {
  if (!writeStats_) {
    return;
  }

  string filename(filePath_);
  filename += "/scribe_stats";

  FileInterfacePtr statsFile =
      FileInterface::createFileInterface(fsType_, filename);
  if (!statsFile ||
      !statsFile->createDirectory(filePath_) ||
      !statsFile->openWrite()) {
    LOG_OPER("[%s] Failed to open stats file <%s> of type <%s> for writing",
             categoryHandled_.c_str(), filename.c_str(), fsType_.c_str());
    // This isn't enough of a problem to change our status
    return;
  }

  time_t rawTime = time(NULL);
  struct tm timeInfo;
  localtime_r(&rawTime, &timeInfo);

  ostringstream msg;
  msg << timeInfo.tm_year + 1900  << '-'
      << setw(2) << setfill('0') << timeInfo.tm_mon + 1 << '-'
      << setw(2) << setfill('0') << timeInfo.tm_mday << '-'
      << setw(2) << setfill('0') << timeInfo.tm_hour << ':'
      << setw(2) << setfill('0') << timeInfo.tm_min;

  msg << " wrote <" << currentSize_ << "> bytes in <" << eventsWritten_
      << "> events to file <" << currentFilename_ << ">" << endl;

  statsFile->write(msg.str());
  statsFile->close();
}

// Returns the number of bytes to pad to align to the specified chunk size
unsigned long FileStoreBase::bytesToPad(unsigned long nextMessageLength,
                                        unsigned long currentFileSize,
                                        unsigned long chunkSize) {

  if (chunkSize > 0) {
    unsigned long spaceLeftInChunk = chunkSize - currentFileSize % chunkSize;
    if (nextMessageLength > spaceLeftInChunk) {
      return spaceLeftInChunk;
    } else {
      return 0;
    }
  }
  // chunkSize <= 0 means don't do any chunking
  return 0;
}

// set subDirectory_ to the name of this machine
void FileStoreBase::setHostNameSubDir() {
  if (!subDirectory_.empty()) {
    string errorMesg = "WARNING: Bad config - ";
    errorMesg += "use_hostname_sub_directory will override sub_directory path";
    LOG_OPER("[%s] %s", categoryHandled_.c_str(), errorMesg.c_str());
  }

  char hostname[255];
  int error = gethostname(hostname, sizeof(hostname));
  if (error) {
    LOG_OPER("[%s] WARNING: gethostname returned error: %d ",
             categoryHandled_.c_str(), error);
  }

  string hostString(hostname);

  if (hostString.empty()) {
    LOG_OPER("[%s] WARNING: could not get host name",
             categoryHandled_.c_str());
  } else {
    subDirectory_ = hostString;
  }
}

} //! namespace scribe
