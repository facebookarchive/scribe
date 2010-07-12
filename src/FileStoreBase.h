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

#ifndef SCRIBE_FILE_STORE_BASE_H
#define SCRIBE_FILE_STORE_BASE_H

#include "Common.h"
#include "Store.h"

namespace scribe {

/* defines used by the store class */
enum RollPeriod {
  ROLL_NEVER,
  ROLL_HOURLY,
  ROLL_DAILY,
  ROLL_OTHER
};

/*
 * Abstract class that serves as a base for file-based stores.
 * This class has logic for naming files and deciding when to rotate.
 */
class FileStoreBase : public Store {
 public:
  FileStoreBase(StoreQueue* storeq,
                const string& category,
                const string &type, bool multiCategory);
  ~FileStoreBase();

  virtual void copyCommon(const FileStoreBase *base);
  bool open();
  void configure(StoreConfPtr configuration, StoreConfPtr parent);
  virtual void periodicCheck();

 protected:
  // We need to pass arguments to open when called internally.
  // The external open function just calls this with default args.
  virtual bool openInternal(bool incrementFilename, struct tm* currentTime) = 0;
  virtual void rotateFile(time_t currentTime = 0);


  // appends information about the current file to a log file in the same
  // directory
  virtual void printStats();

  // Returns the number of bytes to pad to align to the specified block size
  unsigned long bytesToPad(unsigned long nextMessageLength,
                           unsigned long currentFileSize,
                           unsigned long chunkSize);

  // A full filename includes an absolute path and a sequence number suffix.
  string makeBaseFilename(struct tm* creationTime);
  string makeFullFilename(int suffix, struct tm* creationTime,
                               bool useFullPath = true);
  string makeBaseSymlink();
  string makeFullSymlink();
  int  findOldestFile(const string& baseFilename);
  int  findNewestFile(const string& baseFilename);
  int  getFileSuffix(const string& filename,
                     const string& baseFilename);
  void setHostNameSubDir();

  // Configuration
  string baseFilePath_;
  string subDirectory_;
  string filePath_;
  string baseFileName_;
  string baseSymlinkName_;
  unsigned long maxSize_;
  unsigned long maxWriteSize_;
  RollPeriod rollPeriod_;
  time_t rollPeriodLength_;
  unsigned long rollHour_;
  unsigned long rollMinute_;
  string fsType_;
  unsigned long chunkSize_;
  bool writeFollowing_;       // If this flag is enabled, the name of the file
                              // following this one will be recorded at the end
                              // of the current file
  bool writeCategory_;        // Record category names if set.
  bool createSymlink_;
  bool writeStats_;
  bool rotateOnReopen_;

  // State
  unsigned long currentSize_;
  time_t lastRollTime_;        // either hour, day or time since epoch,
                               // depending on rollPeriod
  string currentFilename_;// this isn't used to choose the next file name,
                               // we just need it for reporting
  unsigned long eventsWritten_;// This is how many events this process has
                               // written to the currently open file. It is NOT
                               // necessarily the number of lines in the file

 private:
  // disallow copy, assignment, and empty construction
  FileStoreBase(FileStoreBase& rhs);
  FileStoreBase& operator=(FileStoreBase& rhs);
};

} //! namespace scribe

#endif //! SCRIBE_FILE_STORE_BASE_H
