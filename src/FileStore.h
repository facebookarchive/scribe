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

#ifndef SCRIBE_FILE_STORE_H
#define SCRIBE_FILE_STORE_H

#include "Common.h"
#include "FileInterface.h"
#include "FileStoreBase.h"

namespace scribe {

/*
 * This file-based store uses an instance of a FileInterface class that
 * handles the details of interfacing with the filesystem. (see FileInterface.h)
 */
class FileStore : public FileStoreBase {

 public:
  FileStore(StoreQueue* storeq, const string& category,
            bool multiCategory, bool isBufferFile = false);
  ~FileStore();

  StorePtr copy(const string &category);
  bool handleMessages(LogEntryVectorPtr messages);
  bool isOpen();
  void configure(StoreConfPtr configuration, StoreConfPtr parent);
  void close();
  void flush();

  // Each read does its own open and close and gets the whole file.
  // This is separate from the write file, and not really a consistent
  // interface.
  bool readOldest(/*out*/ LogEntryVectorPtr messages,
                  struct tm* now);
  virtual bool replaceOldest(LogEntryVectorPtr messages,
                             struct tm* now);
  void deleteOldest(struct tm* now);
  bool empty(struct tm* now);

 protected:
  // Implement FileStoreBase virtual function
  bool openInternal(bool incrementFilename, struct tm* currentTime);
  bool writeMessages(LogEntryVectorPtr messages,
                     FileInterfacePtr writeFile = FileInterfacePtr());
  string makeFullFilename(int suffix, struct tm* creationTime,
                               bool useFullPath = true);
  string getFullFilename(int suffix, struct tm* creationTime,
                              bool useFullPath = true);

  bool isBufferFile_;
  bool addNewlines_;

  // State
  FileInterfacePtr writeFile_;
  // a buffer to convert messages to/from binary blocks.
  // We make it a member variable to avoid memory alloc/reclaim
  shared_ptr<thrift::TMemoryBuffer> convertBuffer_;

 private:
  // disallow copy, assignment, and empty construction
  FileStore(FileStore& rhs);
  FileStore& operator=(FileStore& rhs);
  long lostBytes_;
};

} //! namespace scribe

#endif //! SCRIBE_FILE_STORE_H
