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

#ifndef SCRIBE_THRIFT_FILE_STORE_H
#define SCRIBE_THRIFT_FILE_STORE_H

#include "Common.h"
#include "FileStore.h"

namespace scribe {

/*
 * This file-based store relies on thrift's TFileTransport to do the writing
 */
class ThriftFileStore : public FileStoreBase {
 public:
  ThriftFileStore(StoreQueue* storeq,
                  const string& category,
                  bool multiCategory);
  ~ThriftFileStore();

  StorePtr copy(const string &category);
  bool handleMessages(LogEntryVectorPtr messages);
  bool open();
  bool isOpen();
  void configure(StoreConfPtr configuration, StoreConfPtr parent);
  void close();
  void flush();
  bool createFileDirectory();

 protected:
  // Implement FileStoreBase virtual function
  bool openInternal(bool incrementFilename, struct tm* currentTime);

  shared_ptr<thrift::TTransport> thriftFileTransport_;

  unsigned long flushFrequencyMs_;
  unsigned long msgBufferSize_;
  unsigned long useSimpleFile_;

 private:
  // disallow copy, assignment, and empty construction
  ThriftFileStore(ThriftFileStore& rhs);
  ThriftFileStore& operator=(ThriftFileStore& rhs);
};

} //! namespace scribe

#endif //! SCRIBE_THRIFT_FILE_STORE_H
