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
// @author Avinash Lakshman
// @author Jason Sobel

#ifndef SCRIBE_FILE_INTERFACE_H
#define SCRIBE_FILE_INTERFACE_H

#include "Common.h"

namespace scribe {

class FileInterface;
typedef shared_ptr<FileInterface> FileInterfacePtr;

class FileInterface {
 public:
  FileInterface(const string& name, bool framed);
  virtual ~FileInterface();

  static FileInterfacePtr createFileInterface(const string& type,
                                              const string& name,
                                              bool framed = false);
  static vector<string> list(const string& path,
                                       const string& fsType);

  // Test if the specific file exists in the filesystem.
  // Returns:
  //   true  - file exists
  //   false  - file does not exist
  //   throws exception on error
  virtual bool exists() = 0;
  virtual bool openRead() = 0;
  virtual bool openWrite() = 0;
  virtual bool openTruncate() = 0;
  virtual bool isOpen() = 0;
  virtual void close() = 0;
  virtual bool write(const string& data) = 0;
  virtual void flush() = 0;
  virtual unsigned long fileSize() = 0;
  virtual long readNext(string* item) = 0;
  virtual void deleteFile() = 0;
  virtual void listImpl(const string& path, vector<string>* files) = 0;
  virtual string getFrame(unsigned dataSize) { return string(); }
  virtual bool createDirectory(const string& path) = 0;
  virtual bool createSymlink(const string& oldPath, const string& newPath) = 0;

 protected:
  bool framed_;
  string filename_;

  unsigned unserializeUInt(const char* buffer);
  void serializeUInt(unsigned data, char* buffer);
};

} //! namespace scribe

#endif //! SCRIBE_FILE_INTERFACE_H
