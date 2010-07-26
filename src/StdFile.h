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

#ifndef SCRIBE_STD_FILE_H
#define SCRIBE_STD_FILE_H

#include "Common.h"
#include "FileInterface.h"

namespace scribe {

class StdFile : public FileInterface,
                private boost::noncopyable {
 public:
  StdFile(const string& name, bool framed);
  virtual ~StdFile();

  bool  exists();
  bool openRead();
  bool openWrite();
  bool openTruncate();
  bool isOpen();
  void close();
  bool write(const string& data);
  void flush();
  unsigned long fileSize();
  long readNext(string* item);
  void deleteFile();
  void listImpl(const string& path, vector<string>* files);
  string getFrame(unsigned dataSize);
  bool createDirectory(const string& path);
  bool createSymlink(const string& newPath, const string& oldPath);

 private:
  bool open(std::ios_base::openmode mode);

  char* inputBuffer_;
  unsigned bufferSize_;
  std::fstream file_;

  // disallow empty construction
  StdFile();
};

} //! namespace scribe

#endif //! SCRIBE_STD_FILE_H
