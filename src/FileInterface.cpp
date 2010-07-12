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
// @author Jason Sobel
// @author Avinash Lakshman

#include "Common.h"
#include "FileInterface.h"
#include "StdFile.h"
#include "HdfsFile.h"

static const int kUintSize = 4;

namespace scribe {

FileInterfacePtr FileInterface::createFileInterface(const string& type,
                                                    const string& name,
                                                    bool framed) {
  if (0 == type.compare("std")) {
    return FileInterfacePtr(new StdFile(name, framed));
  } else if (0 == type.compare("hdfs")) {
    return FileInterfacePtr(new HdfsFile(name));
  } else {
    return FileInterfacePtr();
  }
}

vector<string> FileInterface::list(const string& path, const string& fsType) {
  vector<string> files;
  FileInterfacePtr concreteFile = createFileInterface(fsType, path);
  if (concreteFile) {
    concreteFile->listImpl(path, &files);
  }
  return files;
}

FileInterface::FileInterface(const string& name, bool frame)
  : framed_(frame), filename_(name) {
}

FileInterface::~FileInterface() {
}

// Buffer had better be at least kUintSize long!
unsigned FileInterface::unserializeUInt(const char* buffer) {
  unsigned retval = 0;
  for (int i = 0; i < kUintSize; ++i) {
    retval |= (unsigned char)buffer[i] << (8 * i);
  }
  return retval;
}

void FileInterface::serializeUInt(unsigned data, char* buffer) {
  for (int i = 0; i < kUintSize; ++i) {
    buffer[i] = (unsigned char)((data >> (8 * i)) & 0xFF);
  }
}

} //! namespace scribe
