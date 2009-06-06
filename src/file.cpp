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

#include "common.h"
#include "file.h"
#include "HdfsFile.h"

// INITIAL_BUFFER_SIZE must always be >= UINT_SIZE
#define INITIAL_BUFFER_SIZE 4096
#define UINT_SIZE 4

using namespace std;
using boost::shared_ptr;

boost::shared_ptr<FileInterface> FileInterface::createFileInterface(const std::string& type,
                                                                    const std::string& name,
                                                                    bool framed) {
  if (0 == type.compare("std")) {
    return shared_ptr<FileInterface>(new StdFile(name, framed));
  } else if (0 == type.compare("hdfs")) {
    return shared_ptr<FileInterface>(new HdfsFile(name));
  } else {
    return shared_ptr<FileInterface>();
  }
}

std::vector<std::string> FileInterface::list(const std::string& path, const std::string &fsType) {
  std::vector<std::string> files;
  shared_ptr<FileInterface> concrete_file = createFileInterface(fsType, path);
  if (concrete_file) {
    concrete_file->listImpl(path, files);
  }
  return files;
}

FileInterface::FileInterface(const std::string& name, bool frame)
  : framed(frame), filename(name) {
}

FileInterface::~FileInterface() {
}

StdFile::StdFile(const std::string& name, bool frame)
  : FileInterface(name, frame), inputBuffer(NULL), bufferSize(0) {
}

StdFile::~StdFile() {
  if (inputBuffer) {
    delete[] inputBuffer;
    inputBuffer = NULL;
  }
}

bool StdFile::openRead() {
  return open(fstream::in);
}

bool StdFile::openWrite() {
  // open file for write in append mode
  ios_base::openmode mode = fstream::out | fstream::app;
  return open(mode);
}

bool StdFile::openTruncate() {
  // open an existing file for write and truncate its contents
  ios_base::openmode mode = fstream::out | fstream::app | fstream::trunc;
  return open(mode);
}

bool StdFile::open(ios_base::openmode mode) {

  if (file.is_open()) {
    return false;
  }

  file.open(filename.c_str(), mode);

  return file.good();
}

bool StdFile::isOpen() {
  return file.is_open();
}

void StdFile::close() {
  if (file.is_open()) {
    file.close();
  }
}

string StdFile::getFrame(unsigned data_length) {

  if (framed) {
    char buf[UINT_SIZE];
    serializeUInt(data_length, buf);
    return string(buf, UINT_SIZE);

  } else {
    return string();
  }
}

bool StdFile::write(const std::string& data) {

  if (!file.is_open()) {
    return false;
  }

  file << data;
  if (file.bad()) {
    return false;
  }
  return true;
}

void StdFile::flush() {
  if (file.is_open()) {
    file.flush();
  }
}

bool StdFile::readNext(std::string& _return) {

  if (!inputBuffer) {
    bufferSize = INITIAL_BUFFER_SIZE;
    inputBuffer = new char[bufferSize];
  }

  if (framed) {
    unsigned size;
    file.read(inputBuffer, UINT_SIZE);  // assumes INITIAL_BUFFER_SIZE > UINT_SIZE
    if (file.good() && (size = unserializeUInt(inputBuffer))) {

      // check if size is larger than half the max uint size
      if (size >= (((unsigned)1) << (UINT_SIZE*8 - 1))) {
        LOG_OPER("WARNING: attempting to read message of size %d bytes", size);

        // Do not try to make bufferSize any larger than this or you might overflow
        bufferSize = size;
      }

      while (size > bufferSize) {
        bufferSize = 2 * bufferSize;
        delete[] inputBuffer;
        inputBuffer = new char[bufferSize];
      }
      file.read(inputBuffer, size);
      if (file.good()) {
        _return.assign(inputBuffer, size);
        return true;
      } else {
        int offset = file.tellg();
        LOG_OPER("ERROR: Failed to read file %s at offset %d",
                 filename.c_str(), offset);
        return false;
      }
    }
  } else {
    file.getline(inputBuffer, bufferSize);
    if (file.good()) {
      _return = inputBuffer;
      return true;
    }
  }
  return false;
}

unsigned long StdFile::fileSize() {
  unsigned long size = 0;
  try {
    size = boost::filesystem::file_size(filename.c_str());
  } catch(std::exception const& e) {
    LOG_OPER("Failed to get size for file <%s> error <%s>", filename.c_str(), e.what());
    size = 0;
  }
  return size;
}

void StdFile::listImpl(const std::string& path, std::vector<std::string>& _return) {
  try {
    if (boost::filesystem::exists(path)) {
      boost::filesystem::directory_iterator dir_iter(path), end_iter;

      for ( ; dir_iter != end_iter; ++dir_iter) {
        _return.push_back(dir_iter->filename());
      }
    }
  } catch (std::exception const& e) {
    LOG_OPER("exception <%s> listing files in <%s>",
             e.what(), path.c_str());
  }
}

void StdFile::deleteFile() {
  boost::filesystem::remove(filename);
}

bool StdFile::createDirectory(std::string path) {
  try {
    boost::filesystem::create_directory(path);
  } catch(std::exception const& e) {
    LOG_OPER("Exception < %s > trying to create directory", e.what());
    return false;
  }

  return true;
}

bool StdFile::createSymlink(std::string oldpath, std::string newpath) {
  if (symlink(oldpath.c_str(), newpath.c_str()) == 0) {
    return true;
  }

  return false;
}

// Buffer had better be at least UINT_SIZE long!
unsigned FileInterface::unserializeUInt(const char* buffer) {
  unsigned retval = 0;
  int i;
  for (i = 0; i < UINT_SIZE; ++i) {
    retval |= (unsigned char)buffer[i] << (8 * i);
  }
  return retval;
}

void FileInterface::serializeUInt(unsigned data, char* buffer) {
  int i;
  for (i = 0; i < UINT_SIZE; ++i) {
    buffer[i] = (unsigned char)((data >> (8 * i)) & 0xFF);
  }
}
