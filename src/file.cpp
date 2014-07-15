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

#define INITIAL_BUFFER_SIZE (64 * 1024)
#define LARGE_BUFFER_SIZE (16 * INITIAL_BUFFER_SIZE) /* arbitrarily chosen */
#define UINT_SIZE 4

using namespace std;
using boost::shared_ptr;

boost::shared_ptr<FileInterface> FileInterface::createFileInterface(const std::string& type,
                                                                    const std::string& name,
                                                                    bool framed) {
  if (0 == type.compare("std")) {
    return boost::shared_ptr<FileInterface>(new StdFile(name, framed));
  } else if (0 == type.compare("hdfs")) {
    return boost::shared_ptr<FileInterface>(new HdfsFile(name));
  } else {
    return boost::shared_ptr<FileInterface>();
  }
}

std::vector<std::string> FileInterface::list(const std::string& path, const std::string &fsType) {
  std::vector<std::string> files;
  boost::shared_ptr<FileInterface> concrete_file = createFileInterface(fsType, path);
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

/*
 * read the next frame in the file that is currently open. returns the
 * body of the frame in _return.
 *
 * returns a negative number if it
 * encounters any problem when reading from the file. The negative
 * number is the number of bytes in the file that will not be read
 * becuase of this problem (most likely corruption of file).
 *
 * returns 0 on end of file or when it encounters a frame of size 0
 *
 * On success it returns the number of bytes in the frame's body
 *
 * This function assumes that the file it is reading is framed.
 */
long
StdFile::readNext(std::string& _return) {
  long size;

#define CALC_LOSS() do {                    \
  int offset = file.tellg();                \
  if (offset != -1) {                       \
    size = -(fileSize() - offset);          \
  } else {                                  \
    size = -fileSize();                     \
  }                                         \
  if (size > 0) {                           \
    /* loss size can't be positive          \
     * choose a arbitrary but reasonable
     * value for loss
     */                                     \
    size = -(1000 * 1000 * 1000);           \
  }                                         \
  /* loss size can be 0 */                  \
}  while (0)

  if (!inputBuffer) {
    bufferSize = INITIAL_BUFFER_SIZE;
    inputBuffer = (char *) malloc(bufferSize);
    if (inputBuffer == NULL) {
      CALC_LOSS();
      LOG_OPER("WARNING: nomem Data Loss loss %ld bytes in %s", size,
          filename.c_str());
     return (size);
    }
  }

  file.read(inputBuffer, UINT_SIZE);
  if (!file.good() || (size = unserializeUInt(inputBuffer)) == 0) {
    /* end of file */
    return (0);
  }
  // check if most signiifcant bit set - should never be set
  if (size >= INT_MAX) {
    /* Definitely corrupted. Stop reading any further */
    CALC_LOSS();
    LOG_OPER("WARNING: Corruption Data Loss %ld bytes in %s", size,
        filename.c_str());
    return (size);
  }

  if (size > bufferSize) {
    bufferSize = ((size + INITIAL_BUFFER_SIZE - 1) / INITIAL_BUFFER_SIZE) *
        INITIAL_BUFFER_SIZE;
    free(inputBuffer);
    inputBuffer = (char *) malloc(bufferSize);
    if (bufferSize > LARGE_BUFFER_SIZE) {
      LOG_OPER("WARNING: allocating large buffer Corruption? %d", bufferSize);
    }
  }
  if (inputBuffer == NULL) {
    CALC_LOSS();
    LOG_OPER("WARNING: nomem Corruption? Data Loss %ld bytes in %s", size,
        filename.c_str());
    return (size);
  }
  file.read(inputBuffer, size);
  if (file.good()) {
    _return.assign(inputBuffer, size);
  } else {
    CALC_LOSS();
    LOG_OPER("WARNING: Data Loss %ld bytes in %s", size, filename.c_str());
  }
  if (bufferSize > LARGE_BUFFER_SIZE) {
    free(inputBuffer);
    inputBuffer = NULL;
  }
  return (size);
#undef CALC_LOSS
}

unsigned long StdFile::fileSize() {
  unsigned long size = 0;
  try {
    size = boost::filesystem::file_size(filename.c_str());
  } catch(const std::exception& e) {
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
#if BOOST_VERSION > 104900
        _return.push_back(dir_iter->path().filename().string());
#elif BOOST_VERSION < 104400
        _return.push_back(dir_iter->filename());
#elif defined(BOOST_FILESYSTEM_VERSION) && BOOST_FILESYSTEM_VERSION == 2
        _return.push_back(dir_iter->filename());
#else
        _return.push_back(dir_iter->path().filename().string());
#endif
      }
    }
  } catch (const std::exception& e) {
    LOG_OPER("exception <%s> listing files in <%s>",
             e.what(), path.c_str());
  }
}

void StdFile::deleteFile() {
  boost::filesystem::remove(filename);
}

bool StdFile::createDirectory(std::string path) {
  try {
    boost::filesystem::create_directories(path);
  } catch(const std::exception& e) {
    LOG_OPER("Exception < %s > in StdFile::createDirectory for path %s ",
      e.what(),path.c_str());
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
