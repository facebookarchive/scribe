// Copyright (c) 2009- Facebook
// Distributed under the Scribe Software License
//
// See accompanying file LICENSE or visit the Scribe site at:
// http://developers.facebook.com/scribe/
//

#include <limits>
#include "common.h"
#include "file.h"
#include "HdfsFile.h"

using namespace std;

HdfsFile::HdfsFile(const std::string& name) : FileInterface(name, false), inputBuffer_(NULL), bufferSize_(0) {
  LOG_OPER("[hdfs] Connecting to HDFS");

  // First attempt to parse the hdfs cluster from the path name specified.
  // If it fails, then use the default hdfs cluster.
  fileSys = connectToPath(name.c_str());

  if (fileSys == 0) {
    // ideally, we should throw an exception here, but the scribe store code
    // does not handle this elegantly now.
    LOG_OPER("[hdfs] ERROR: HDFS is not configured for file: %s", name.c_str());
  }
  hfile = 0;
}

HdfsFile::~HdfsFile() {
  if (fileSys) {
    hdfsDisconnect(fileSys);
  }
  fileSys = 0;
  hfile = 0;
}

bool HdfsFile::openRead() {
  if (fileSys) {
    hfile = hdfsOpenFile(fileSys, filename.c_str(), O_RDONLY, 0, 0, 0);
  }
  if (hfile) {
    LOG_OPER("[hdfs] opened for read %s", filename.c_str());
    return true;
  }
  return false;
}

bool HdfsFile::openWrite() {
  int flags;

  if (!fileSys) {
    return false;
  }
  if (hfile) {
    LOG_OPER("[hdfs] already opened for write %s", filename.c_str());
    return false;
  }

  if (hdfsExists(fileSys, filename.c_str()) == 0) {
    flags = O_WRONLY|O_APPEND; // file exists, append to it.
  } else {
    flags = O_WRONLY;
  }
  hfile = hdfsOpenFile(fileSys, filename.c_str(), flags, 0, 0, 0);
  if (hfile) {
    if (flags & O_APPEND) {
      LOG_OPER("[hdfs] opened for append %s", filename.c_str());
    } else {
      LOG_OPER("[hdfs] opened for write %s", filename.c_str());
    }
    return true;
  }
  return false;
}

bool HdfsFile::openTruncate() {
  LOG_OPER("[hdfs] truncate %s", filename.c_str());
  deleteFile();
  return openWrite();
}

bool HdfsFile::isOpen() {
   bool retVal = (hfile) ? true : false;
   return retVal;
}

void HdfsFile::close() {
  if (fileSys) {
    if (hfile) {
      hdfsCloseFile(fileSys, hfile );
      LOG_OPER("[hdfs] closed %s", filename.c_str());
    }
    hfile = 0;
  }
}

bool HdfsFile::write(const std::string& data) {
  if (!isOpen()) {
    bool success = openWrite();

    if (!success) {
      return false;
    }
  }
  tSize bytesWritten = hdfsWrite(fileSys, hfile, data.data(),
                                 (tSize) data.length());
  bool retVal = (bytesWritten == (tSize) data.length()) ? true : false;
  return retVal;
}

void HdfsFile::flush() {
  if (hfile) {
    hdfsFlush(fileSys, hfile);
  }
}

unsigned long HdfsFile::fileSize() {
  long size = 0L;

  if (fileSys) {
    hdfsFileInfo* pFileInfo = hdfsGetPathInfo(fileSys, filename.c_str());
    if (pFileInfo != NULL) {
      size = pFileInfo->mSize;
      hdfsFreeFileInfo(pFileInfo, 1);
    }
  }
  return size;
}

void HdfsFile::deleteFile() {
  if (fileSys) {
    hdfsDelete(fileSys, filename.c_str());
  }
  LOG_OPER("[hdfs] deleteFile %s", filename.c_str());
}

void HdfsFile::listImpl(const std::string& path,
                        std::vector<std::string>& _return) {
  if (!fileSys) {
    return;
  }

  int value = hdfsExists(fileSys, path.c_str());
  if (value == 0) {
    int numEntries = 0;
    hdfsFileInfo* pHdfsFileInfo = 0;
    pHdfsFileInfo = hdfsListDirectory(fileSys, path.c_str(), &numEntries);
    if (pHdfsFileInfo) {
      for(int i = 0; i < numEntries; i++) {
        char* pathname = pHdfsFileInfo[i].mName;
        char* filename = rindex(pathname, '/');
        if (filename != NULL) {
          _return.push_back(filename+1);
        }
      }
      hdfsFreeFileInfo(pHdfsFileInfo, numEntries);
    }
  }
}

bool HdfsFile::readNext(std::string& _return) {
   return false;           // frames not yet supported
}

string HdfsFile::getFrame(unsigned data_length) {
  return std::string();    // not supported
}

bool HdfsFile::createDirectory(std::string path) {
  // opening the file will create the directories.
  return true;
}

/**
 * HDFS currently does not support symlinks. So we create a
 * normal file and write the symlink data into it
 */
bool HdfsFile::createSymlink(std::string oldpath, std::string newpath) {
  LOG_OPER("[hdfs] Creating symlink oldpath %s newpath %s",
           oldpath.c_str(), newpath.c_str());
  HdfsFile* link = new HdfsFile(newpath);
  if (link->openWrite() == false) {
    LOG_OPER("[hdfs] Creating symlink failed because %s already exists.",
             newpath.c_str());
    return false;
  }
  if (link->write(oldpath) == false) {
    LOG_OPER("[hdfs] Writing symlink %s failed", newpath.c_str());
    return false;
  }
  link->close();
  return true;
}

/**
 * If the URI is specified of the form
 * hdfs://server::port/path, then connect to the
 * specified cluster
 */
hdfsFS HdfsFile::connectToPath(const char* uri) {
  const char proto[] = "hdfs://";
 
  if (strncmp(proto, uri, strlen(proto)) != 0) {
    // uri doesn't start with hdfs:// -> use default:0, which is special
    // to libhdfs.
    return hdfsConnectNewInstance("default", 0);
  }
 
  // Skip the hdfs:// part.
  uri += strlen(proto);
  // Find the next colon.
  const char* colon = strchr(uri, ':');
  // No ':' or ':' is the last character.
  if (!colon || !colon[1]) {
    LOG_OPER("[hdfs] Missing port specification: \"%s\"", uri);
    return NULL;
  }
 
  char* endptr = NULL;
  const long port = strtol(colon + 1, &endptr, 10);
  if (port < 0) {
    LOG_OPER("[hdfs] Invalid port specification (negative): \"%s\"", uri);
    return NULL;
  } else if (port > std::numeric_limits<tPort>::max()) {
    LOG_OPER("[hdfs] Invalid port specification (out of range): \"%s\"", uri);
    return NULL;
  }
 
  char* const host = (char*) malloc(colon - uri + 1);
  memcpy((char*) host, uri, colon - uri);
  host[colon - uri] = '\0';
 
  LOG_OPER("[hdfs] Before hdfsConnectNewInstance(%s, %li)", host, port);
  hdfsFS fs = hdfsConnectNewInstance(host, port);
  LOG_OPER("[hdfs] After hdfsConnectNewInstance");
  free(host);
  return fs;
}
