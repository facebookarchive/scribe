// Copyright (c) 2009- Facebook
// Distributed under the Scribe Software License
//
// See accompanying file LICENSE or visit the Scribe site at:
// http://developers.facebook.com/scribe/
//

#include <boost/scoped_ptr.hpp>

#include "Common.h"
#include "FileInterface.h"
#include "HdfsFile.h"

using namespace boost;

using scoped_ptr;

namespace scribe {

HdfsFile::HdfsFile(const string& name)
  : FileInterface(name, false), inputBuffer_(NULL), bufferSize_(0) {
  LOG_OPER("[hdfs] Connecting to HDFS for %s", name.c_str());

  // Attempt to parse the hdfs cluster from the path name specified.
  fileSys_ = connectToPath(name);

  if (fileSys_ == 0) {
    // ideally, we should throw an exception here, but the scribe store code
    // does not handle this elegantly now.
    LOG_OPER("[hdfs] ERROR: HDFS is not configured for file: %s", name.c_str());
  }
  hfile_ = 0;

}

HdfsFile::~HdfsFile() {
  if (fileSys_) {
    LOG_OPER("[hdfs] disconnecting fileSys_ for %s", filename_.c_str());
    hdfsDisconnect(fileSys_);
    LOG_OPER("[hdfs] disconnected fileSys_ for %s", filename_.c_str());
  }
  fileSys_ = 0;
  hfile_ = 0;
}

// true for existence
// false for not absence
// throws exception for error
bool HdfsFile::exists() {
  if (!fileSys_) {
    throw std::runtime_error("No Filesys in HdfsFile::exists");
  }

  int value = hdfsExists(fileSys_, filename_.c_str());
  if (value == 0) {
    return true;
  } else if (value == 1) {
    return false;
  } else {
    throw std::runtime_error(kErrorHdfsExists);
  }
}

bool HdfsFile::openRead() {
  if (!fileSys_) {
    fileSys_ = connectToPath(filename_);
  }
  if (fileSys_) {
    hfile_ = hdfsOpenFile(fileSys_, filename_.c_str(), O_RDONLY, 0, 0, 0);
  }
  if (hfile_) {
    LOG_OPER("[hdfs] opened for read %s", filename_.c_str());
    return true;
  }
  return false;
}

bool HdfsFile::openWrite() {
  int flags;

  if (!fileSys_) {
    fileSys_ = connectToPath(filename_);
  }
  if (!fileSys_) {
    LOG_OPER("[hdfs] cannot open hdfs for write %s", filename_.c_str());
    return false;
  }

  if (hfile_) {
    LOG_OPER("[hdfs] already opened for write %s", filename_.c_str());
    return false;
  }


  int ret = hdfsExists(fileSys_, filename.c_str());
  if (0 == ret) {
    flags = O_WRONLY|O_APPEND; // file exists, append to it.
  } else if (1 == ret) {
    flags = O_WRONLY;
  }
  else {
    throw std::runtime_error(kErrorHdfsExists);
  }

  hfile_ = hdfsOpenFile(fileSys_, filename_.c_str(), flags, 0, 0, 0);
  if (hfile_) {
    if (flags & O_APPEND) {
      LOG_OPER("[hdfs] opened for append %s", filename_.c_str());
    } else {
      LOG_OPER("[hdfs] opened for write %s", filename.c_str());
    }
    return true;
  }
  return false;
}

bool HdfsFile::openTruncate() {
  LOG_OPER("[hdfs] truncate %s", filename_.c_str());
  deleteFile();
  return openWrite();
}

bool HdfsFile::isOpen() {
  bool retVal = (hfile_) ? true : false;
  return retVal;
}

void HdfsFile::close() {
  if (fileSys_) {
    if (hfile_) {
      LOG_OPER("[hdfs] closing %s", filename.c_str());
      hdfsCloseFile(fileSys_, hfile_);
      LOG_OPER("[hdfs] closed %s", filename_.c_str());
    } else {
      LOG_OPER("[hdfs] No hfile_!  So no write/flush!");
    }
    hfile_ = 0;

    // Close the file system
    LOG_OPER("[hdfs] disconnecting fileSys_ for %s", filename_.c_str());
    hdfsDisconnect(fileSys_);
    LOG_OPER("[hdfs] disconnected fileSys_ for %s", filename_.c_str());
    fileSys_ = 0;
  }
}

bool HdfsFile::write(const string& data) {
  if (!isOpen()) {
    bool success = openWrite();

    if (!success) {
      return false;
    }
  }

  tSize bytesWritten = hdfsWrite(fileSys_, hfile_, data.data(),
                                 (tSize) data.length());
  return (bytesWritten == (tSize) data.length()) ? true : false;
}

void HdfsFile::flush() {
  if (hfile) {
    hdfsFlush(fileSys, hfile);
  }
}

unsigned long HdfsFile::fileSize() {
  long size = 0L;

  if (fileSys_) {
    hdfsFileInfo* pFileInfo = hdfsGetPathInfo(fileSys_, filename_.c_str());
    if (pFileInfo != NULL) {
      size = pFileInfo->mSize;
      hdfsFreeFileInfo(pFileInfo, 1);
    }
  }
  return size;
}

void HdfsFile::deleteFile() {
  if (fileSys_) {
    hdfsDelete(fileSys_, filename_.c_str());
  }
  LOG_OPER("[hdfs] deleteFile %s", filename_.c_str());
}

void HdfsFile::listImpl(const string& path, vector<string>* files) {
  if (!fileSys_) {
    return;
  }

  int value = hdfsExists(fileSys_, path.c_str());
  switch (value) {
  case 0: {
    int numEntries = 0;
    hdfsFileInfo* pHdfsFileInfo = 0;
    pHdfsFileInfo = hdfsListDirectory(fileSys_, path.c_str(), &numEntries);
    if (numEntries >= 0) {
      for(int i = 0; i < numEntries; i++) {
        char* pathname = pHdfsFileInfo[i].mName;
        char* filename = rindex(pathname, '/');
        if (filename != NULL) {
          files->push_back(filename+1);
        }
      }
      if (pHdfsFileInfo != NULL) {
        hdfsFreeFileInfo(pHdfsFileInfo, numEntries);
      }
    } else {
      // numEntries < 0 indicates error
      throw std::runtime_error(
          string("hdfsListDirectory call failed with error ") +
          boost::lexical_cast<string>(errno));
    }
    break;
  }
  case 1:
    // directory does not exist, do nothing
    break;
    // anything else should be an error
  default:
    throw std::runtime_error(kErrorHdfsExists);
  }
}

long HdfsFile::readNext(string* item) {
  /* choose a reasonable value for loss */
  return (-1000 * 1000 * 1000);
}

string HdfsFile::getFrame(unsigned dataLength) {
  return string();    // not supported
}

bool HdfsFile::createDirectory(const string& path) {
  // opening the file will create the directories.
  return true;
}

/**
 * HDFS currently does not support symlinks. So we create a
 * normal file and write the symlink data into it
 */
bool HdfsFile::createSymlink(const string& oldPath, const string& newPath) {
  LOG_OPER("[hdfs] Creating symlink oldpath %s newpath %s",
           oldPath.c_str(), newPath.c_str());
  scoped_ptr<HdfsFile> link(new HdfsFile(newPath));
  if (link->openWrite() == false) {
    LOG_OPER("[hdfs] Creating symlink failed because %s already exists.",
             newPath.c_str());
    return false;
  }
  if (link->write(oldPath) == false) {
    LOG_OPER("[hdfs] Writing symlink %s failed", newPath.c_str());
    return false;
  }
  link->close();
  return true;
}

/**
 * If the URI is specified of the form
 * hdfs://server::port/path, then connect to the
 * specified cluster
 * else connect to default
 */
hdfsFS HdfsFile::connectToPath(const string& uri) {

  if (uri.empty()) {
    return NULL;
  }

  if (uri.find(kProto) != 0) {
    // uri doesn't start with hdfs:// -> use default:0, which is special
    // to libhdfs.
    return hdfsConnectNewInstance("default", 0);
  }

  vector <string> parts;
  split(parts, uri, is_any_of(":/"), token_compress_on);
  if (parts.size() < 3) {
    return NULL;
  }
  // parts[1] = hosts, parts[2] = port
  string host(parts[1]);
  tPort port;
  try {
    port = lexical_cast<tPort>(parts[2]);
  } catch(const bad_lexical_cast&) {
    LOG_OPER("[hdfs] lexical cast failed");
    return NULL;
  }

  LOG_OPER("[hdfs] Before hdfsConnectNewInstance(%s, %li)", host, port);
  hdfsFS fs = hdfsConnectNewInstance(host.c_str(), port);
  LOG_OPER("[hdfs] After hdfsConnectNewInstance");
  return fs;
}

} //! namespace scribe

