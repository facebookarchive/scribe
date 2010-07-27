// Copyright (c) 2009- Facebook
// Distributed under the Scribe Software License
//
// See accompanying file LICENSE or visit the Scribe site at:
// http://developers.facebook.com/scribe/
//
#ifndef SCRIBE_HDFS_FILE_H
#define SCRIBE_HDFS_FILE_H

#include "Common.h"

#ifdef USE_SCRIBE_HDFS
#include "hdfs.h"

namespace scribe {

const string kErrorHdfsExists("hdfsExists failed ");

class HdfsFile : public FileInterface,
                 private boost::noncopyable {
 public:
  HdfsFile(const string& name);
  virtual ~HdfsFile();

  static void init();        // initialize hdfs subsystem
  bool exists();
  bool openRead();           // open for reading file
  bool openWrite();          // open for appending to file
  bool openTruncate();       // truncate and open for write
  bool isOpen();             // is file open?
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
  char* inputBuffer_;
  unsigned bufferSize_;
  hdfsFS fileSys_;
  hdfsFile hfile_;
  hdfsFS connectToPath(const string& uri);

  // disallow copy, assignment, and empty construction
  HdfsFile();
};

} //! namespace scribe

#else

namespace scribe {

class HdfsFile : public FileInterface,
                 private boost::noncopyable {
 public:
  HdfsFile(const string& name) : FileInterface(name, false) {
    LOG_OPER("[hdfs] ERROR: HDFS is not supported.  file: %s", name.c_str());
    LOG_OPER("[hdfs] If you want HDFS Support, please recompile scribe with HDFS support");
  }
  static void init() {};
  bool exists()   { return false; }
  bool openRead() { return false; };           // open for reading file
  bool openWrite(){ return false; };           // open for appending to file
  bool openTruncate() { return false; }        // open for write and truncate
  bool isOpen()   { return false; };           // is file open?
  void close()    {};
  bool write(const string& data) { return false; };
  void flush()    {};
  unsigned long fileSize() { return 0; };
  long readNext(string* item) { return false; };
  void deleteFile() {};
  void listImpl(const string& path, vector<string>* files) {};
  string getFrame(unsigned dataSize) { return string(); };
  bool createDirectory(const string& path) { return false; };
  bool createSymlink(const string& newPath, const string& oldPath) {
    return false;
  };
};

} //! namespace scribe

#endif // USE_SCRIBE_HDFS

#endif //! SCRIBE_HDFS_FILE_H
