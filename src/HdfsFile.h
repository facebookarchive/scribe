// Copyright (c) 2009- Facebook
// Distributed under the Scribe Software License
//
// See accompanying file LICENSE or visit the Scribe site at:
// http://developers.facebook.com/scribe/
//
#ifndef HDFS_FILE_H
#define HDFS_FILE_H

#ifdef USE_SCRIBE_HDFS
#include "hdfs.h"

class HdfsFile : public FileInterface {
 public:
  HdfsFile(const std::string& name);
  virtual ~HdfsFile();

  static void init();        // initialize hdfs subsystem
  int  exists();
  bool openRead();           // open for reading file
  bool openWrite();          // open for appending to file
  bool openTruncate();       // truncate and open for write
  bool isOpen();             // is file open?
  void close();
  bool write(const std::string& data);
  void flush();
  unsigned long fileSize();
  long readNext(std::string& _return);
  void deleteFile();
  void listImpl(const std::string& path, std::vector<std::string>& _return);
  std::string getFrame(unsigned data_size);
  bool createDirectory(std::string path);
  bool createSymlink(std::string newpath, std::string oldpath);

 private:
  char* inputBuffer_;
  unsigned bufferSize_;
  hdfsFS fileSys;
  hdfsFile hfile;
  hdfsFS connectToPath(const char* uri);

  // disallow copy, assignment, and empty construction
  HdfsFile();
  HdfsFile(HdfsFile& rhs);
  HdfsFile& operator=(HdfsFile& rhs);
};

/**
 * A static lock
 */
class HdfsLock {
  private:
    static bool lockInitialized;

  public:
    static pthread_mutex_t lock;
    static bool initLock() {
      pthread_mutex_init(&lock, NULL);
      return true;
    }
};

#else

class HdfsFile : public FileInterface {
 public:
  HdfsFile(const std::string& name) : FileInterface(name, false) {
    LOG_OPER("[hdfs] ERROR: HDFS is not supported.  file: %s", name.c_str());
    LOG_OPER("[hdfs] If you want HDFS Support, please recompile scribe with HDFS support");
  }
  static void init() {};
  int  exists()   { return 0; }
  bool openRead() { return false; };           // open for reading file
  bool openWrite(){ return false; };           // open for appending to file
  bool openTruncate() { return false; }        // open for write and truncate
  bool isOpen()   { return false; };           // is file open?
  void close()    {};
  bool write(const std::string& data) { return false; };
  void flush()    {};
  void sync()     {};
  unsigned long fileSize() { return 0; };
  long readNext(std::string& _return) { return false; };
  void deleteFile() {};
  void listImpl(const std::string& path, std::vector<std::string>& _return) {};
  std::string getFrame(unsigned data_size) { return 0; };
  bool createDirectory(std::string path) { return false; };
  bool createSymlink(std::string newpath, std::string oldpath) { return false; };
};
#endif // USE_SCRIBE_HDFS

#endif // HDFS_FILE_H
