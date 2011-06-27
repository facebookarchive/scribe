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

#include <algorithm>
#include "common.h"
#include "scribe_server.h"
#include "network_dynamic_config.h"

using namespace std;
using namespace boost;
using namespace boost::filesystem;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace scribe::thrift;

#define DEFAULT_FILESTORE_MAX_SIZE                1000000000
#define DEFAULT_FILESTORE_MAX_WRITE_SIZE          1000000
#define DEFAULT_FILESTORE_ROLL_HOUR               1
#define DEFAULT_FILESTORE_ROLL_MINUTE             15
#define DEFAULT_BUFFERSTORE_SEND_RATE             1
#define DEFAULT_BUFFERSTORE_AVG_RETRY_INTERVAL    300
#define DEFAULT_BUFFERSTORE_RETRY_INTERVAL_RANGE  60
#define DEFAULT_BUCKETSTORE_DELIMITER             ':'
#define DEFAULT_NETWORKSTORE_CACHE_TIMEOUT        300
#define DEFAULT_BUFFERSTORE_BYPASS_MAXQSIZE_RATIO 0.75

// magic threshold
#define DEFAULT_NETWORKSTORE_DUMMY_THRESHOLD      4096

// Parameters for adaptive_backoff
#define DEFAULT_MIN_RETRY                         5
#define DEFAULT_MAX_RETRY                         100
#define DEFAULT_RANDOM_OFFSET_RANGE               20
const double MULT_INC_FACTOR =                    1.414; //sqrt(2)
#define ADD_DEC_FACTOR                            2
#define CONT_SUCCESS_THRESHOLD                    1

ConnPool g_connPool;

const string meta_logfile_prefix = "scribe_meta<new_logfile>: ";

// Checks if we should try sending a dummy Log in the n/w store
bool shouldSendDummy(boost::shared_ptr<logentry_vector_t> messages) {
  size_t size = 0;
  for (logentry_vector_t::iterator iter = messages->begin();
      iter != messages->end(); ++iter) {
    size += (**iter).message.size();
    if (size > DEFAULT_NETWORKSTORE_DUMMY_THRESHOLD) {
      return true;
    }
  }
  return false;
}


boost::shared_ptr<Store>
Store::createStore(StoreQueue* storeq, const string& type,
                   const string& category, bool readable,
                   bool multi_category) {
  if (0 == type.compare("file")) {
    return shared_ptr<Store>(new FileStore(storeq, category, multi_category,
                                          readable));
  } else if (0 == type.compare("buffer")) {
    return shared_ptr<Store>(new BufferStore(storeq,category, multi_category));
  } else if (0 == type.compare("network")) {
    return shared_ptr<Store>(new NetworkStore(storeq, category,
                                              multi_category));
  } else if (0 == type.compare("bucket")) {
    return shared_ptr<Store>(new BucketStore(storeq, category,
                                            multi_category));
  } else if (0 == type.compare("thriftfile")) {
    return shared_ptr<Store>(new ThriftFileStore(storeq, category,
                                                multi_category));
  } else if (0 == type.compare("null")) {
    return shared_ptr<Store>(new NullStore(storeq, category, multi_category));
  } else if (0 == type.compare("multi")) {
    return shared_ptr<Store>(new MultiStore(storeq, category, multi_category));
  } else if (0 == type.compare("category")) {
    return shared_ptr<Store>(new CategoryStore(storeq, category,
                                              multi_category));
  } else if (0 == type.compare("multifile")) {
    return shared_ptr<Store>(new MultiFileStore(storeq, category,
                                                multi_category));
  } else if (0 == type.compare("thriftmultifile")) {
    return shared_ptr<Store>(new ThriftMultiFileStore(storeq, category,
                                                      multi_category));
  } else {
    return shared_ptr<Store>();
  }
}

Store::Store(StoreQueue* storeq,
             const string& category,
             const string &type,
             bool multi_category)
  : categoryHandled(category),
    multiCategory(multi_category),
    storeType(type),
    storeQueue(storeq) {
  pthread_mutex_init(&statusMutex, NULL);
}

Store::~Store() {
  pthread_mutex_destroy(&statusMutex);
}

void Store::configure(pStoreConf configuration, pStoreConf parent) {
  storeConf = configuration;
  storeConf->setParent(parent);
}

void Store::setStatus(const std::string& new_status) {
  pthread_mutex_lock(&statusMutex);
  status = new_status;
  pthread_mutex_unlock(&statusMutex);
}

std::string Store::getStatus() {
  pthread_mutex_lock(&statusMutex);
  std::string return_status(status);
  pthread_mutex_unlock(&statusMutex);
  return return_status;
}

bool Store::readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,
                       struct tm* now) {
  LOG_OPER("[%s] ERROR: attempting to read from a write-only store",
          categoryHandled.c_str());
  return false;
}

bool Store::replaceOldest(boost::shared_ptr<logentry_vector_t> messages,
                          struct tm* now) {
  LOG_OPER("[%s] ERROR: attempting to read from a write-only store",
          categoryHandled.c_str());
  return false;
}

void Store::deleteOldest(struct tm* now) {
   LOG_OPER("[%s] ERROR: attempting to read from a write-only store",
            categoryHandled.c_str());
}

bool Store::empty(struct tm* now) {
  LOG_OPER("[%s] ERROR: attempting to read from a write-only store",
            categoryHandled.c_str());
  return true;
}

const std::string& Store::getType() {
  return storeType;
}

FileStoreBase::FileStoreBase(StoreQueue* storeq,
                             const string& category,
                             const string &type, bool multi_category)
  : Store(storeq, category, type, multi_category),
    baseFilePath("/tmp"),
    subDirectory(""),
    filePath("/tmp"),
    baseFileName(category),
    baseSymlinkName(""),
    maxSize(DEFAULT_FILESTORE_MAX_SIZE),
    maxWriteSize(DEFAULT_FILESTORE_MAX_WRITE_SIZE),
    rollPeriod(ROLL_NEVER),
    rollPeriodLength(0),
    rollHour(DEFAULT_FILESTORE_ROLL_HOUR),
    rollMinute(DEFAULT_FILESTORE_ROLL_MINUTE),
    fsType("std"),
    chunkSize(0),
    writeMeta(false),
    writeCategory(false),
    createSymlink(true),
    writeStats(false),
    rotateOnReopen(false),
    currentSize(0),
    lastRollTime(0),
    eventsWritten(0) {
}

FileStoreBase::~FileStoreBase() {
}

void FileStoreBase::configure(pStoreConf configuration, pStoreConf parent) {
  Store::configure(configuration, parent);

  // We can run using defaults for all of these, but there are
  // a couple of suspicious things we warn about.
  std::string tmp;
  configuration->getString("file_path", baseFilePath);
  configuration->getString("sub_directory", subDirectory);
  configuration->getString("use_hostname_sub_directory", tmp);

  if (0 == tmp.compare("yes")) {
    setHostNameSubDir();
  }

  filePath = baseFilePath;
  if (!subDirectory.empty()) {
    filePath += "/" + subDirectory;
  }


  if (!configuration->getString("base_filename", baseFileName)) {
    LOG_OPER(
        "[%s] WARNING: Bad config - no base_filename specified for file store",
        categoryHandled.c_str());
  }

  // check if symlink name is optionally specified
  configuration->getString("base_symlink_name", baseSymlinkName);

  if (configuration->getString("rotate_period", tmp)) {
    if (0 == tmp.compare("hourly")) {
      rollPeriod = ROLL_HOURLY;
    } else if (0 == tmp.compare("daily")) {
      rollPeriod = ROLL_DAILY;
    } else if (0 == tmp.compare("never")) {
      rollPeriod = ROLL_NEVER;
    } else {
      errno = 0;
      char* endptr;
      rollPeriod = ROLL_OTHER;
      rollPeriodLength = strtol(tmp.c_str(), &endptr, 10);

      bool ok = errno == 0 && rollPeriodLength > 0 && endptr != tmp.c_str() &&
                (*endptr == '\0' || endptr[1] == '\0');
      switch (*endptr) {
        case 'w':
          rollPeriodLength *= 60 * 60 * 24 * 7;
          break;
        case 'd':
          rollPeriodLength *= 60 * 60 * 24;
          break;
        case 'h':
          rollPeriodLength *= 60 * 60;
          break;
        case 'm':
          rollPeriodLength *= 60;
          break;
        case 's':
        case '\0':
          break;
        default:
          ok = false;
          break;
      }

      if (!ok) {
        rollPeriod = ROLL_NEVER;
        LOG_OPER("[%s] WARNING: Bad config - invalid format of rotate_period, rotations disabled",
            categoryHandled.c_str());
      }
    }
  }

  if (configuration->getString("write_meta", tmp)) {
    if (0 == tmp.compare("yes")) {
      writeMeta = true;
    }
  }
  if (configuration->getString("write_category", tmp)) {
    if (0 == tmp.compare("yes")) {
      writeCategory = true;
    }
  }

  if (configuration->getString("create_symlink", tmp)) {
    if (0 == tmp.compare("yes")) {
      createSymlink = true;
    } else {
      createSymlink = false;
    }
  }

  if (configuration->getString("write_stats", tmp)) {
    if (0 == tmp.compare("yes")) {
      writeStats = true;
    } else {
      writeStats = false;
    }
  }

  configuration->getString("fs_type", fsType);

  configuration->getUnsigned("max_size", maxSize);
  if(0 == maxSize) {
    maxSize = ULONG_MAX;
  }
  configuration->getUnsigned("max_write_size", maxWriteSize);
  configuration->getUnsigned("rotate_hour", rollHour);
  configuration->getUnsigned("rotate_minute", rollMinute);
  configuration->getUnsigned("chunk_size", chunkSize);

  if (configuration->getString("rotate_on_reopen", tmp)) {
    if (0 == tmp.compare("yes")) {
      rotateOnReopen = true;
    } else {
      rotateOnReopen = false;
    }
  }
}

void FileStoreBase::copyCommon(const FileStoreBase *base) {
  subDirectory = base->subDirectory;
  chunkSize = base->chunkSize;
  maxSize = base->maxSize;
  maxWriteSize = base->maxWriteSize;
  rollPeriod = base->rollPeriod;
  rollPeriodLength = base->rollPeriodLength;
  rollHour = base->rollHour;
  rollMinute = base->rollMinute;
  fsType = base->fsType;
  writeMeta = base->writeMeta;
  writeCategory = base->writeCategory;
  createSymlink = base->createSymlink;
  baseSymlinkName = base->baseSymlinkName;
  writeStats = base->writeStats;
  rotateOnReopen = base->rotateOnReopen;

  /*
   * append the category name to the base file path and change the
   * baseFileName to the category name. these are arbitrary, could be anything
   * unique
   */
  baseFilePath = base->baseFilePath + std::string("/") + categoryHandled;
  filePath = baseFilePath;
  if (!subDirectory.empty()) {
    filePath += "/" + subDirectory;
  }

  baseFileName = categoryHandled;
}

bool FileStoreBase::open() {
  return openInternal(rotateOnReopen, NULL);
}

// Decides whether conditions are sufficient for us to roll files
void FileStoreBase::periodicCheck() {

  time_t rawtime = time(NULL);
  struct tm timeinfo;
  localtime_r(&rawtime, &timeinfo);

  // Roll the file if we're over max size, or an hour or day has passed
  bool rotate = ((currentSize > maxSize) && (maxSize != 0));
  if (!rotate) {
    switch (rollPeriod) {
      case ROLL_DAILY:
        rotate = timeinfo.tm_mday != lastRollTime &&
                 static_cast<uint>(timeinfo.tm_hour) >= rollHour &&
                 static_cast<uint>(timeinfo.tm_min) >= rollMinute;
        break;
      case ROLL_HOURLY:
        rotate = timeinfo.tm_hour != lastRollTime &&
                 static_cast<uint>(timeinfo.tm_min) >= rollMinute;
        break;
      case ROLL_OTHER:
        rotate = rawtime >= lastRollTime + rollPeriodLength;
        break;
      case ROLL_NEVER:
        break;
    }
  }

  if (rotate) {
    rotateFile(rawtime);
  }
}

void FileStoreBase::rotateFile(time_t currentTime) {
  struct tm timeinfo;

  currentTime = currentTime > 0 ? currentTime : time(NULL);
  localtime_r(&currentTime, &timeinfo);

  LOG_OPER("[%s] %d:%d rotating file <%s> old size <%lu> max size <%lu>",
           categoryHandled.c_str(), timeinfo.tm_hour, timeinfo.tm_min,
           makeBaseFilename(&timeinfo).c_str(), currentSize,
           maxSize == ULONG_MAX ? 0 : maxSize);

  printStats();
  openInternal(true, &timeinfo);
}

string FileStoreBase::makeFullFilename(int suffix, struct tm* creation_time,
                                       bool use_full_path) {

  ostringstream filename;

  if (use_full_path) {
    filename << filePath << '/';
  }
  filename << makeBaseFilename(creation_time);
  filename << '_' << setw(5) << setfill('0') << suffix;

  return filename.str();
}

string FileStoreBase::makeBaseSymlink() {
  ostringstream base;
  if (!baseSymlinkName.empty()) {
    base << baseSymlinkName << "_current";
  } else {
    base << baseFileName << "_current";
  }
  return base.str();
}

string FileStoreBase::makeFullSymlink() {
  ostringstream filename;
  filename << filePath << '/' << makeBaseSymlink();
  return filename.str();
}

string FileStoreBase::makeBaseFilename(struct tm* creation_time) {
  ostringstream filename;

  filename << baseFileName;
  if (rollPeriod != ROLL_NEVER) {
    filename << '-' << creation_time->tm_year + 1900  << '-'
             << setw(2) << setfill('0') << creation_time->tm_mon + 1 << '-'
             << setw(2) << setfill('0')  << creation_time->tm_mday;

  }
  return filename.str();
}

// returns the suffix of the newest file matching base_filename
int FileStoreBase::findNewestFile(const string& base_filename) {

  std::vector<std::string> files = FileInterface::list(filePath, fsType);

  int max_suffix = -1;
  std::string retval;
  for (std::vector<std::string>::iterator iter = files.begin();
       iter != files.end();
       ++iter) {

    int suffix = getFileSuffix(*iter, base_filename);
    if (suffix > max_suffix) {
      max_suffix = suffix;
    }
  }
  return max_suffix;
}

int FileStoreBase::findOldestFile(const string& base_filename) {

  std::vector<std::string> files = FileInterface::list(filePath, fsType);

  int min_suffix = -1;
  std::string retval;
  for (std::vector<std::string>::iterator iter = files.begin();
       iter != files.end();
       ++iter) {

    int suffix = getFileSuffix(*iter, base_filename);
    if (suffix >= 0 &&
        (min_suffix == -1 || suffix < min_suffix)) {
      min_suffix = suffix;
    }
  }
  return min_suffix;
}

int FileStoreBase::getFileSuffix(const string& filename,
                                const string& base_filename) {
  int suffix = -1;
  string::size_type suffix_pos = filename.rfind('_');

  bool retVal = (0 == filename.substr(0, suffix_pos).compare(base_filename));

  if (string::npos != suffix_pos &&
      filename.length() > suffix_pos &&
      retVal) {
    stringstream stream;
    stream << filename.substr(suffix_pos + 1);
    stream >> suffix;
  }
  return suffix;
}

void FileStoreBase::printStats() {
  if (!writeStats) {
    return;
  }

  string filename(filePath);
  filename += "/scribe_stats";

  boost::shared_ptr<FileInterface> stats_file =
      FileInterface::createFileInterface(fsType, filename);
  if (!stats_file ||
      !stats_file->createDirectory(filePath) ||
      !stats_file->openWrite()) {
    LOG_OPER("[%s] Failed to open stats file <%s> of type <%s> for writing",
             categoryHandled.c_str(), filename.c_str(), fsType.c_str());
    // This isn't enough of a problem to change our status
    return;
  }

  time_t rawtime = time(NULL);
  struct tm timeinfo;
  localtime_r(&rawtime, &timeinfo);

  ostringstream msg;
  msg << timeinfo.tm_year + 1900  << '-'
      << setw(2) << setfill('0') << timeinfo.tm_mon + 1 << '-'
      << setw(2) << setfill('0') << timeinfo.tm_mday << '-'
      << setw(2) << setfill('0') << timeinfo.tm_hour << ':'
      << setw(2) << setfill('0') << timeinfo.tm_min;

  msg << " wrote <" << currentSize << "> bytes in <" << eventsWritten
      << "> events to file <" << currentFilename << ">" << endl;

  stats_file->write(msg.str());
  stats_file->close();
}

// Returns the number of bytes to pad to align to the specified chunk size
unsigned long FileStoreBase::bytesToPad(unsigned long next_message_length,
                                        unsigned long current_file_size,
                                        unsigned long chunk_size) {

  if (chunk_size > 0) {
    unsigned long space_left_in_chunk =
              chunk_size - current_file_size % chunk_size;
    if (next_message_length > space_left_in_chunk) {
      return space_left_in_chunk;
    } else {
      return 0;
    }
  }
  // chunk_size <= 0 means don't do any chunking
  return 0;
}

// set subDirectory to the name of this machine
void FileStoreBase::setHostNameSubDir() {
  if (!subDirectory.empty()) {
    string error_msg = "WARNING: Bad config - ";
    error_msg += "use_hostname_sub_directory will override sub_directory path";
    LOG_OPER("[%s] %s", categoryHandled.c_str(), error_msg.c_str());
  }

  char hostname[255];
  int error = gethostname(hostname, sizeof(hostname));
  if (error) {
    LOG_OPER("[%s] WARNING: gethostname returned error: %d ",
             categoryHandled.c_str(), error);
  }

  string hoststring(hostname);

  if (hoststring.empty()) {
    LOG_OPER("[%s] WARNING: could not get host name",
             categoryHandled.c_str());
  } else {
    subDirectory = hoststring;
  }
}

FileStore::FileStore(StoreQueue* storeq,
                     const string& category,
                     bool multi_category, bool is_buffer_file)
  : FileStoreBase(storeq, category, "file", multi_category),
    isBufferFile(is_buffer_file),
    addNewlines(false),
    lostBytes_(0) {
}

FileStore::~FileStore() {
}

void FileStore::configure(pStoreConf configuration, pStoreConf parent) {
  FileStoreBase::configure(configuration, parent);

  // We can run using defaults for all of these, but there are
  // a couple of suspicious things we warn about.
  if (isBufferFile) {
    // scheduled file rotations of buffer files lead to too many messy cases
    rollPeriod = ROLL_NEVER;

    // Chunks don't work with the buffer file. There's no good reason
    // for this, it's just that the FileStore handles chunk padding and
    // the FileInterface handles framing, and you need to look at both to
    // read a file that's both chunked and framed. The buffer file has
    // to be framed, so we don't allow it to be chunked.
    // (framed means we write a message size to disk before the message
    //  data, which allows us to identify separate messages in binary data.
    //  Chunked means we pad with zeroes to ensure that every multiple
    //  of n bytes is the start of a message, which helps in recovering
    //  corrupted binary data and seeking into large files)
    chunkSize = 0;

    // Combine all categories in a single file for buffers
    if (multiCategory) {
      writeCategory = true;
    }
  }

  unsigned long inttemp = 0;
  configuration->getUnsigned("add_newlines", inttemp);
  addNewlines = inttemp ? true : false;
}

bool FileStore::openInternal(bool incrementFilename, struct tm* current_time) {
  bool success = false;
  struct tm timeinfo;

  if (!current_time) {
    time_t rawtime = time(NULL);
    localtime_r(&rawtime, &timeinfo);
    current_time = &timeinfo;
  }

  try {
    int suffix = findNewestFile(makeBaseFilename(current_time));

    if (incrementFilename) {
      ++suffix;
    }

    // this is the case where there's no file there and we're not incrementing
    if (suffix < 0) {
      if (rollPeriod == ROLL_HOURLY) {
        suffix = current_time->tm_hour;
      }
      else {
        suffix = 0;
      }
    }

    string file = makeFullFilename(suffix, current_time);

    switch (rollPeriod) {
      case ROLL_DAILY:
        lastRollTime = current_time->tm_mday;
        break;
      case ROLL_HOURLY:
        lastRollTime = current_time->tm_hour;
        break;
      case ROLL_OTHER:
        lastRollTime = time(NULL);
        break;
      case ROLL_NEVER:
        break;
    }

    if (writeFile) {
      if (writeMeta) {
        writeFile->write(meta_logfile_prefix + file);
      }
      writeFile->close();
    }

    writeFile = FileInterface::createFileInterface(fsType, file, isBufferFile);
    if (!writeFile) {
      LOG_OPER("[%s] Failed to create file <%s> of type <%s> for writing",
               categoryHandled.c_str(), file.c_str(), fsType.c_str());
      setStatus("file open error");
      return false;
    }

    success = writeFile->createDirectory(baseFilePath);

    // If we created a subdirectory, we need to create two directories
    if (success && !subDirectory.empty()) {
      success = writeFile->createDirectory(filePath);
    }

    if (!success) {
      LOG_OPER("[%s] Failed to create directory for file <%s>",
               categoryHandled.c_str(), file.c_str());
      setStatus("File open error");
      return false;
    }

    success = writeFile->openWrite();


    if (!success) {
      LOG_OPER("[%s] Failed to open file <%s> for writing",
              categoryHandled.c_str(),
              file.c_str());
      setStatus("File open error");
    } else {

      /* just make a best effort here, and don't error if it fails */
      if (createSymlink && !isBufferFile) {
        string symlinkName = makeFullSymlink();
        boost::shared_ptr<FileInterface> tmp =
          FileInterface::createFileInterface(fsType, symlinkName, isBufferFile);
        tmp->deleteFile();
        string symtarget = makeFullFilename(suffix, current_time, false);
        writeFile->createSymlink(symtarget, symlinkName);
      }
      // else it confuses the filename code on reads

      LOG_OPER("[%s] Opened file <%s> for writing", categoryHandled.c_str(),
              file.c_str());

      currentSize = writeFile->fileSize();
      currentFilename = file;
      eventsWritten = 0;
      setStatus("");
    }

  } catch(const std::exception& e) {
    LOG_OPER("[%s] Failed to create/open file of type <%s> for writing",
             categoryHandled.c_str(), fsType.c_str());
    LOG_OPER("Exception: %s", e.what());
    setStatus("file create/open error");

    return false;
  }
  return success;
}

bool FileStore::isOpen() {
  return writeFile && writeFile->isOpen();
}

void FileStore::close() {
  if (writeFile) {
    writeFile->close();
  }
}

void FileStore::flush() {
  if (writeFile) {
    writeFile->flush();
  }
}

shared_ptr<Store> FileStore::copy(const std::string &category) {
  FileStore *store = new FileStore(storeQueue, category, multiCategory,
                                   isBufferFile);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->addNewlines = addNewlines;
  store->copyCommon(this);
  return copied;
}

bool FileStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {

  if (!isOpen()) {
    if (!open()) {
      LOG_OPER("[%s] File failed to open FileStore::handleMessages()",
               categoryHandled.c_str());
      return false;
    }
  }

  // write messages to current file
  return writeMessages(messages);
}

// writes messages to either the specified file or the the current writeFile
bool FileStore::writeMessages(boost::shared_ptr<logentry_vector_t> messages,
                              boost::shared_ptr<FileInterface> file) {
  // Data is written to a buffer first, then sent to disk in one call to write.
  // This costs an extra copy of the data, but dramatically improves latency with
  // network based files. (nfs, etc)
  string        write_buffer;
  bool          success = true;
  unsigned long current_size_buffered = 0; // size of data in write_buffer
  unsigned long num_buffered = 0;
  unsigned long num_written = 0;
  boost::shared_ptr<FileInterface> write_file;
  unsigned long max_write_size = min(maxSize, maxWriteSize);

  // if no file given, use current writeFile
  if (file) {
    write_file = file;
  } else {
    write_file = writeFile;
  }

  try {
    for (logentry_vector_t::iterator iter = messages->begin();
         iter != messages->end();
         ++iter) {

      // have to be careful with the length here. getFrame wants the length without
      // the frame, then bytesToPad wants the length of the frame and the message.
      unsigned long length = 0;
      unsigned long message_length = (*iter)->message.length();
      string frame, category_frame;

      if (addNewlines) {
        ++message_length;
      }

      length += message_length;

      if (writeCategory) {
        //add space for category+newline and category frame
        unsigned long category_length = (*iter)->category.length() + 1;
        length += category_length;

        category_frame = write_file->getFrame(category_length);
        length += category_frame.length();
      }

      // frame is a header that the underlying file class can add to each message
      frame = write_file->getFrame(message_length);

      length += frame.length();

      // padding to align messages on chunk boundaries
      unsigned long padding = bytesToPad(length, current_size_buffered, chunkSize);

      length += padding;

      if (padding) {
        write_buffer += string(padding, 0);
      }

      if (writeCategory) {
        write_buffer += category_frame;
        write_buffer += (*iter)->category + "\n";
      }

      write_buffer += frame;
      write_buffer += (*iter)->message;

      if (addNewlines) {
        write_buffer += "\n";
      }

      current_size_buffered += length;
      num_buffered++;

      // Write buffer if processing last message or if larger than allowed
      if ((current_size_buffered > max_write_size && maxSize != 0) ||
          messages->end() == iter + 1 ) {
        if (!write_file->write(write_buffer)) {
          LOG_OPER("[%s] File store failed to write (%lu) messages to file",
                   categoryHandled.c_str(), messages->size());
          setStatus("File write error");
          success = false;
          break;
        }

        num_written += num_buffered;
        currentSize += current_size_buffered;
        num_buffered = 0;
        current_size_buffered = 0;
        write_buffer = "";
      }

      // rotate file if large enough and not writing to a separate file
      if ((currentSize > maxSize && maxSize != 0 )&& !file) {
        rotateFile();
        write_file = writeFile;
      }
    }
  } catch (const std::exception& e) {
    LOG_OPER("[%s] File store failed to write. Exception: %s",
             categoryHandled.c_str(), e.what());
    success = false;
  }

  eventsWritten += num_written;

  if (!success) {
    close();

    // update messages to include only the messages that were not handled
    if (num_written > 0) {
      messages->erase(messages->begin(), messages->begin() + num_written);
    }
  }

  return success;
}

// Deletes the oldest file
// currently gets invoked from within a bufferstore
void FileStore::deleteOldest(struct tm* now) {

  int index = findOldestFile(makeBaseFilename(now));
  if (index < 0) {
    return;
  }
  shared_ptr<FileInterface> deletefile = FileInterface::createFileInterface(fsType,
                                            makeFullFilename(index, now));
  if (lostBytes_) {
    g_Handler->incCounter(categoryHandled, "bytes lost", lostBytes_);
    lostBytes_ = 0;
  }
  deletefile->deleteFile();
}

// Replace the messages in the oldest file at this timestamp with the input messages
bool FileStore::replaceOldest(boost::shared_ptr<logentry_vector_t> messages,
                              struct tm* now) {
  string base_name = makeBaseFilename(now);
  int index = findOldestFile(base_name);
  if (index < 0) {
    LOG_OPER("[%s] Could not find files <%s>", categoryHandled.c_str(), base_name.c_str());
    return false;
  }

  string filename = makeFullFilename(index, now);

  // Need to close and reopen store in case we already have this file open
  close();

  shared_ptr<FileInterface> infile = FileInterface::createFileInterface(fsType,
                                          filename, isBufferFile);

  // overwrite the old contents of the file
  bool success;
  if (infile->openTruncate()) {
    success = writeMessages(messages, infile);

  } else {
    LOG_OPER("[%s] Failed to open file <%s> for writing and truncate",
             categoryHandled.c_str(), filename.c_str());
    success = false;
  }

  // close this file and re-open store
  infile->close();
  open();

  return success;
}

bool FileStore::readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,
                           struct tm* now) {

  long loss;

  int index = findOldestFile(makeBaseFilename(now));
  if (index < 0) {
    // This isn't an error. It's legit to call readOldest when there aren't any
    // files left, in which case the call succeeds but returns messages empty.
    return true;
  }
  std::string filename = makeFullFilename(index, now);

  shared_ptr<FileInterface> infile = FileInterface::createFileInterface(fsType,
                                              filename, isBufferFile);

  if (!infile->openRead()) {
    LOG_OPER("[%s] Failed to open file <%s> for reading",
            categoryHandled.c_str(), filename.c_str());
    return false;
  }

  uint32_t bsize = 0;
  std::string message;
  while ((loss = infile->readNext(message)) > 0) {
    if (!message.empty()) {
      logentry_ptr_t entry = logentry_ptr_t(new LogEntry);

      // check whether a category is stored with the message
      if (writeCategory) {
        // get category without trailing \n
        entry->category = message.substr(0, message.length() - 1);

        if ((loss = infile->readNext(message)) <= 0) {
          LOG_OPER("[%s] category not stored with message <%s> "
              "corruption?, incompatible config change?",
              categoryHandled.c_str(), entry->category.c_str());
          break;
        }
      } else {
        entry->category = categoryHandled;
      }

      entry->message = message;

      messages->push_back(entry);
      bsize += entry->category.size();
      bsize += entry->message.size();
    }
  }
  if (loss < 0) {
    lostBytes_ = -loss;
  } else {
    lostBytes_ = 0;
  }
  infile->close();

  LOG_OPER("[%s] read <%lu> entries of <%d> bytes from file <%s>",
        categoryHandled.c_str(), messages->size(), bsize, filename.c_str());
  return true;
}

bool FileStore::empty(struct tm* now) {
  std::vector<std::string> files = FileInterface::list(filePath, fsType);

  std::string base_filename = makeBaseFilename(now);
  for (std::vector<std::string>::iterator iter = files.begin();
       iter != files.end();
       ++iter) {
    int suffix =  getFileSuffix(*iter, base_filename);
    if (-1 != suffix) {
      std::string fullname = makeFullFilename(suffix, now);
      shared_ptr<FileInterface> file = FileInterface::createFileInterface(fsType,
                                                                      fullname);
      if (file->fileSize()) {
        return false;
      }
    } // else it doesn't match the filename for this store
  }
  return true;

}


ThriftFileStore::ThriftFileStore(StoreQueue* storeq,
                                 const std::string& category,
                                 bool multi_category)
  : FileStoreBase(storeq, category, "thriftfile", multi_category),
    flushFrequencyMs(0),
    msgBufferSize(0),
    useSimpleFile(0) {
}

ThriftFileStore::~ThriftFileStore() {
}

shared_ptr<Store> ThriftFileStore::copy(const std::string &category) {
  ThriftFileStore *store = new ThriftFileStore(storeQueue, category, multiCategory);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->flushFrequencyMs = flushFrequencyMs;
  store->msgBufferSize = msgBufferSize;
  store->copyCommon(this);
  return copied;
}

bool ThriftFileStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  if (!isOpen()) {
    if (!open()) {
      return false;
    }
  }

  unsigned long messages_handled = 0;
  for (logentry_vector_t::iterator iter = messages->begin();
       iter != messages->end();
       ++iter) {

    // This length is an estimate -- what the ThriftLogFile actually writes is
    // a black box to us
    uint32_t length = (*iter)->message.size();

    try {
      thriftFileTransport->write(reinterpret_cast<const uint8_t*>((*iter)->message.data()), length);
      currentSize += length;
      ++eventsWritten;
      ++messages_handled;
    } catch (const TException& te) {
      LOG_OPER("[%s] Thrift file store failed to write to file: %s\n", categoryHandled.c_str(), te.what());
      setStatus("File write error");

      // If we already handled some messages, remove them from vector before
      // returning failure
      if (messages_handled) {
        messages->erase(messages->begin(), iter);
      }
      return false;
    }
  }
  // We can't wait until periodicCheck because we could be getting
  // a lot of data all at once in a failover situation
  if (currentSize > maxSize && maxSize != 0) {
    rotateFile();
  }

  return true;
}

bool ThriftFileStore::open() {
  return openInternal(true, NULL);
}

bool ThriftFileStore::isOpen() {
  return thriftFileTransport && thriftFileTransport->isOpen();
}

void ThriftFileStore::configure(pStoreConf configuration, pStoreConf parent) {
  FileStoreBase::configure(configuration, parent);
  configuration->getUnsigned("flush_frequency_ms", flushFrequencyMs);
  configuration->getUnsigned("msg_buffer_size", msgBufferSize);
  configuration->getUnsigned("use_simple_file", useSimpleFile);
}

void ThriftFileStore::close() {
  thriftFileTransport.reset();
}

void ThriftFileStore::flush() {
  // TFileTransport has its own periodic flushing mechanism, and we
  // introduce deadlocks if we try to call it from more than one place
  return;
}

bool ThriftFileStore::openInternal(bool incrementFilename, struct tm* current_time) {
  struct tm timeinfo;

  if (!current_time) {
    time_t rawtime = time(NULL);
    localtime_r(&rawtime, &timeinfo);
    current_time = &timeinfo;
  }
  int suffix;
  try {
    suffix = findNewestFile(makeBaseFilename(current_time));
  } catch(const std::exception& e) {
    LOG_OPER("Exception < %s > in ThriftFileStore::openInternal",
      e.what());
    return false;
  }

  if (incrementFilename) {
    ++suffix;
  }

  // this is the case where there's no file there and we're not incrementing
  if (suffix < 0) {
    suffix = 0;
  }

  string filename = makeFullFilename(suffix, current_time);
  /* try to create the directory containing the file */
  if (!createFileDirectory()) {
    LOG_OPER("[%s] Could not create path for file: %s",
             categoryHandled.c_str(), filename.c_str());
    return false;
  }

  switch (rollPeriod) {
    case ROLL_DAILY:
      lastRollTime = current_time->tm_mday;
      break;
    case ROLL_HOURLY:
      lastRollTime = current_time->tm_hour;
      break;
    case ROLL_OTHER:
      lastRollTime = time(NULL);
      break;
    case ROLL_NEVER:
      break;
  }


  try {
    if (useSimpleFile) {
      thriftFileTransport.reset(new TSimpleFileTransport(filename, false, true));
    } else {
      TFileTransport *transport = new TFileTransport(filename);
      thriftFileTransport.reset(transport);

      if (chunkSize != 0) {
	transport->setChunkSize(chunkSize);
      }
      if (flushFrequencyMs > 0) {
	transport->setFlushMaxUs(flushFrequencyMs * 1000);
      }
      if (msgBufferSize > 0) {
	transport->setEventBufferSize(msgBufferSize);
      }
    }

    LOG_OPER("[%s] Opened file <%s> for writing",
        categoryHandled.c_str(), filename.c_str());

    struct stat st;
    if (stat(filename.c_str(), &st) == 0) {
      currentSize = st.st_size;
    } else {
      currentSize = 0;
    }
    currentFilename = filename;
    eventsWritten = 0;
    setStatus("");
  } catch (const TException& te) {
    LOG_OPER("[%s] Failed to open file <%s> for writing: %s\n",
        categoryHandled.c_str(), filename.c_str(), te.what());
    setStatus("File open error");
    return false;
  }

  /* just make a best effort here, and don't error if it fails */
  if (createSymlink) {
    string symlinkName = makeFullSymlink();
    unlink(symlinkName.c_str());
    string symtarget = makeFullFilename(suffix, current_time, false);
    symlink(symtarget.c_str(), symlinkName.c_str());
  }

  return true;
}

bool ThriftFileStore::createFileDirectory () {
  try {
    boost::filesystem::create_directories(filePath);
  } catch(const std::exception& e) {
    LOG_OPER("Exception < %s > in ThriftFileStore::createFileDirectory for path %s",
      e.what(),filePath.c_str());
    return false;
  }
  return true;
}

BufferStore::BufferStore(StoreQueue* storeq,
                        const string& category,
                        bool multi_category)
  : Store(storeq, category, "buffer", multi_category),
    bufferSendRate(DEFAULT_BUFFERSTORE_SEND_RATE),
    avgRetryInterval(DEFAULT_BUFFERSTORE_AVG_RETRY_INTERVAL),
    retryIntervalRange(DEFAULT_BUFFERSTORE_RETRY_INTERVAL_RANGE),
    replayBuffer(true),
    adaptiveBackoff(false),
    minRetryInterval(DEFAULT_MIN_RETRY),
    maxRetryInterval(DEFAULT_MAX_RETRY),
    maxRandomOffset(DEFAULT_RANDOM_OFFSET_RANGE),
    retryInterval(DEFAULT_MIN_RETRY),
    numContSuccess(0),
    state(DISCONNECTED),
    flushStreaming(false),
    maxByPassRatio(DEFAULT_BUFFERSTORE_BYPASS_MAXQSIZE_RATIO) {

    lastOpenAttempt = time(NULL);

  // we can't open the client conection until we get configured
}

BufferStore::~BufferStore() {

}

void BufferStore::configure(pStoreConf configuration, pStoreConf parent) {
  Store::configure(configuration, parent);

  // Constructor defaults are fine if these don't exist
  configuration->getUnsigned("buffer_send_rate", (unsigned long&) bufferSendRate);

  // Used for linear backoff case
  configuration->getUnsigned("retry_interval",
                             (unsigned long&) avgRetryInterval);
  configuration->getUnsigned("retry_interval_range",
                             (unsigned long&) retryIntervalRange);

  // Used in case of adaptive backoff
  // max_random_offset should be some fraction of max_retry_interval
  // 20% of max_retry_interval should be a decent value
  // if you are using max_random_offset > max_retry_interval you should
  // probably not be using adaptive backoff and using retry_interval and
  // retry_interval_range parameters to do linear backoff instead
  configuration->getUnsigned("min_retry_interval",
                             (unsigned long&) minRetryInterval);
  configuration->getUnsigned("max_retry_interval",
                             (unsigned long&) maxRetryInterval);
  configuration->getUnsigned("max_random_offset",
                             (unsigned long&) maxRandomOffset);
  if (maxRandomOffset > maxRetryInterval) {
    LOG_OPER(" Warning max_random_offset > max_retry_interval look at using adaptive_backoff=no instead setting max_random_offset to max_retry_interval");
    maxRandomOffset = maxRetryInterval;
  }

  string tmp;
  if (configuration->getString("replay_buffer", tmp) && tmp != "yes") {
    replayBuffer = false;
  }

  if (configuration->getString("flush_streaming", tmp) && tmp == "yes") {
    flushStreaming = true;
  }

  if (configuration->getString("buffer_bypass_max_ratio", tmp)) {
    double d = strtod(tmp.c_str(), NULL);
    if (d > 0 && d <= 1) {
      maxByPassRatio = d;
    } else {
      LOG_OPER("[%s] Bad config - buffer_bypass_max_ratio <%s> range is (0, 1]",
          categoryHandled.c_str(), tmp.c_str());
    }
  }

  if (configuration->getString("adaptive_backoff", tmp) && tmp == "yes") {
    adaptiveBackoff = true;
  }

  if (retryIntervalRange > avgRetryInterval) {
    LOG_OPER("[%s] Bad config - retry_interval_range must be less than retry_interval. Using <%d> as range instead of <%d>",
             categoryHandled.c_str(), (int)avgRetryInterval,
             (int)retryIntervalRange);
    retryIntervalRange = avgRetryInterval;
  }
  if (minRetryInterval > maxRetryInterval) {
    LOG_OPER("[%s] Bad config - min_retry_interval must be less than max_retry_interval. Using <%d> and  <%d>, the default values instead",
             categoryHandled.c_str(), DEFAULT_MIN_RETRY, DEFAULT_MAX_RETRY );
    minRetryInterval = DEFAULT_MIN_RETRY;
    maxRetryInterval = DEFAULT_MAX_RETRY;
  }

  pStoreConf secondary_store_conf;
  if (!configuration->getStore("secondary", secondary_store_conf)) {
    string msg("Bad config - buffer store doesn't have secondary store");
    setStatus(msg);
    cout << msg << endl;
  } else {
    string type;
    if (!secondary_store_conf->getString("type", type)) {
      string msg("Bad config - buffer secondary store doesn't have a type");
      setStatus(msg);
      cout << msg << endl;
    } else {
      // If replayBuffer is true, then we need to create a readable store
      secondaryStore = createStore(storeQueue, type, categoryHandled,
                                   replayBuffer, multiCategory);
      secondaryStore->configure(secondary_store_conf, storeConf);
    }
  }

  pStoreConf primary_store_conf;
  if (!configuration->getStore("primary", primary_store_conf)) {
    string msg("Bad config - buffer store doesn't have primary store");
    setStatus(msg);
    cout << msg << endl;
  } else {
    string type;
    if (!primary_store_conf->getString("type", type)) {
      string msg("Bad config - buffer primary store doesn't have a type");
      setStatus(msg);
      cout << msg << endl;
    } else if (0 == type.compare("multi")) {
      // Cannot allow multistores in bufferstores as they can partially fail to
      // handle a message.  We cannot retry sending a messages that was
      // already handled by a subset of stores in the multistore.
      string msg("Bad config - buffer primary store cannot be multistore");
      setStatus(msg);
    } else {
      primaryStore = createStore(storeQueue, type, categoryHandled, false,
                                  multiCategory);
      primaryStore->configure(primary_store_conf, storeConf);
    }
  }

  // If the config is bad we'll still try to write the data to a
  // default location on local disk.
  if (!secondaryStore) {
    secondaryStore = createStore(storeQueue, "file", categoryHandled, true,
                                multiCategory);
  }
  if (!primaryStore) {
    primaryStore = createStore(storeQueue, "file", categoryHandled, false,
                               multiCategory);
  }
}

bool BufferStore::isOpen() {
  return primaryStore->isOpen() || secondaryStore->isOpen();
}

bool BufferStore::open() {

  // try to open the primary store, and set the state accordingly
  if (primaryStore->open()) {
    // in case there are files left over from a previous instance
    changeState(SENDING_BUFFER);

    // If we don't need to send buffers, skip to streaming
    if (!replayBuffer) {
      // We still switch state to SENDING_BUFFER first just to make sure we
      // can open the secondary store
      changeState(STREAMING);
    }
  } else {
    secondaryStore->open();
    changeState(DISCONNECTED);
  }

  return isOpen();
}

void BufferStore::close() {
  if (primaryStore->isOpen()) {
    primaryStore->flush();
    primaryStore->close();
  }
  if (secondaryStore->isOpen()) {
    secondaryStore->flush();
    secondaryStore->close();
  }
}

void BufferStore::flush() {
  if (primaryStore->isOpen()) {
    primaryStore->flush();
  }
  if (secondaryStore->isOpen()) {
    secondaryStore->flush();
  }
}

shared_ptr<Store> BufferStore::copy(const std::string &category) {
  BufferStore *store = new BufferStore(storeQueue, category, multiCategory);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->bufferSendRate = bufferSendRate;
  store->avgRetryInterval = avgRetryInterval;
  store->retryIntervalRange = retryIntervalRange;
  store->retryInterval = retryInterval;
  store->numContSuccess = numContSuccess;
  store->replayBuffer = replayBuffer;
  store->minRetryInterval = minRetryInterval;
  store->maxRetryInterval = maxRetryInterval;
  store->maxRandomOffset = maxRandomOffset;
  store->adaptiveBackoff = adaptiveBackoff;

  store->primaryStore = primaryStore->copy(category);
  store->secondaryStore = secondaryStore->copy(category);
  return copied;
}

bool BufferStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {

  if (state == STREAMING || (flushStreaming && state == SENDING_BUFFER)) {
    if (primaryStore->handleMessages(messages)) {
      if (adaptiveBackoff) {
        setNewRetryInterval(true);
      }
      return true;
    } else {
      changeState(DISCONNECTED);
    }
  }

  if (state != STREAMING) {
    // If this fails there's nothing else we can do here.
    return secondaryStore->handleMessages(messages);
  }

  return false;
}

// handles entry and exit conditions for states
void BufferStore::changeState(buffer_state_t new_state) {

  // leaving this state
  switch (state) {
  case STREAMING:
    secondaryStore->open();
    break;
  case DISCONNECTED:
    // Assume that if we are now able to leave the disconnected state, any
    // former warning has now been fixed.
    setStatus("");
    break;
  case SENDING_BUFFER:
    break;
  default:
    break;
  }

  // entering this state
  switch (new_state) {
  case STREAMING:
    if (secondaryStore->isOpen()) {
      secondaryStore->close();
    }
    break;
  case DISCONNECTED:
    // Do not set status here as it is possible to be in this frequently.
    // Whatever caused us to enter this state should have either set status
    // or chosen not to set status.
    g_Handler->incCounter(categoryHandled, "retries");
    setNewRetryInterval(false);
    lastOpenAttempt = time(NULL);
    if (!secondaryStore->isOpen()) {
      secondaryStore->open();
    }
    break;
  case SENDING_BUFFER:
    if (!secondaryStore->isOpen()) {
      secondaryStore->open();
    }
    break;
  default:
    break;
  }

  LOG_OPER("[%s] Changing state from <%s> to <%s>",
      categoryHandled.c_str(), stateAsString(state), stateAsString(new_state));
  state = new_state;
}

void BufferStore::periodicCheck() {

  // This class is responsible for checking its children
  primaryStore->periodicCheck();
  secondaryStore->periodicCheck();

  time_t now = time(NULL);
  struct tm nowinfo;
  localtime_r(&now, &nowinfo);

  if (state == DISCONNECTED) {
    if (now - lastOpenAttempt > retryInterval) {
      if (primaryStore->open()) {
        // Success.  Check if we need to send buffers from secondary to primary
        if (replayBuffer) {
          changeState(SENDING_BUFFER);
        } else {
          changeState(STREAMING);
        }
      } else {
        // this resets the retry timer
        changeState(DISCONNECTED);
      }
    }
  }

  // send data in case of backup
  if (state == SENDING_BUFFER) {
    // if queue size is getting large return so that there is time to forward
    // incoming messages directly to the primary store without buffering to
    // secondary store.
    if (flushStreaming) {
      uint64_t qsize = storeQueue->getSize();
      if(qsize >=
          maxByPassRatio * g_Handler->getMaxQueueSize()) {
        return;
      }
    }

    // Read a group of messages from the secondary store and send them to
    // the primary store. Note that the primary store could tell us to try
    // again later, so this isn't very efficient if it reads too many
    // messages at once. (if the secondary store is a file, the number of
    // messages read is controlled by the max file size)
    // parameter max_size for filestores in the configuration
    unsigned sent = 0;
    try {
      for (sent = 0; sent < bufferSendRate; ++sent) {
        boost::shared_ptr<logentry_vector_t> messages(new logentry_vector_t);
        // Reads come complete buffered file
        // this file size is controlled by max_size in the configuration
        if (secondaryStore->readOldest(messages, &nowinfo)) {

          unsigned long size = messages->size();
          if (size) {
            if (primaryStore->handleMessages(messages)) {
              secondaryStore->deleteOldest(&nowinfo);
              if (adaptiveBackoff) {
                setNewRetryInterval(true);
              }
            } else {

              if (messages->size() != size) {
                // We were only able to process some, but not all of this batch
                // of messages.  Replace this batch of messages with
                // just the messages that were not processed.
                LOG_OPER("[%s] buffer store primary store processed %lu/%lu messages",
                    categoryHandled.c_str(), size - messages->size(), size);

                // Put back un-handled messages
                if (!secondaryStore->replaceOldest(messages, &nowinfo)) {
                  // Nothing we can do but try to remove oldest messages and
                  // report a loss
                  LOG_OPER("[%s] buffer store secondary store lost %lu messages",
                      categoryHandled.c_str(), messages->size());
                  g_Handler->incCounter(categoryHandled, "lost", messages->size());
                  secondaryStore->deleteOldest(&nowinfo);
                }
              }
              changeState(DISCONNECTED);
              break;
            }
          }  else {
            // else it's valid for read to not find anything but not error
            secondaryStore->deleteOldest(&nowinfo);
          }
        } else {
          // This is bad news. We'll stay in the sending state
          // and keep trying to read.
          setStatus("Failed to read from secondary store");
          LOG_OPER("[%s] WARNING: buffer store can't read from secondary store",
              categoryHandled.c_str());
          break;
        }

        if (secondaryStore->empty(&nowinfo)) {
          LOG_OPER("[%s] No more buffer files to send, switching to streaming mode",
              categoryHandled.c_str());
          changeState(STREAMING);

          break;
        }
      }
    } catch(const std::exception& e) {
      LOG_OPER("[%s] Failed in secondary to primary transfer ",
          categoryHandled.c_str());
      LOG_OPER("Exception: %s", e.what());
      setStatus("bufferstore sending_buffer failure");
      changeState(DISCONNECTED);
    }
  }// if state == SENDING_BUFFER
}

/*
 * This functions sets a new time interval after which the buffer store
 * will retry connecting to primary. There are two modes based on the
 * config parameter 'adaptive_backoff'.
 *
 *
 * When adaptive_backoff=yes this function uses an Additive Increase and
 * Multiplicative Decrease strategy which is commonly used in networking
 * protocols for congestion avoidance, while achieving fairness and good
 * throughput for multiple senders.
 * The algorithm works as follows. Whenever the buffer store is able to
 * achieve CONT_SUCCESS_THRESHOLD continuous successful sends to the
 * primary store its retry interval is decreased by ADD_DEC_FACTOR.
 * Whenever the buffer store fails to send to primary its retry interval
 * is increased by multiplying a MULT_INC_FACTOR to it. To avoid thundering
 * herds problems a random offset is added to this new retry interval
 * controlled by 'max_random_offset' config parameter.
 * The range of the retry interval is controlled by config parameters
 * 'min_retry_interval' and 'max_retry_interval'.
 * Currently CONT_SUCCESS_THRESHOLD, ADD_DEC_FACTOR and MULT_INC_FACTOR
 * are not config parameters. This can be done later if need be.
 *
 *
 * In case adaptive_backoff=no, the new retry interval is calculated
 * using the config parameters 'avg_retry_interval' and
 * 'retry_interval_range'
 */
void BufferStore::setNewRetryInterval(bool success) {

  if (adaptiveBackoff) {
    time_t prevRetryInterval = retryInterval;
    if (success) {
      numContSuccess++;
      if (numContSuccess >= CONT_SUCCESS_THRESHOLD) {
        if (retryInterval > ADD_DEC_FACTOR) {
          retryInterval -= ADD_DEC_FACTOR;
        }
        else {
          retryInterval = minRetryInterval;
        }
        if (retryInterval < minRetryInterval) {
          retryInterval = minRetryInterval;
        }
        numContSuccess = 0;
      }
      else {
        return;
      }
    }
    else {
      retryInterval = static_cast <time_t> (retryInterval*MULT_INC_FACTOR);
      retryInterval += (rand() % maxRandomOffset);
      if (retryInterval > maxRetryInterval) {
        retryInterval = maxRetryInterval;
      }
      numContSuccess = 0;
    }
    // prevent unnecessary prints
    if (prevRetryInterval == retryInterval) {
      return;
    }
  }
  else {
    retryInterval = avgRetryInterval - retryIntervalRange/2
                    + rand() % retryIntervalRange;
  }
  LOG_OPER("[%s] choosing new retry interval <%lu> seconds",
           categoryHandled.c_str(),
           (unsigned long) retryInterval);
}

const char* BufferStore::stateAsString(buffer_state_t state) {
switch (state) {
  case STREAMING:
    return "STREAMING";
  case DISCONNECTED:
    return "DISCONNECTED";
  case SENDING_BUFFER:
    return "SENDING_BUFFER";
  default:
    return "unknown state";
  }
}

std::string BufferStore::getStatus() {

  // This order is intended to give precedence to the errors
  // that are likely to be the worst. We can handle a problem
  // with the primary store, but not the secondary.
  std::string return_status = secondaryStore->getStatus();
  if (return_status.empty()) {
    return_status = Store::getStatus();
  }
  if (return_status.empty()) {
    return_status = primaryStore->getStatus();
  }
  return return_status;
}


NetworkStore::NetworkStore(StoreQueue* storeq,
                          const string& category,
                          bool multi_category)
  : Store(storeq, category, "network", multi_category),
    useConnPool(false),
    serviceBased(false),
    remotePort(0),
    serviceCacheTimeout(DEFAULT_NETWORKSTORE_CACHE_TIMEOUT),
    ignoreNetworkError(false),
    configmod(NULL),
    opened(false),
    lastServiceCheck(0) {
  // we can't open the connection until we get configured

  // the bool for opened ensures that we don't make duplicate
  // close calls, which would screw up the connection pool's
  // reference counting.
}

NetworkStore::~NetworkStore() {
  close();
}

void NetworkStore::configure(pStoreConf configuration, pStoreConf parent) {
  Store::configure(configuration, parent);
  // Error checking is done on open()
  // service takes precedence over host + port
  if (configuration->getString("smc_service", serviceName)) {
    serviceBased = true;

    // Constructor defaults are fine if these don't exist
    configuration->getString("service_options", serviceOptions);
    configuration->getUnsigned("service_cache_timeout", serviceCacheTimeout);
  } else {
    serviceBased = false;
    configuration->getString("remote_host", remoteHost);
    configuration->getUnsigned("remote_port", remotePort);
  }

  if (!configuration->getInt("timeout", timeout)) {
    timeout = DEFAULT_SOCKET_TIMEOUT_MS;
  }

  string temp;
  if (configuration->getString("use_conn_pool", temp)) {
    if (0 == temp.compare("yes")) {
      useConnPool = true;
    }
  }
  if (configuration->getString("ignore_network_error", temp)) {
    if (0 == temp.compare("yes")) {
      ignoreNetworkError = true;
    }
  }

  // if this network store dynamic configured?
  // get network dynamic updater parameters
  string dynamicType;
  if (configuration->getString("dynamic_config_type", dynamicType)) {
    // get dynamic config module
    configmod = getNetworkDynamicConfigMod(dynamicType.c_str());
    if (configmod) {
      if (!configmod->isConfigValidFunc(categoryHandled, configuration.get())) {
        LOG_OPER("[%s] dynamic network configuration is not valid.",
                categoryHandled.c_str());
        configmod = NULL;
      } else {
        // set remote host port
        string host;
        uint32_t port;
        if (configmod->getHostFunc(categoryHandled, storeConf.get(), host, port)) {
          remoteHost = host;
          remotePort = port;
          LOG_OPER("[%s] dynamic configred network store destination configured:<%s:%lu>",
            categoryHandled.c_str(), remoteHost.c_str(), remotePort);
        }
      }
    } else {
      LOG_OPER("[%s] dynamic network configuration is not valid. Unable to find network dynamic configuration module with name <%s>",
                categoryHandled.c_str(), dynamicType.c_str());
    }
  }
}

void NetworkStore::periodicCheck() {
  if (configmod) {
    // get the network updater type
    string host;
    uint32_t port;
    bool success = configmod->getHostFunc(categoryHandled, storeConf.get(), host, port);
    if (success && (host != remoteHost || port != remotePort)) {
      // if it is different from the current configuration
      // then close and open again
      LOG_OPER("[%s] dynamic configred network store destination changed. old value:<%s:%lu>, new value:<%s:%lu>",
               categoryHandled.c_str(), remoteHost.c_str(), remotePort,
               host.c_str(), (long unsigned)port);
      remoteHost = host;
      remotePort = port;
      close();
    }
  }
}

bool NetworkStore::open() {
  if (isOpen()) {
    /* re-opening an already open NetworkStore can be bad. For example,
     * it can lead to bad reference counting on g_connpool connections
     */
    return (true);
  }
  if (serviceBased) {
    bool success = true;
    time_t now = time(NULL);

    // Only get list of servers if we haven't already gotten them recently
    if (lastServiceCheck <= (time_t) (now - serviceCacheTimeout)) {
      lastServiceCheck = now;

      success = scribe::network_config::getService(serviceName, serviceOptions,
                                                   servers);
    }

    // Cannot open if we couldn't find any servers
    if (!success || servers.empty()) {
      LOG_OPER("[%s] Failed to get servers from service", categoryHandled.c_str());
      setStatus("Could not get list of servers from service");
      return false;
    }

    if (useConnPool) {
      opened = g_connPool.open(serviceName, servers, static_cast<int>(timeout));
    } else {
      if (unpooledConn != NULL) {
        LOG_OPER("Logic error: NetworkStore::open unpooledConn is not NULL"
            " service = %s", serviceName.c_str());
      }
      unpooledConn = shared_ptr<scribeConn>(new scribeConn(serviceName,
            servers, static_cast<int>(timeout)));
      opened = unpooledConn->open();
      if (!opened) {
        unpooledConn.reset();
      }
    }

  } else if (remotePort <= 0 || remoteHost.empty()) {
    LOG_OPER("[%s] Bad config - won't attempt to connect to <%s:%lu>",
        categoryHandled.c_str(), remoteHost.c_str(), remotePort);
    setStatus("Bad config - invalid location for remote server");
    return false;
  } else {
    if (useConnPool) {
      opened = g_connPool.open(remoteHost, remotePort,
          static_cast<int>(timeout));
    } else {
      // only open unpooled connection if not already open
      if (unpooledConn != NULL) {
        LOG_OPER("Logic error: NetworkStore::open unpooledConn is not NULL"
            " %s:%lu", remoteHost.c_str(), remotePort);
      }
      unpooledConn = shared_ptr<scribeConn>(new scribeConn(remoteHost,
          remotePort, static_cast<int>(timeout)));
      opened = unpooledConn->open();
      if (!opened) {
        unpooledConn.reset();
      }
    }
  }

  if (opened || ignoreNetworkError) {
    // clear status on success or if we should not signal error here
    setStatus("");
  } else {
    setStatus("Failed to connect");
  }
  return opened;
}

void NetworkStore::close() {
  if (!opened) {
    return;
  }
  opened = false;
  if (useConnPool) {
    if (serviceBased) {
      g_connPool.close(serviceName);
    } else {
      g_connPool.close(remoteHost, remotePort);
    }
  } else {
    if (unpooledConn != NULL) {
      unpooledConn->close();
    }
    unpooledConn.reset();
  }
}

bool NetworkStore::isOpen() {
  return opened;
}

shared_ptr<Store> NetworkStore::copy(const std::string &category) {
  NetworkStore *store = new NetworkStore(storeQueue, category, multiCategory);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->useConnPool = useConnPool;
  store->serviceBased = serviceBased;
  store->timeout = timeout;
  store->remoteHost = remoteHost;
  store->remotePort = remotePort;
  store->serviceName = serviceName;

  return copied;
}


// If the size of messages is greater than a threshold
// first try sending an empty vector to catch dfqs
bool
NetworkStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  int ret;

  if (!isOpen()) {
    if (!open()) {
    LOG_OPER("[%s] Could not open NetworkStore in handleMessages",
             categoryHandled.c_str());
    return false;
    }
  }

  bool tryDummySend = shouldSendDummy(messages);
  boost::shared_ptr<logentry_vector_t> dummymessages(new logentry_vector_t);

  if (useConnPool) {
    if (serviceBased) {
      if (!tryDummySend ||
          ((ret = g_connPool.send(serviceName, dummymessages)) == CONN_OK)) {
        ret = g_connPool.send(serviceName, messages);
      }
    } else {
      if (!tryDummySend ||
          (ret = g_connPool.send(remoteHost, remotePort, dummymessages)) ==
          CONN_OK) {
        ret = g_connPool.send(remoteHost, remotePort, messages);
      }
    }
  } else if (unpooledConn) {
    if (!tryDummySend ||
        ((ret = unpooledConn->send(dummymessages)) == CONN_OK)) {
      ret = unpooledConn->send(messages);
    }
  } else {
    ret = CONN_FATAL;
    LOG_OPER("[%s] Logic error: NetworkStore::handleMessages unpooledConn "
        "is NULL", categoryHandled.c_str());
  }
  if (ret == CONN_FATAL) {
    close();
  }
  return (ret == CONN_OK);
}

void NetworkStore::flush() {
  // Nothing to do
}

BucketStore::BucketStore(StoreQueue* storeq,
                        const string& category,
                        bool multi_category)
  : Store(storeq, category, "bucket", multi_category),
    bucketType(context_log),
    delimiter(DEFAULT_BUCKETSTORE_DELIMITER),
    removeKey(false),
    opened(false),
    bucketRange(0),
    numBuckets(1) {
}

BucketStore::~BucketStore() {

}

// Given a single bucket definition, create multiple buckets
void BucketStore::createBucketsFromBucket(pStoreConf configuration,
    pStoreConf bucket_conf) {
  string error_msg, bucket_subdir, type, path, failure_bucket;
  bool needs_bucket_subdir = false;
  unsigned long bucket_offset = 0;
  pStoreConf tmp;

  // check for extra bucket definitions
  if (configuration->getStore("bucket0", tmp) ||
      configuration->getStore("bucket1", tmp)) {
    error_msg = "bucket store has too many buckets defined";
    goto handle_error;
  }

  bucket_conf->getString("type", type);
  if (type != "file" && type != "thriftfile") {
    error_msg = "store contained in a bucket store must have a type of ";
    error_msg += "either file or thriftfile if not defined explicitely";
    goto handle_error;
  }

  needs_bucket_subdir = true;
  if (!configuration->getString("bucket_subdir", bucket_subdir)) {
    error_msg =
      "bucketizer containing file stores must have a bucket_subdir";
    goto handle_error;
  }
  if (!bucket_conf->getString("file_path", path)) {
    error_msg =
      "file store contained by bucketizer must have a file_path";
    goto handle_error;
  }

  // set starting bucket number if specified
  configuration->getUnsigned("bucket_offset", bucket_offset);

  // check if failure bucket was given a different name
  configuration->getString("failure_bucket", failure_bucket);

  // We actually create numBuckets + 1 stores. Messages are normally
  // hashed into buckets 1 through numBuckets, and messages that can't
  // be hashed are put in bucket 0.

  for (unsigned int i = 0; i <= numBuckets; ++i) {

    shared_ptr<Store> newstore =
      createStore(storeQueue, type, categoryHandled, false, multiCategory);

    if (!newstore) {
      error_msg = "can't create store of type: ";
      error_msg += type;
      goto handle_error;
    }

    // For file/thrift file buckets, create unique filepath for each bucket
    if (needs_bucket_subdir) {
      if (i == 0 && !failure_bucket.empty()) {
        bucket_conf->setString("file_path", path + '/' + failure_bucket);
      } else {
        // the bucket number is appended to the file path
        unsigned int bucket_id = i + bucket_offset;

        ostringstream oss;
        oss << path << '/' << bucket_subdir << setw(3) << setfill('0')
            << bucket_id;
        bucket_conf->setString("file_path", oss.str());
      }
    }

    buckets.push_back(newstore);
    newstore->configure(bucket_conf, storeConf);
  }

  return;

handle_error:
  setStatus(error_msg);
  LOG_OPER("[%s] Bad config - %s", categoryHandled.c_str(),
           error_msg.c_str());
  numBuckets = 0;
  buckets.clear();
}

// Checks for a bucket definition for every bucket from 0 to numBuckets
// and configures each bucket
void BucketStore::createBuckets(pStoreConf configuration) {
  string error_msg, tmp_string;
  pStoreConf tmp;
  unsigned long i;

  if (configuration->getString("bucket_subdir", tmp_string)) {
    error_msg = "cannot have bucket_subdir when defining multiple buckets";
    goto handle_error;
  }

  if (configuration->getString("bucket_offset", tmp_string)) {
    error_msg = "cannot have bucket_offset when defining multiple buckets";
    goto handle_error;
  }

  if (configuration->getString("failure_bucket", tmp_string)) {
    error_msg = "cannot have failure_bucket when defining multiple buckets";
    goto handle_error;
  }

  // Configure stores named 'bucket0, bucket1, bucket2, ... bucket{numBuckets}
  for (i = 0; i <= numBuckets; i++) {
    pStoreConf   bucket_conf;
    string       type, bucket_name;
    stringstream ss;

    ss << "bucket" << i;
    bucket_name = ss.str();

    if (!configuration->getStore(bucket_name, bucket_conf)) {
      error_msg = "could not find bucket definition for " + bucket_name;
      goto handle_error;
    }

    if (!bucket_conf->getString("type", type)) {
      error_msg = "store contained in a bucket store must have a type";
      goto handle_error;
    }

    shared_ptr<Store> bucket =
      createStore(storeQueue, type, categoryHandled, false, multiCategory);

    buckets.push_back(bucket);
    //add bucket id configuration
    bucket_conf->setUnsigned("bucket_id", i);
    bucket_conf->setUnsigned("network::bucket_id", i);
    bucket_conf->setUnsigned("file::bucket_id", i);
    bucket_conf->setUnsigned("thriftfile::bucket_id", i);
    bucket_conf->setUnsigned("buffer::bucket_id", i);
    bucket->configure(bucket_conf, storeConf);
  }

  // Check if an extra bucket is defined
  if (configuration->getStore("bucket" + (numBuckets + 1), tmp)) {
    error_msg = "bucket store has too many buckets defined";
    goto handle_error;
  }

  return;

handle_error:
  setStatus(error_msg);
  LOG_OPER("[%s] Bad config - %s", categoryHandled.c_str(),
           error_msg.c_str());
  numBuckets = 0;
  buckets.clear();
}

/**
   * Buckets in a bucket store can be defined explicitly or implicitly:
   *
   * #Explicitly
   * <store>
   *   type=bucket
   *   num_buckets=2
   *   bucket_type=key_hash
   *
   *   <bucket0>
   *     ...
   *   </bucket0>
   *
   *   <bucket1>
   *     ...
   *   </bucket1>
   *
   *   <bucket2>
   *     ...
   *   </bucket2>
   * </store>
   *
   * #Implicitly
   * <store>
   *   type=bucket
   *   num_buckets=2
   *   bucket_type=key_hash
   *
   *   <bucket>
   *     ...
   *   </bucket>
   * </store>
   */
void BucketStore::configure(pStoreConf configuration, pStoreConf parent) {
  Store::configure(configuration, parent);

  string error_msg, bucketizer_str, remove_key_str;
  unsigned long delim_long = 0;
  pStoreConf bucket_conf;
  //set this to true for bucket types that have a delimiter
  bool need_delimiter = false;

  configuration->getString("bucket_type", bucketizer_str);

  // Figure out th bucket type from the bucketizer string
  if (0 == bucketizer_str.compare("context_log")) {
    bucketType = context_log;
  } else if (0 == bucketizer_str.compare("random")) {
      bucketType = random;
  } else if (0 == bucketizer_str.compare("key_hash")) {
    bucketType = key_hash;
    need_delimiter = true;
  } else if (0 == bucketizer_str.compare("key_modulo")) {
    bucketType = key_modulo;
    need_delimiter = true;
  } else if (0 == bucketizer_str.compare("key_range")) {
    bucketType = key_range;
    need_delimiter = true;
    configuration->getUnsigned("bucket_range", bucketRange);

    if (bucketRange == 0) {
      LOG_OPER("[%s] config warning - bucket_range is 0",
               categoryHandled.c_str());
    }
  }

  // This is either a key_hash or key_modulo, not context log, figure out the delimiter and store it
  if (need_delimiter) {
    configuration->getUnsigned("delimiter", delim_long);
    if (delim_long > 255) {
      LOG_OPER("[%s] config warning - delimiter is too large to fit in a char, using default", categoryHandled.c_str());
      delimiter = DEFAULT_BUCKETSTORE_DELIMITER;
    } else if (delim_long == 0) {
      LOG_OPER("[%s] config warning - delimiter is zero, using default", categoryHandled.c_str());
      delimiter = DEFAULT_BUCKETSTORE_DELIMITER;
    } else {
      delimiter = (char)delim_long;
    }
  }

  // Optionally remove the key and delimiter of each message before bucketizing
  configuration->getString("remove_key", remove_key_str);
  if (remove_key_str == "yes") {
    removeKey = true;

    if (bucketType == context_log) {
      error_msg =
        "Bad config - bucketizer store of type context_log do not support remove_key";
      goto handle_error;
    }
  }

  if (!configuration->getUnsigned("num_buckets", numBuckets)) {
    error_msg = "Bad config - bucket store must have num_buckets";
    goto handle_error;
  }

  // Buckets can be defined explicitely or by specifying a single "bucket"
  if (configuration->getStore("bucket", bucket_conf)) {
    createBucketsFromBucket(configuration, bucket_conf);
  } else {
    createBuckets(configuration);
  }

  return;

handle_error:
  setStatus(error_msg);
  LOG_OPER("[%s] %s", categoryHandled.c_str(), error_msg.c_str());
  numBuckets = 0;
  buckets.clear();
}

bool BucketStore::open() {
  // we have one extra bucket for messages we can't hash
  if (numBuckets <= 0 || buckets.size() != numBuckets + 1) {
    LOG_OPER("[%s] Can't open bucket store with <%d> of <%lu> buckets", categoryHandled.c_str(), (int)buckets.size(), numBuckets);
    return false;
  }

  for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
       iter != buckets.end();
       ++iter) {

    if (!(*iter)->open()) {
      close();
      opened = false;
      return false;
    }
  }
  opened = true;
  return true;
}

bool BucketStore::isOpen() {
  return opened;
}

void BucketStore::close() {
  // don't check opened, because we can call this when some, but
  // not all, contained stores are opened. Calling close on a contained
  // store that's already closed shouldn't hurt anything.
  for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
       iter != buckets.end();
       ++iter) {
    (*iter)->close();
  }
  opened = false;
}

void BucketStore::flush() {
  for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
       iter != buckets.end();
       ++iter) {
    (*iter)->flush();
  }
}

string BucketStore::getStatus() {

  string retval = Store::getStatus();

  std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
  while (retval.empty() && iter != buckets.end()) {
    retval = (*iter)->getStatus();
    ++iter;
  }
  return retval;
}

// Call periodicCheck on all containing stores
void BucketStore::periodicCheck() {
  // Call periodic check on all bucket stores in a random order
  uint32_t sz = buckets.size();
  vector<uint32_t> storeIndex(sz);
  for (uint32_t i = 0; i < sz; ++i) {
    storeIndex[i] = i;
  }
  random_shuffle(storeIndex.begin(), storeIndex.end());

  for (uint32_t i = 0; i < sz; ++i) {
    uint32_t idx = storeIndex[i];
    buckets[idx]->periodicCheck();
  }
}

shared_ptr<Store> BucketStore::copy(const std::string &category) {
  BucketStore *store = new BucketStore(storeQueue, category, multiCategory);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->numBuckets = numBuckets;
  store->bucketType = bucketType;
  store->delimiter = delimiter;

  for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
       iter != buckets.end();
       ++iter) {
    store->buckets.push_back((*iter)->copy(category));
  }

  return copied;
}

/*
 * Bucketize <messages> and try to send to each contained bucket store
 * At the end of the function <messages> will contain all the messages that
 * could not be processed
 * Returns true if all messages were successfully sent, false otherwise.
 */
bool BucketStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  bool success = true;

  boost::shared_ptr<logentry_vector_t> failed_messages(new logentry_vector_t);
  vector<shared_ptr<logentry_vector_t> > bucketed_messages;
  bucketed_messages.resize(numBuckets + 1);

  if (numBuckets == 0) {
    LOG_OPER("[%s] Failed to write - no buckets configured",
             categoryHandled.c_str());
    setStatus("Failed write to bucket store");
    return false;
  }

  // batch messages by bucket
  for (logentry_vector_t::iterator iter = messages->begin();
       iter != messages->end();
       ++iter) {
    unsigned bucket = bucketize((*iter)->message);

    if (!bucketed_messages[bucket]) {
      bucketed_messages[bucket] =
        shared_ptr<logentry_vector_t> (new logentry_vector_t);
    }

    bucketed_messages[bucket]->push_back(*iter);
  }

  // handle all batches of messages
  for (unsigned long i = 0; i <= numBuckets; i++) {
    shared_ptr<logentry_vector_t> batch = bucketed_messages[i];

    if (batch) {

      if (removeKey) {
        // Create new set of messages with keys removed
        shared_ptr<logentry_vector_t> key_removed =
          shared_ptr<logentry_vector_t> (new logentry_vector_t);

        for (logentry_vector_t::iterator iter = batch->begin();
             iter != batch->end();
             ++iter) {
          logentry_ptr_t entry = logentry_ptr_t(new LogEntry);
          entry->category = (*iter)->category;
          entry->message = getMessageWithoutKey((*iter)->message);
          key_removed->push_back(entry);
        }
        batch = key_removed;
      }

      if (!buckets[i]->handleMessages(batch)) {
        // keep track of messages that were not handled
        failed_messages->insert(failed_messages->end(),
                                bucketed_messages[i]->begin(),
                                bucketed_messages[i]->end());
        success = false;
      }
    }
  }

  if (!success) {
    // return failed logentrys in messages
    messages->swap(*failed_messages);
  }

  return success;
}

// Return the bucket number a message must be put into
unsigned long BucketStore::bucketize(const std::string& message) {

  string::size_type length = message.length();

  if (bucketType == context_log) {
    // the key is in ascii after the third delimiter
    char delim = 1;
    string::size_type pos = 0;
    for (int i = 0; i < 3; ++i) {
      pos = message.find(delim, pos);
      if (pos == string::npos || length <= pos + 1) {
        return 0;
      }
      ++pos;
    }
    if (message[pos] == delim) {
      return 0;
    }

    uint32_t id = strtoul(message.substr(pos).c_str(), NULL, 10);
    if (id == 0) {
      return 0;
    }

    if (numBuckets == 0) {
      return 0;
    } else {
      return (scribe::integerhash::hash32(id) % numBuckets) + 1;
    }
  } else if (bucketType == random) {
    // return any random bucket
    return (rand() % numBuckets) + 1;
  } else {
    // just hash everything before the first user-defined delimiter
    string::size_type pos = message.find(delimiter);
    if (pos == string::npos) {
      // if no delimiter found, write to bucket 0
      return 0;
    }

    string key = message.substr(0, pos).c_str();
    if (key.empty()) {
      // if no key found, write to bucket 0
      return 0;
    }

    if (numBuckets == 0) {
      return 0;
    } else {
      switch (bucketType) {
        case key_modulo:
          // No hashing, just simple modulo
          return (atol(key.c_str()) % numBuckets) + 1;
          break;
        case key_range:
          if (bucketRange == 0) {
            return 0;
          } else {
            // Calculate what bucket this key would fall into if we used
            // bucket_range to compute the modulo
           double key_mod = atol(key.c_str()) % bucketRange;
           return (unsigned long) ((key_mod / bucketRange) * numBuckets) + 1;
          }
          break;
        case key_hash:
        default:
          // Hashing by default.
          return (scribe::strhash::hash32(key.c_str()) % numBuckets) + 1;
          break;
      }
    }
  }

  return 0;
}

string BucketStore::getMessageWithoutKey(const std::string& message) {
  string::size_type pos = message.find(delimiter);

  if (pos == string::npos) {
    return message;
  }

  return message.substr(pos+1);
}


NullStore::NullStore(StoreQueue* storeq,
                     const std::string& category,
                     bool multi_category)
  : Store(storeq, category, "null", multi_category)
{}

NullStore::~NullStore() {
}

boost::shared_ptr<Store> NullStore::copy(const std::string &category) {
  NullStore *store = new NullStore(storeQueue, category, multiCategory);
  shared_ptr<Store> copied = shared_ptr<Store>(store);
  return copied;
}

bool NullStore::open() {
  return true;
}

bool NullStore::isOpen() {
  return true;
}

void NullStore::configure(pStoreConf configuration, pStoreConf parent) {
  Store::configure(configuration, parent);
}

void NullStore::close() {
}

bool NullStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  g_Handler->incCounter(categoryHandled, "ignored", messages->size());
  return true;
}

void NullStore::flush() {
}

bool NullStore::readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,
                       struct tm* now) {
  return true;
}

bool NullStore::replaceOldest(boost::shared_ptr<logentry_vector_t> messages,
                              struct tm* now) {
  return true;
}

void NullStore::deleteOldest(struct tm* now) {
}

bool NullStore::empty(struct tm* now) {
  return true;
}

MultiStore::MultiStore(StoreQueue* storeq,
                      const std::string& category,
                      bool multi_category)
  : Store(storeq, category, "multi", multi_category) {
}

MultiStore::~MultiStore() {
}

boost::shared_ptr<Store> MultiStore::copy(const std::string &category) {
  MultiStore *store = new MultiStore(storeQueue, category, multiCategory);
  store->report_success = this->report_success;
  boost::shared_ptr<Store> tmp_copy;
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    tmp_copy = (*iter)->copy(category);
    store->stores.push_back(tmp_copy);
  }

  return shared_ptr<Store>(store);
}

bool MultiStore::open() {
  bool all_result = true;
  bool any_result = false;
  bool cur_result;
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    cur_result = (*iter)->open();
    any_result |= cur_result;
    all_result &= cur_result;
  }
  return (report_success == SUCCESS_ALL) ? all_result : any_result;
}

bool MultiStore::isOpen() {
  bool all_result = true;
  bool any_result = false;
  bool cur_result;
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    cur_result = (*iter)->isOpen();
    any_result |= cur_result;
    all_result &= cur_result;
  }
  return (report_success == SUCCESS_ALL) ? all_result : any_result;
}

void MultiStore::configure(pStoreConf configuration, pStoreConf parent) {
  Store::configure(configuration, parent);
  /**
   * in this store, we look for other numbered stores
   * in the following fashion:
   * <store>
   *   type=multi
   *   report_success=all|any
   *   <store0>
   *     ...
   *   </store0>
       ...
   *   <storen>
   *     ...
   *   </storen>
   * </store>
   */
  pStoreConf cur_conf;
  string cur_type;
  boost::shared_ptr<Store> cur_store;
  string report_preference;

  // find reporting preference
  if (configuration->getString("report_success", report_preference)) {
    if (0 == report_preference.compare("all")) {
      report_success = SUCCESS_ALL;
      LOG_OPER("[%s] MULTI: Logging success only if all stores succeed.",
               categoryHandled.c_str());
    } else if (0 == report_preference.compare("any")) {
      report_success = SUCCESS_ANY;
      LOG_OPER("[%s] MULTI: Logging success if any store succeeds.",
               categoryHandled.c_str());
    } else {
      LOG_OPER("[%s] MULTI: %s is an invalid value for report_success.",
               categoryHandled.c_str(), report_preference.c_str());
      setStatus("MULTI: Invalid report_success value.");
      return;
    }
  } else {
    report_success = SUCCESS_ALL;
  }

  // find stores
  for (int i=0; ;++i) {
    stringstream ss;
    ss << "store" << i;
    if (!configuration->getStore(ss.str(), cur_conf)) {
      // allow this to be 0 or 1 indexed
      if (i == 0) {
        continue;
      }

      // no store for this id? we're finished.
      break;
    } else {
      // find this store's type
      if (!cur_conf->getString("type", cur_type)) {
        LOG_OPER("[%s] MULTI: Store %d is missing type.", categoryHandled.c_str(), i);
        setStatus("MULTI: Store is missing type.");
        return;
      } else {
        // add it to the list
        cur_store = createStore(storeQueue, cur_type, categoryHandled, false,
                                multiCategory);
        LOG_OPER("[%s] MULTI: Configured store of type %s successfully.",
                 categoryHandled.c_str(), cur_type.c_str());
        cur_store->configure(cur_conf, storeConf);
        stores.push_back(cur_store);
      }
    }
  }

  if (stores.size() == 0) {
    setStatus("MULTI: No stores found, invalid store.");
    LOG_OPER("[%s] MULTI: No stores found, invalid store.", categoryHandled.c_str());
  }
}

void MultiStore::close() {
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    (*iter)->close();
  }
}

bool MultiStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  bool all_result = true;
  bool any_result = false;
  bool cur_result;
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    cur_result = (*iter)->handleMessages(messages);
    any_result |= cur_result;
    all_result &= cur_result;
  }

  // We cannot accurately report the number of messages not handled as messages
  // can be partially handled by a subset of stores.  So a multistore failure
  // will over-record the number of lost messages.
  return (report_success == SUCCESS_ALL) ? all_result : any_result;
}

// Call periodicCheck on all contained stores
void MultiStore::periodicCheck() {
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    (*iter)->periodicCheck();
  }
}

void MultiStore::flush() {
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    (*iter)->flush();
  }
}

CategoryStore::CategoryStore(StoreQueue* storeq,
                             const std::string& category,
                             bool multiCategory)
  : Store(storeq, category, "category", multiCategory) {
}

CategoryStore::CategoryStore(StoreQueue* storeq,
                             const std::string& category,
                             const std::string& name, bool multiCategory)
  : Store(storeq, category, name, multiCategory) {
}

CategoryStore::~CategoryStore() {
}

boost::shared_ptr<Store> CategoryStore::copy(const std::string &category) {
  CategoryStore *store = new CategoryStore(storeQueue, category, multiCategory);

  store->modelStore = modelStore->copy(category);

  return shared_ptr<Store>(store);
}

bool CategoryStore::open() {
  bool result = true;

  for (map<string, shared_ptr<Store> >::iterator iter = stores.begin();
      iter != stores.end();
      ++iter) {
    result &= iter->second->open();
  }

  return result;
}

bool CategoryStore::isOpen() {

  for (map<string, shared_ptr<Store> >::iterator iter = stores.begin();
      iter != stores.end();
      ++iter) {
    if (!iter->second->isOpen()) {
      return false;
    }
  }

  return true;
}

void CategoryStore::configure(pStoreConf configuration, pStoreConf parent) {
  Store::configure(configuration, parent);
  /**
   *  Parse the store defined and use this store as a model to create a
   *  new store for every new category we see later.
   *  <store>
   *    type=category
   *    <model>
   *      type=...
   *      ...
   *    </model>
   *  </store>
   */
  pStoreConf cur_conf;

  if (!configuration->getStore("model", cur_conf)) {
    setStatus("CATEGORYSTORE: NO stores found, invalid store.");
    LOG_OPER("[%s] CATEGORYSTORE: No stores found, invalid store.",
             categoryHandled.c_str());
  } else {
    string cur_type;

    // find this store's type
    if (!cur_conf->getString("type", cur_type)) {
      LOG_OPER("[%s] CATEGORYSTORE: Store is missing type.",
               categoryHandled.c_str());
      setStatus("CATEGORYSTORE: Store is missing type.");
      return;
    }

    configureCommon(cur_conf, parent, cur_type);
  }
}

void CategoryStore::configureCommon(pStoreConf configuration,
                                    pStoreConf parent,
                                    const string type) {
  Store::configure(configuration, parent);
  // initialize model store
  modelStore = createStore(storeQueue, type, categoryHandled, false, false);
  LOG_OPER("[%s] %s: Configured store of type %s successfully.",
           categoryHandled.c_str(), getType().c_str(), type.c_str());
  modelStore->configure(configuration, parent);
}

void CategoryStore::close() {
  for (map<string, shared_ptr<Store> >::iterator iter = stores.begin();
      iter != stores.end();
      ++iter) {
    iter->second->close();
  }
}

bool CategoryStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  shared_ptr<logentry_vector_t> singleMessage(new logentry_vector_t);
  shared_ptr<logentry_vector_t> failed_messages(new logentry_vector_t);
  logentry_vector_t::iterator message_iter;

  for (message_iter = messages->begin();
      message_iter != messages->end();
      ++message_iter) {
    map<string, shared_ptr<Store> >::iterator store_iter;
    shared_ptr<Store> store;
    string category = (*message_iter)->category;

    store_iter = stores.find(category);

    if (store_iter == stores.end()) {
      // Create new store for this category
      store = modelStore->copy(category);
      store->open();
      stores[category] = store;
    } else {
      store = store_iter->second;
    }

    if (store == NULL || !store->isOpen()) {
      LOG_OPER("[%s] Failed to open store for category <%s>",
               categoryHandled.c_str(), category.c_str());
      failed_messages->push_back(*message_iter);
      continue;
    }

    // send this message to the store that handles this category
    singleMessage->clear();
    singleMessage->push_back(*message_iter);

    if (!store->handleMessages(singleMessage)) {
      LOG_OPER("[%s] Failed to handle message for category <%s>",
               categoryHandled.c_str(), category.c_str());
      failed_messages->push_back(*message_iter);
      continue;
    }
  }

  if (!failed_messages->empty()) {
    // Did not handle all messages, update message vector
    messages->swap(*failed_messages);
    return false;
  } else {
    return true;
  }
}

void CategoryStore::periodicCheck() {
  for (map<string, shared_ptr<Store> >::iterator iter = stores.begin();
      iter != stores.end();
      ++iter) {
    iter->second->periodicCheck();
  }
}

void CategoryStore::flush() {
  for (map<string, shared_ptr<Store> >::iterator iter = stores.begin();
      iter != stores.end();
      ++iter) {
    iter->second->flush();
  }
}

MultiFileStore::MultiFileStore(StoreQueue* storeq,
                               const std::string& category,
                               bool multi_category)
  : CategoryStore(storeq, category, "MultiFileStore", multi_category) {
}

MultiFileStore::~MultiFileStore() {
}

void MultiFileStore::configure(pStoreConf configuration, pStoreConf parent) {
  configureCommon(configuration, parent, "file");
}

ThriftMultiFileStore::ThriftMultiFileStore(StoreQueue* storeq,
                                          const std::string& category,
                                           bool multi_category)
  : CategoryStore(storeq, category, "ThriftMultiFileStore", multi_category) {
}

ThriftMultiFileStore::~ThriftMultiFileStore() {
}

void ThriftMultiFileStore::configure(pStoreConf configuration, pStoreConf parent) {
  configureCommon(configuration, parent, "thriftfile");
}
