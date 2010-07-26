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

#include "Common.h"
#include "FileStore.h"
#include "ScribeServer.h"
#include "TimeLatency.h"

using namespace scribe::thrift;

static const std::string kMetaLogfilePrefix = "scribe_meta<new_logfile>: ";

namespace scribe {

FileStore::FileStore(StoreQueue* storeq,
                     const string& category,
                     bool multiCategory,
                     bool isBufferFile)
  : FileStoreBase(storeq, category, "file", multiCategory),
    isBufferFile_(isBufferFile),
    addNewlines_(false),
    convertBuffer_(new TMemoryBuffer()),
    lostBytes_(0) {
}

FileStore::~FileStore() {
}

void FileStore::configure(StoreConfPtr configuration, StoreConfPtr parent) {
  FileStoreBase::configure(configuration, parent);

  // We can run using defaults for all of these, but there are
  // a couple of suspicious things we warn about.
  if (isBufferFile_) {
    // scheduled file rotations of buffer files lead to too many messy cases
    rollPeriod_ = ROLL_NEVER;

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
    chunkSize_ = 0;

    // Combine all categories in a single file for buffers
    if (multiCategory_) {
      writeCategory_ = true;
    }
  }

  unsigned long intTemp = 0;
  configuration->getUnsigned("add_newlines", &intTemp);
  addNewlines_ = intTemp ? true : false;
}

bool FileStore::openInternal(bool incrementFilename, struct tm* currentTime) {
  bool success = false;
  struct tm timeInfo;

  if (!currentTime) {
    time_t rawTime = time(NULL);
    localtime_r(&rawTime, &timeInfo);
    currentTime = &timeInfo;
  }

  try {
    int suffix = findNewestFile(makeBaseFilename(currentTime));

    if (incrementFilename) {
      ++suffix;
    }

    // this is the case where there's no file there and we're not incrementing
    if (suffix < 0) {
      suffix = 0;
    }

    string file = makeFullFilename(suffix, currentTime);

    switch (rollPeriod_) {
      case ROLL_DAILY:
        lastRollTime_ = currentTime->tm_mday;
        break;
      case ROLL_HOURLY:
        lastRollTime_ = currentTime->tm_hour;
        break;
      case ROLL_OTHER:
        lastRollTime_ = time(NULL);
        break;
      case ROLL_NEVER:
        break;
    }

    if (writeFile_) {
      if (writeFollowing_) {
        writeFile_->write(kMetaLogfilePrefix + file);
      }
      writeFile_->close();
    }

    writeFile_ = FileInterface::createFileInterface(fsType_, file,
                                                    isBufferFile_);
    if (!writeFile_) {
      LOG_OPER("[%s] Failed to create file <%s> of type <%s> for writing",
               categoryHandled_.c_str(), file.c_str(), fsType_.c_str());
      setStatus("file open error");
      return false;
    }

    success = writeFile_->createDirectory(baseFilePath_);

    // If we created a subdirectory, we need to create two directories
    if (success && !subDirectory_.empty()) {
      success = writeFile_->createDirectory(filePath_);
    }

    if (!success) {
      LOG_OPER("[%s] Failed to create directory for file <%s>",
               categoryHandled_.c_str(), file.c_str());
      setStatus("File open error");
      return false;
    }

    success = writeFile_->openWrite();


    if (!success) {
      LOG_OPER("[%s] Failed to open file <%s> for writing",
              categoryHandled_.c_str(),
              file.c_str());
      setStatus("File open error");
    } else {

      /* just make a best effort here, and don't error if it fails */
      if (createSymlink_ && !isBufferFile_) {
        string symlinkName = makeFullSymlink();
        FileInterfacePtr tmp = FileInterface::createFileInterface(
            fsType_, symlinkName, isBufferFile_);
        tmp->deleteFile();
        string symlinkTarget = makeFullFilename(suffix, currentTime, false);
        writeFile_->createSymlink(symlinkTarget, symlinkName);
      }
      // else it confuses the filename code on reads

      LOG_OPER("[%s] Opened file <%s> for writing", categoryHandled_.c_str(),
              file.c_str());

      currentSize_ = writeFile_->fileSize();
      currentFilename_ = file;
      eventsWritten_ = 0;
      setStatus("");
    }

  } catch(const std::exception& e) {
    LOG_OPER("[%s] Failed to create/open file of type <%s> for writing",
             categoryHandled_.c_str(), fsType_.c_str());
    LOG_OPER("Exception: %s", e.what());
    setStatus("file create/open error");

    return false;
  }
  return success;
}

bool FileStore::isOpen() {
  return writeFile_ && writeFile_->isOpen();
}

void FileStore::close() {
  if (writeFile_) {
    writeFile_->close();
  }
}

void FileStore::flush() {
  if (writeFile_) {
    writeFile_->flush();
  }
}

StorePtr FileStore::copy(const string &category) {
  StorePtr copied(
    new FileStore(storeQueue_, category, multiCategory_, isBufferFile_)
  );

  FileStore* store = static_cast<FileStore*>(copied.get());
  store->addNewlines_ = addNewlines_;
  store->copyCommon(this);

  return copied;
}

bool FileStore::handleMessages(LogEntryVectorPtr messages) {

  if (!isOpen()) {
    if (!open()) {
      LOG_OPER("[%s] File failed to open FileStore::handleMessages()",
               categoryHandled_.c_str());
      g_handler->stats.addCounter(StatCounters::kFileOpenErr, 1);
      return false;
    }
  }

  g_handler->stats.addCounter(StatCounters::kFileIn, messages->size());

  // write messages to current file
  return writeMessages(messages);
}

// specific logic for buffer files
string FileStore::makeFullFilename(int suffix, struct tm* creationTime,
                                   bool useFullPath) {
  string filename = FileStoreBase::makeFullFilename(suffix, creationTime,
                                                    useFullPath);
  if (isBufferFile_) {
    filename += ".buffer";
  }

  return filename;
}

string FileStore::getFullFilename(int suffix, struct tm* creationTime,
                                  bool useFullPath) {
  // first try the new style filename
  string filename = makeFullFilename(suffix, creationTime, useFullPath);

  // for buffer file, the ".buffer" extension is optional
  if (isBufferFile_) {
    FileInterfacePtr file =
        FileInterface::createFileInterface(fsType_, filename);
    if (file->exists() == 0) {
      // fail over to old style filename
      filename = FileStoreBase::makeFullFilename(suffix, creationTime,
                                                 useFullPath);
    }
  }

  return filename;
}

// writes messages to either the specified file or the the current writeFile_
bool FileStore::writeMessages(LogEntryVectorPtr messages,
                              FileInterfacePtr file) {
  // Data is written to a buffer first, then sent to disk in one call to write.
  // This costs an extra copy of the data, but dramatically improves latency
  // with network based files. (nfs, etc)
  string        writeBuf;
  bool          success = true;
  unsigned long numBuffered = 0;
  unsigned long numWritten = 0;
  FileInterfacePtr writeFile;
  unsigned long maxWriteSize = std::min(maxSize_, maxWriteSize_);
  shared_ptr<TProtocol> protocol;

  protocol = TBinaryProtocolFactory().getProtocol(convertBuffer_);


  // if no file given, use current writeFile_
  if (file) {
    writeFile = file;
  } else {
    writeFile = writeFile_;
  }

  // reserve a reasonable size of the write buffer
  writeBuf.reserve(maxWriteSize + 1024);

  try {
    for (LogEntryVector::iterator iter = messages->begin();
         iter != messages->end();
         ++ iter) {

      if (isBufferFile_) {
        (*iter)->write(protocol.get());
        unsigned long messageLen = convertBuffer_->available_read();

        writeBuf += writeFile->getFrame(messageLen);
        writeBuf += convertBuffer_->getBufferAsString();
        convertBuffer_->resetBuffer();

      } else {
        // have to be careful with the length here. getFrame wants the length
        // without the frame, then bytesToPad wants the length of the frame and
        // the message.
        string messageFrame, categoryFrame;
        unsigned long length = 0;

        if (writeCategory_) {
          //add space for category+newline and category frame
          unsigned long categoryLen = (*iter)->category.length() + 1;
          categoryFrame = writeFile->getFrame(categoryLen);
          length += categoryFrame.length() + categoryLen;
        }

        unsigned long messageLen = (*iter)->message.length();

        if (addNewlines_) {
          ++ messageLen;
        }

        // frame is a header that the underlying file class can add to
        // each message
        messageFrame = writeFile->getFrame(messageLen);
        length += messageFrame.length() + messageLen;

        // padding to align messages on chunk boundaries
        unsigned long padding = bytesToPad(length, writeBuf.size(), chunkSize_);

        if (padding) {
          writeBuf += string(padding, 0);
        }

        if (writeCategory_) {
          writeBuf += categoryFrame;
          writeBuf += (*iter)->category;
          writeBuf += '\n';
        }

        writeBuf += messageFrame;
        writeBuf += (*iter)->message;

        if (addNewlines_) {
          writeBuf += '\n';
        }
      }

      ++ numBuffered;

      if (!isBufferFile_ && isTimeStampPresent(*(*iter))) {
        // report the numbers to fb303 counters
        unsigned long messageTimestamp = getTimeStamp(*(*iter));
        unsigned long currentTime = getCurrentTimeStamp();

        g_handler->reportLatencyWriter((*iter)->category,
            currentTime - messageTimestamp);
      }

      // Write buffer if processing last message or if larger than allowed
      if (writeBuf.size() > maxWriteSize ||
          messages->end() == iter + 1 ) {
        if (!writeFile->write(writeBuf)) {
          LOG_OPER("[%s] File store failed to write (%lu) messages to file",
                   categoryHandled_.c_str(), messages->size());
          setStatus("File write error");
          g_handler->stats.addCounter(StatCounters::kFileWriteErr, 1);
          success = false;
          break;
        }

        g_handler->stats.addCounter(StatCounters::kFileWritten, numBuffered);
        g_handler->stats.addCounter(StatCounters::kFileWrittenBytes,
                                    writeBuf.size());

        numWritten += numBuffered;
        currentSize_ += writeBuf.size();
        numBuffered = 0;
        writeBuf.clear();
      }

      // rotate file if large enough and not writing to a separate file
      if (currentSize_ > maxSize_ && !file) {
        rotateFile();
        writeFile = writeFile_;
      }
    }
  } catch (const std::exception& e) {
    LOG_OPER("[%s] File store failed to write. Exception: %s",
             categoryHandled_.c_str(), e.what());
    g_handler->stats.addCounter(StatCounters::kFileWriteErr, 1);
    success = false;
  }

  eventsWritten_ += numWritten;

  if (!success) {
    close();

    // update messages to include only the messages that were not handled
    if (numWritten > 0) {
      messages->erase(messages->begin(), messages->begin() + numWritten);
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
  FileInterfacePtr file = FileInterface::createFileInterface(fsType_,
                                            getFullFilename(index, now));
  if (lostBytes_) {
    g_handler->incCounter(categoryHandled_, "bytes lost", lostBytes_);
    lostBytes_ = 0;
  }
  file->deleteFile();
}

// Replace the messages in the oldest file at this timestamp with
// the input messages
bool FileStore::replaceOldest(LogEntryVectorPtr messages,
                              struct tm* now) {
  string baseName = makeBaseFilename(now);
  int index = findOldestFile(baseName);
  if (index < 0) {
    LOG_OPER("[%s] Could not find files <%s>",
             categoryHandled_.c_str(), baseName.c_str());
    return false;
  }

  string filename = getFullFilename(index, now);

  // Need to close and reopen store in case we already have this file open
  close();

  FileInterfacePtr inFile = FileInterface::createFileInterface(fsType_,
                                          filename, isBufferFile_);

  // overwrite the old contents of the file
  bool success;
  if (inFile->openTruncate()) {
    success = writeMessages(messages, inFile);

  } else {
    LOG_OPER("[%s] Failed to open file <%s> for writing and truncate",
             categoryHandled_.c_str(), filename.c_str());
    g_handler->stats.addCounter(StatCounters::kFileOpenErr, 1);
    success = false;
  }

  // close this file and re-open store
  inFile->close();
  open();

  return success;
}

bool FileStore::readOldest(/*out*/ LogEntryVectorPtr messages,
                           struct tm* now) {

  long loss;
  shared_ptr<TProtocol> protocol;

  int index = findOldestFile(makeBaseFilename(now));
  if (index < 0) {
    // This isn't an error. It's legit to call readOldest when there aren't any
    // files left, in which case the call succeeds but returns messages empty.
    return true;
  }
  string filename = getFullFilename(index, now);

  FileInterfacePtr inFile = FileInterface::createFileInterface(fsType_,
                                              filename, isBufferFile_);

  if (!inFile->openRead()) {
    LOG_OPER("[%s] Failed to open file <%s> for reading",
            categoryHandled_.c_str(), filename.c_str());
    g_handler->stats.addCounter(StatCounters::kFileOpenErr, 1);
    return false;
  }

  // new style buffer files are thrift encoded
  bool isThriftEncoded = isBufferFile_ &&
    (filename == makeFullFilename(index, now));

  if (isThriftEncoded) {
    protocol = TBinaryProtocolFactory().getProtocol(convertBuffer_);
  }

  uint32_t bsize = 0;
  string message;
  while ((loss = inFile->readNext(&message)) > 0) {
    if (!message.empty()) {
      LogEntryPtr entry(new LogEntry);

      if (isThriftEncoded) {
        convertBuffer_->write(reinterpret_cast<const uint8_t*>(message.data()),
                             message.length());
        entry->read(protocol.get());

      } else {
        // check whether a category is stored with the message
        if (writeCategory_) {
          // get category without trailing \n
          entry->category = message.substr(0, message.length() - 1);
          if ((loss = inFile->readNext(&message)) <= 0) {
            LOG_OPER("[%s] category not stored with metadata/message <%s> "
                     "corruption?, incompatible config change?",
                     categoryHandled_.c_str(), entry->category.c_str());
            break;
          }
        } else {
          entry->category = categoryHandled_;
        }

        entry->message = message;
      }

      messages->push_back(entry);
      bsize += entry->category.size();
      bsize += entry->message.size();
    }
  }

  if (loss < 0) {
    lostBytes_ = -loss;
    g_handler->stats.addCounter(StatCounters::kFileLostBytes, lostBytes_);
  } else {
    lostBytes_ = 0;
  }
  inFile->close();

  LOG_OPER("[%s] read <%lu> entries of <%d> bytes from file <%s>",
        categoryHandled_.c_str(), messages->size(), bsize, filename.c_str());

  g_handler->stats.addCounter(StatCounters::kFileRead, messages->size());
  g_handler->stats.addCounter(StatCounters::kFileReadBytes, bsize);

  return true;
}

bool FileStore::empty(struct tm* now) {

  vector<string> files = FileInterface::list(filePath_, fsType_);
  string baseFilename = makeBaseFilename(now);
  for (vector<string>::iterator iter = files.begin();
      iter != files.end();
      ++iter) {
    int suffix =  getFileSuffix(*iter, baseFilename);
    if (-1 != suffix) {
      string fullname = FileStoreBase::makeFullFilename(suffix, now);
      FileInterfacePtr file = FileInterface::createFileInterface(fsType_,
          fullname);
      if (file->exists()) {
        return false;
      }
      // The new buffer file format ends in .buffer
      string fullnameBuffer = makeFullFilename(suffix, now);
      FileInterfacePtr file1 = FileInterface::createFileInterface(fsType_,
          fullnameBuffer);
      if (file1->exists()) {
        return false;
      }

    } // else it doesn't match the filename for this store
  }

  return true;
}

} //! namespace scribe
