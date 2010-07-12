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
#include "ScribeServer.h"
#include "ThriftFileStore.h"
#include "thrift/transport/TSimpleFileTransport.h"

using namespace scribe::thrift;

namespace scribe {

ThriftFileStore::ThriftFileStore(StoreQueue* storeq,
                                 const string& category,
                                 bool multiCategory)
  : FileStoreBase(storeq, category, "thriftfile", multiCategory),
    flushFrequencyMs_(0),
    msgBufferSize_(0),
    useSimpleFile_(0) {
}

ThriftFileStore::~ThriftFileStore() {
}

StorePtr ThriftFileStore::copy(const string &category) {
  StorePtr copied(new ThriftFileStore(storeQueue_, category, multiCategory_));

  ThriftFileStore* store = static_cast<ThriftFileStore*>(copied.get());
  store->flushFrequencyMs_ = flushFrequencyMs_;
  store->msgBufferSize_ = msgBufferSize_;
  store->copyCommon(this);

  return copied;
}

bool ThriftFileStore::handleMessages(LogEntryVectorPtr messages) {
  if (!isOpen()) {
    if (!open()) {
      return false;
    }
  }

  unsigned long messagesHandled = 0;
  for (LogEntryVector::iterator iter = messages->begin();
       iter != messages->end();
       ++iter) {

    // This length is an estimate -- what the ThriftLogFile actually writes is
    // a black box to us
    uint32_t length = (*iter)->message.size();

    try {
      thriftFileTransport_->write(
        reinterpret_cast<const uint8_t*>((*iter)->message.data()),
        length
      );
      currentSize_ += length;
      ++eventsWritten_;
      ++messagesHandled;
    } catch (const TException& te) {
      LOG_OPER("[%s] Thrift file store failed to write to file: %s\n",
               categoryHandled_.c_str(), te.what());
      setStatus("File write error");

      // If we already handled some messages, remove them from vector before
      // returning failure
      if (messagesHandled) {
        messages->erase(messages->begin(), iter);
      }
      return false;
    }
  }
  // We can't wait until periodicCheck because we could be getting
  // a lot of data all at once in a failover situation
  if (currentSize_ > maxSize_) {
    rotateFile();
  }

  return true;
}

bool ThriftFileStore::open() {
  return openInternal(true, NULL);
}

bool ThriftFileStore::isOpen() {
  return thriftFileTransport_ && thriftFileTransport_->isOpen();
}

void ThriftFileStore::configure(StoreConfPtr config, StoreConfPtr parent) {
  FileStoreBase::configure(config, parent);
  config->getUnsigned("flush_frequency_ms", &flushFrequencyMs_);
  config->getUnsigned("msg_buffer_size", &msgBufferSize_);
  config->getUnsigned("use_simple_file", &useSimpleFile_);
}

void ThriftFileStore::close() {
  thriftFileTransport_.reset();
}

void ThriftFileStore::flush() {
  // TFileTransport has its own periodic flushing mechanism, and we
  // introduce deadlocks if we try to call it from more than one place
  return;
}

bool ThriftFileStore::openInternal(bool incrementFilename,
                                   struct tm* currentTime) {
  struct tm timeInfo;

  if (!currentTime) {
    time_t rawTime = time(NULL);
    localtime_r(&rawTime, &timeInfo);
    currentTime = &timeInfo;
  }
  int suffix;
  try {
    suffix = findNewestFile(makeBaseFilename(currentTime));
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

  string filename = makeFullFilename(suffix, currentTime);
  /* try to create the directory containing the file */
  if (!createFileDirectory()) {
    LOG_OPER("[%s] Could not create path for file: %s",
             categoryHandled_.c_str(), filename.c_str());
    return false;
  }

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


  try {
    if (useSimpleFile_) {
      thriftFileTransport_.reset(
        new TSimpleFileTransport(filename, false, true)
      );
    } else {
      thriftFileTransport_.reset(new TFileTransport(filename));
      TFileTransport* transport =
        dynamic_cast<TFileTransport*>(thriftFileTransport_.get());

      if (chunkSize_ != 0) {
        transport->setChunkSize(chunkSize_);
      }
      if (flushFrequencyMs_ > 0) {
        transport->setFlushMaxUs(flushFrequencyMs_ * 1000);
      }
      if (msgBufferSize_ > 0) {
        transport->setEventBufferSize(msgBufferSize_);
      }
    }

    LOG_OPER("[%s] Opened file <%s> for writing",
        categoryHandled_.c_str(), filename.c_str());

    struct stat st;
    if (stat(filename.c_str(), &st) == 0) {
      currentSize_ = st.st_size;
    } else {
      currentSize_ = 0;
    }
    currentFilename_ = filename;
    eventsWritten_ = 0;
    setStatus("");
  } catch (const TException& te) {
    LOG_OPER("[%s] Failed to open file <%s> for writing: %s\n",
        categoryHandled_.c_str(), filename.c_str(), te.what());
    setStatus("File open error");
    return false;
  }

  /* just make a best effort here, and don't error if it fails */
  if (createSymlink_) {
    string symlinkName = makeFullSymlink();
    unlink(symlinkName.c_str());
    string symtarget = makeFullFilename(suffix, currentTime, false);
    symlink(symtarget.c_str(), symlinkName.c_str());
  }

  return true;
}

bool ThriftFileStore::createFileDirectory () {
  try {
    boost::filesystem::create_directories(filePath_);
  } catch(const std::exception& e) {
    LOG_OPER("Exception < %s > in ThriftFileStore::createFileDirectory "
             "for path %s", e.what(),filePath_.c_str());
    return false;
  }
  return true;
}

} //! namespace scribe
