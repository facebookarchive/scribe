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
// @author Anthony Giardullo
// @author John Song

#ifndef SCRIBE_STORE_QUEUE_H
#define SCRIBE_STORE_QUEUE_H


#include "Common.h"
#include "Store.h"

namespace scribe {

class Store;
class StoreQueue;
typedef shared_ptr<StoreQueue> StoreQueuePtr;

/*
 * This class implements a queue and a thread for dispatching
 * events to a store. It creates a store object of the requested
 * type, which can in turn create and manage other store objects.
 */
class StoreQueue : private boost::noncopyable {
 public:
  StoreQueue(const string& type,
             const string& category,
             unsigned checkPeriod,
             bool isModel=false,
             bool multiCategory=false);
  StoreQueue(const StoreQueuePtr example, const string &category);
  virtual ~StoreQueue();

  void addMessage(LogEntryPtr entry);
  void configureAndOpen(StoreConfPtr configuration); // closes first if open
  void open();                                       // closes first if open
  void stop();
  shared_ptr<Store> copyStore(const string &category);
  string getStatus(); // Empty string means OK, anything else is an error
  string getBaseType();
  string getCategoryHandled();
  bool isModelStore() { return isModel_;}

  // this needs to be public for the thread creation to get to it,
  // but no one else should ever call it.
  void threadMember();

  // WARNING: don't expect this to be exact, because it could change after
  //          you check. This is only for hueristics to decide when we're
  //          overloaded.
  inline uint64_t getSize() {
    return msgQueueSize_;
  }

 private:
  void storeInitCommon();
  void configureInline(StoreConfPtr configuration);
  void openInline();
  void processFailedMessages(LogEntryVectorPtr messages);

  // implementation of queues and thread
  enum StoreCommandType {
    CMD_CONFIGURE,
    CMD_OPEN,
    CMD_STOP
  };

  struct StoreCommand {
    StoreCommandType command;
    StoreConfPtr configuration;

    StoreCommand(StoreCommandType cmd, StoreConfPtr config = StoreConfPtr())
      : command(cmd), configuration(config) {};
  };

  typedef queue<StoreCommand> CommandQueue;

  // messages and commands are in different queues to allow bulk
  // handling of messages. This means that order of commands with
  // respect to messages is not preserved.
  CommandQueue       cmdQueue_;
  LogEntryVectorPtr  msgQueue_;
  LogEntryVectorPtr  failedMessages_;
  uint64_t           msgQueueSize_;   // in bytes
  shared_ptr<thrift::Thread> storeThread_;

  // Mutexes
  Mutex cmdMutex_;               // Must be held to read/modify cmdQueue_
  Mutex msgMutex_;               // Must be held to read/modify msgQueue_
  // Please never embed mutex scopes

  bool hasWork_;  // whether there are messages or commands queued
  Monitor hasWorkCond_;          // Must be held to read/modify hasWork_

  bool stopping_;
  bool isModel_;
  bool multiCategory_; // Whether multiple categories are handled

  // configuration
  string          categoryHandled_;  // what category this store is handling
  time_t          checkPeriod_;      // how often to call periodicCheck
                                        // in seconds
  uint64_t        targetWriteSize_;  // in bytes
  time_t          maxWriteInterval_; // in seconds
  bool            mustSucceed_;      // Always retry even if secondary fails

  // Store that will handle messages. This can contain other stores.
  shared_ptr<Store> store_;

  // thread factory to create store queue worker threads
  static shared_ptr<thrift::ThreadFactory> threadFactory_;
};

} //! namespace  scribe

#endif //! SCRIBE_STORE_QUEUE_H
