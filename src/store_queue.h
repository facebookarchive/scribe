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

#include "common.h"

class Store;

/*
 * This class implements a queue and a thread for dispatching
 * events to a store. It creates a store object of the requested
 * type, which can in turn create and manage other store objects.
 */
class StoreQueue {
 public:
  StoreQueue(const std::string& type, const std::string& category,
             unsigned check_period, bool is_model=false, bool multi_category=false);
  StoreQueue(const boost::shared_ptr<StoreQueue> example,
             const std::string &category);
  virtual ~StoreQueue();

  void addMessage(logentry_ptr_t entry);
  void configureAndOpen(pStoreConf configuration); // closes first if already open
  void open();                                     // closes first if already open
  void stop();
  boost::shared_ptr<Store> copyStore(const std::string &category);
  std::string getStatus(); // An empty string means OK, anything else is an error
  std::string getBaseType();
  std::string getCategoryHandled();
  bool isModelStore() { return isModel;}

  // this needs to be public for the thread creation to get to it,
  // but no one else should ever call it.
  void threadMember();

  // WARNING: don't expect this to be exact, because it could change after you check.
  //          This is only for hueristics to decide when we're overloaded.
  inline unsigned long long getSize() {
    return msgQueueSize;
  }
 private:
  void storeInitCommon();
  void configureInline(pStoreConf configuration);
  void openInline();
  void processFailedMessages(boost::shared_ptr<logentry_vector_t> messages);

  // implementation of queues and thread
  enum store_command_t {
    CMD_CONFIGURE,
    CMD_OPEN,
    CMD_STOP
  };

  class StoreCommand {
  public:
    store_command_t command;
    pStoreConf configuration;

    StoreCommand(store_command_t cmd, pStoreConf config = pStoreConf())
      : command(cmd), configuration(config) {};
  };

  typedef std::queue<StoreCommand> cmd_queue_t;

  // messages and commands are in different queues to allow bulk
  // handling of messages. This means that order of commands with
  // respect to messages is not preserved.
  cmd_queue_t cmdQueue;
  boost::shared_ptr<logentry_vector_t> msgQueue;
  boost::shared_ptr<logentry_vector_t> failedMessages;
  unsigned long long msgQueueSize;   // in bytes
  pthread_t storeThread;

  // Mutexes
  pthread_mutex_t cmdMutex;     // Must be held to read/modify cmdQueue
  pthread_mutex_t msgMutex;     // Must be held to read/modify msgQueue
  pthread_mutex_t hasWorkMutex; // Must be held to read/modify hasWork
  // If acquiring multiple mutexes, always acquire in this order:
  // {cmdMutex, msgMutex, hasWorkMutex}

  bool hasWork;  // whether there are messages or commands queued
  pthread_cond_t hasWorkCond; // cond variable to wait on for hasWork

  bool stopping;
  bool isModel;
  bool multiCategory; // Whether multiple categories are handled

  // configuration
  std::string        categoryHandled;  // what category this store is handling
  time_t             checkPeriod;      // how often to call periodicCheck in seconds
  unsigned long long targetWriteSize;  // in bytes
  time_t             maxWriteInterval; // in seconds
  bool               mustSucceed;      // Always retry even if secondary fails

  // Store that will handle messages. This can contain other stores.
  boost::shared_ptr<Store> store;
};

#endif //!defined SCRIBE_STORE_QUEUE_H
