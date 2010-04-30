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

#include "common.h"
#include "scribe_server.h"

using namespace std;
using namespace boost;
using namespace scribe::thrift;

#define DEFAULT_TARGET_WRITE_SIZE  16384LL
#define DEFAULT_MAX_WRITE_INTERVAL 1

void* threadStatic(void *this_ptr) {
  StoreQueue *queue_ptr = (StoreQueue*)this_ptr;
  queue_ptr->threadMember();
  return NULL;
}

StoreQueue::StoreQueue(const string& type, const string& category,
                       unsigned check_period, bool is_model, bool multi_category)
  : msgQueueSize(0),
    hasWork(false),
    stopping(false),
    isModel(is_model),
    multiCategory(multi_category),
    categoryHandled(category),
    checkPeriod(check_period),
    targetWriteSize(DEFAULT_TARGET_WRITE_SIZE),
    maxWriteInterval(DEFAULT_MAX_WRITE_INTERVAL),
    mustSucceed(true) {

  store = Store::createStore(this, type, category,
                            false, multiCategory);
  if (!store) {
    throw std::runtime_error("createStore failed in StoreQueue constructor. Invalid type?");
  }
  storeInitCommon();
}

StoreQueue::StoreQueue(const boost::shared_ptr<StoreQueue> example,
                       const std::string &category)
  : msgQueueSize(0),
    hasWork(false),
    stopping(false),
    isModel(false),
    multiCategory(example->multiCategory),
    categoryHandled(category),
    checkPeriod(example->checkPeriod),
    targetWriteSize(example->targetWriteSize),
    maxWriteInterval(example->maxWriteInterval),
    mustSucceed(example->mustSucceed) {

  store = example->copyStore(category);
  if (!store) {
    throw std::runtime_error("createStore failed copying model store");
  }
  storeInitCommon();
}


StoreQueue::~StoreQueue() {
  if (!isModel) {
    pthread_mutex_destroy(&cmdMutex);
    pthread_mutex_destroy(&msgMutex);
    pthread_mutex_destroy(&hasWorkMutex);
    pthread_cond_destroy(&hasWorkCond);
  }
}

void StoreQueue::addMessage(boost::shared_ptr<LogEntry> entry) {
  if (isModel) {
    LOG_OPER("ERROR: called addMessage on model store");
  } else {
    bool waitForWork = false;

    pthread_mutex_lock(&msgMutex);
    msgQueue->push_back(entry);
    msgQueueSize += entry->message.size();

    waitForWork = (msgQueueSize >= targetWriteSize) ? true : false;
    pthread_mutex_unlock(&msgMutex);

    // Wake up store thread if we have enough messages
    if (waitForWork == true) {
      // signal that there is work to do if not already signaled
      pthread_mutex_lock(&hasWorkMutex);
      if (!hasWork) {
        hasWork = true;
        pthread_cond_signal(&hasWorkCond);
      }
      pthread_mutex_unlock(&hasWorkMutex);
    }
  }
}

void StoreQueue::configureAndOpen(pStoreConf configuration) {
  // model store has to handle this inline since it has no queue
  if (isModel) {
    configureInline(configuration);
  } else {
    pthread_mutex_lock(&cmdMutex);
    StoreCommand cmd(CMD_CONFIGURE, configuration);
    cmdQueue.push(cmd);
    pthread_mutex_unlock(&cmdMutex);

    // signal that there is work to do if not already signaled
    pthread_mutex_lock(&hasWorkMutex);
    if (!hasWork) {
      hasWork = true;
      pthread_cond_signal(&hasWorkCond);
    }
    pthread_mutex_unlock(&hasWorkMutex);
  }
}

void StoreQueue::stop() {
  if (isModel) {
    LOG_OPER("ERROR: called stop() on model store");
  } else if(!stopping) {
    pthread_mutex_lock(&cmdMutex);
    StoreCommand cmd(CMD_STOP);
    cmdQueue.push(cmd);
    stopping = true;
    pthread_mutex_unlock(&cmdMutex);

    // signal that there is work to do if not already signaled
    pthread_mutex_lock(&hasWorkMutex);
    if (!hasWork) {
      hasWork = true;
      pthread_cond_signal(&hasWorkCond);
    }
    pthread_mutex_unlock(&hasWorkMutex);

    pthread_join(storeThread, NULL);
  }
}

void StoreQueue::open() {
  if (isModel) {
    LOG_OPER("ERROR: called open() on model store");
  } else {
    pthread_mutex_lock(&cmdMutex);
    StoreCommand cmd(CMD_OPEN);
    cmdQueue.push(cmd);
    pthread_mutex_unlock(&cmdMutex);

    // signal that there is work to do if not already signaled
    pthread_mutex_lock(&hasWorkMutex);
    if (!hasWork) {
      hasWork = true;
      pthread_cond_signal(&hasWorkCond);
    }
    pthread_mutex_unlock(&hasWorkMutex);
  }
}

shared_ptr<Store> StoreQueue::copyStore(const std::string &category) {
  return store->copy(category);
}

std::string StoreQueue::getCategoryHandled() {
  return categoryHandled;
}


std::string StoreQueue::getStatus() {
  return store->getStatus();
}

std::string StoreQueue::getBaseType() {
  return store->getType();
}

void StoreQueue::threadMember() {
  LOG_OPER("store thread starting");

  if (isModel) {
    LOG_OPER("ERROR: store thread starting on model store, exiting");
    return;
  }

  if (!store) {
    LOG_OPER("store is NULL, store thread exiting");
    return;
  }

  // init time of last periodic check to time of 0
  time_t last_periodic_check = 0;

  time_t last_handle_messages;
  time(&last_handle_messages);

  // initialize absolute timestamp
  struct timespec abs_timeout;
  memset(&abs_timeout, 0, sizeof(struct timespec));

  bool stop = false;
  bool open = false;
  while (!stop) {

    // handle commands
    //
    pthread_mutex_lock(&cmdMutex);
    while (!cmdQueue.empty()) {
      StoreCommand cmd = cmdQueue.front();
      cmdQueue.pop();

      switch (cmd.command) {
      case CMD_CONFIGURE:
        configureInline(cmd.configuration);
        openInline();
        open = true;
        break;
      case CMD_OPEN:
        openInline();
        open = true;
        break;
      case CMD_STOP:
        stop = true;
        break;
      default:
        LOG_OPER("LOGIC ERROR: unknown command to store queue");
        break;
      }
    }

    // handle periodic tasks
    //
    time_t this_loop;
    time(&this_loop);
    if (!stop && open && this_loop - last_periodic_check > checkPeriod) {
      store->periodicCheck();
      last_periodic_check = this_loop;
    }

    pthread_mutex_lock(&msgMutex);
    pthread_mutex_unlock(&cmdMutex);

    boost::shared_ptr<logentry_vector_t> messages;

    // handle messages if stopping, enough time has passed, or queue is large
    //
    if (stop ||
        (this_loop - last_handle_messages > maxWriteInterval) ||
        msgQueueSize >= targetWriteSize) {

      if (failedMessages) {
        // process any messages we were not able to process last time
        messages = failedMessages;
        failedMessages = boost::shared_ptr<logentry_vector_t>();
      } else if (msgQueueSize > 0) {
        // process message in queue
        messages = msgQueue;
        msgQueue = boost::shared_ptr<logentry_vector_t>(new logentry_vector_t);
        msgQueueSize = 0;
      }

      // reset timer
      last_handle_messages = this_loop;
    }

    pthread_mutex_unlock(&msgMutex);

    if (messages) {
      if (!store->handleMessages(messages)) {
        // Store could not handle these messages
        processFailedMessages(messages);
      }
      store->flush();
    }

    if (!stop) {
      // set timeout to when we need to handle messages or do a periodic check
      abs_timeout.tv_sec = min(last_periodic_check + checkPeriod,
                               last_handle_messages + maxWriteInterval);

      // must wait until after this time
      abs_timeout.tv_sec++;

      // wait until there's some work to do or we timeout
      pthread_mutex_lock(&hasWorkMutex);
      if (!hasWork) {
	pthread_cond_timedwait(&hasWorkCond, &hasWorkMutex, &abs_timeout);
      }
      hasWork = false;
      pthread_mutex_unlock(&hasWorkMutex);
    }

  } // while (!stop)

  store->close();
}

void StoreQueue::processFailedMessages(shared_ptr<logentry_vector_t> messages) {
  // If the store was not able to process these messages, we will either
  // requeue them or give up depending on the value of mustSucceed

  if (mustSucceed) {
    // Save failed messages
    failedMessages = messages;

    LOG_OPER("[%s] WARNING: Re-queueing %lu messages!",
             categoryHandled.c_str(), messages->size());
    g_Handler->incCounter(categoryHandled, "requeue", messages->size());
  } else {
    // record messages as being lost
    LOG_OPER("[%s] WARNING: Lost %lu messages!",
             categoryHandled.c_str(), messages->size());
    g_Handler->incCounter(categoryHandled, "lost", messages->size());
  }
}

void StoreQueue::storeInitCommon() {
  // model store doesn't need this stuff
  if (!isModel) {
    msgQueue = boost::shared_ptr<logentry_vector_t>(new logentry_vector_t);
    pthread_mutex_init(&cmdMutex, NULL);
    pthread_mutex_init(&msgMutex, NULL);
    pthread_mutex_init(&hasWorkMutex, NULL);
    pthread_cond_init(&hasWorkCond, NULL);

    pthread_create(&storeThread, NULL, threadStatic, (void*) this);
  }
}

void StoreQueue::configureInline(pStoreConf configuration) {
  // Constructor defaults are fine if these don't exist
  configuration->getUnsignedLongLong("target_write_size", targetWriteSize);
  configuration->getUnsigned("max_write_interval",
                            (unsigned long&) maxWriteInterval);

  string tmp;
  if (configuration->getString("must_succeed", tmp) && tmp == "no") {
    mustSucceed = false;
  }

  store->configure(configuration, pStoreConf());
}

void StoreQueue::openInline() {
  if (store->isOpen()) {
    store->close();
  }
  if (!isModel) {
    store->open();
  }
}
