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

#include "Common.h"
#include "ScribeServer.h"

using namespace scribe::thrift;


static const uint64_t kDefaultTargetWriteSize = 16384;
static const time_t kDefaultMaxWriteInterval  = 1;

namespace scribe {

shared_ptr<ThreadFactory> StoreQueue::threadFactory_(
  new PosixThreadFactory(PosixThreadFactory::ROUND_ROBIN,
                         PosixThreadFactory::NORMAL,
                         1,
                         false)
);

class StoreQueueTask : public Runnable {
 private:
  StoreQueue* queue_;

 public:
  StoreQueueTask(StoreQueue* queue)
    : queue_(queue) {
  }

  virtual ~StoreQueueTask() {}

  virtual void run() {
    queue_->threadMember();
  }
};


StoreQueue::StoreQueue(const string& type, const string& category,
                       unsigned checkPeriod, bool isModel, bool multiCategory)
  : msgQueueSize_(0),
    hasWork_(false),
    stopping_(false),
    isModel_(isModel),
    multiCategory_(multiCategory),
    categoryHandled_(category),
    checkPeriod_(checkPeriod),
    targetWriteSize_(kDefaultTargetWriteSize),
    maxWriteInterval_(kDefaultMaxWriteInterval),
    mustSucceed_(true) {

  store_ = Store::createStore(this, type, category,
                            false, multiCategory_);
  if (!store_) {
    throw std::runtime_error("createStore failed in StoreQueue constructor. "
                             "Invalid type?");
  }
  storeInitCommon();
}

StoreQueue::StoreQueue(const StoreQueuePtr example,
                       const string &category)
  : msgQueueSize_(0),
    hasWork_(false),
    stopping_(false),
    isModel_(false),
    multiCategory_(example->multiCategory_),
    categoryHandled_(category),
    checkPeriod_(example->checkPeriod_),
    targetWriteSize_(example->targetWriteSize_),
    maxWriteInterval_(example->maxWriteInterval_),
    mustSucceed_(example->mustSucceed_) {

  store_ = example->copyStore(category);
  if (!store_) {
    throw std::runtime_error("createStore failed copying model store");
  }
  storeInitCommon();
}


StoreQueue::~StoreQueue() {
}

void StoreQueue::addMessage(LogEntryPtr entry) {
  if (isModel_) {
    LOG_OPER("ERROR: called addMessage on model store");
  } else {
    bool waitForWork = false;

    {
      Guard g(msgMutex_);

      msgQueue_->push_back(entry);
      msgQueueSize_ += entry->message.size();

      waitForWork = (msgQueueSize_ >= targetWriteSize_) ? true : false;
    }

    // Wake up store thread if we have enough messages
    if (waitForWork == true) {
      // signal that there is work to do if not already signaled
      Synchronized s(hasWorkCond_);

      if (!hasWork_) {
        hasWork_ = true;
        hasWorkCond_.notify();
      }
    }
  }
}

void StoreQueue::configureAndOpen(StoreConfPtr configuration) {
  // model store has to handle this inline since it has no queue
  if (isModel_) {
    configureInline(configuration);
  } else {
    {
      Guard g(cmdMutex_);

      StoreCommand cmd(CMD_CONFIGURE, configuration);
      cmdQueue_.push(cmd);
    }

    // signal that there is work to do if not already signaled
    {
      Synchronized s(hasWorkCond_);

      if (!hasWork_) {
        hasWork_ = true;
        hasWorkCond_.notify();;
      }
    }
  }
}

void StoreQueue::stop() {
  if (isModel_) {
    LOG_OPER("ERROR: called stop() on model store");
  } else if(!stopping_) {
    {
      Guard g(cmdMutex_);

      StoreCommand cmd(CMD_STOP);
      cmdQueue_.push(cmd);
      stopping_ = true;
    }

    // signal that there is work to do if not already signaled
    {
      Synchronized s(hasWorkCond_);

      if (!hasWork_) {
        hasWork_ = true;
        hasWorkCond_.notify();
      }
    }

    storeThread_->join();
  }
}

void StoreQueue::open() {
  if (isModel_) {
    LOG_OPER("ERROR: called open() on model store");
  } else {
    {
      Guard g(cmdMutex_);

      StoreCommand cmd(CMD_OPEN);
      cmdQueue_.push(cmd);
    }

    // signal that there is work to do if not already signaled
    {
      Synchronized s(hasWorkCond_);

      if (!hasWork_) {
        hasWork_ = true;
        hasWorkCond_.notify();
      }
    }
  }
}

StorePtr StoreQueue::copyStore(const string &category) {
  return store_->copy(category);
}

string StoreQueue::getCategoryHandled() {
  return categoryHandled_;
}


string StoreQueue::getStatus() {
  return store_->getStatus();
}

string StoreQueue::getBaseType() {
  return store_->getType();
}

void StoreQueue::threadMember() {
  LOG_OPER("store thread starting");
  if (isModel_) {
    LOG_OPER("ERROR: store thread starting on model store, exiting");
    return;
  }

  if (!store_) {
    LOG_OPER("store is NULL, store thread exiting");
    return;
  }

  // init time of last periodic check to time of 0
  time_t lastPeriodicCheck = 0;

  time_t lastHandleMessages;
  time(&lastHandleMessages);

  bool stop = false;
  bool open = false;
  while (!stop) {
    time_t thisLoop;

    // handle commands
    //
    {
      Guard g(cmdMutex_);

      while (!cmdQueue_.empty()) {
        StoreCommand cmd = cmdQueue_.front();
        cmdQueue_.pop();

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
      time(&thisLoop);
      if (!stop && ((thisLoop - lastPeriodicCheck) >= checkPeriod_)) {
        if (open) {
          store_->periodicCheck();
        }
        lastPeriodicCheck = thisLoop;
      }
    }

    LogEntryVectorPtr messages;

    {
      Guard g(msgMutex_);

      // handle messages if stopping, enough time has passed, or queue is large
      //
      if (stop ||
          (thisLoop - lastHandleMessages >= maxWriteInterval_) ||
          msgQueueSize_ >= targetWriteSize_) {

        if (failedMessages_) {
          // process any messages we were not able to process last time
          messages = failedMessages_;
          failedMessages_ = LogEntryVectorPtr();
        } else if (msgQueueSize_ > 0) {
          // process message in queue
          messages = msgQueue_;
          msgQueue_.reset(new LogEntryVector);
          msgQueueSize_ = 0;
        }

        // reset timer
        lastHandleMessages = thisLoop;
      }
    }

    if (messages) {
      // all pending messages will be either gone or requeued
      g_handler->stats.addCounter(StatCounters::kStoreQueueOut,
                                  messages->size());
      if (!store_->handleMessages(messages)) {
        // Store could not handle these messages,
        // we might requeue these messages or they might get lost.
        processFailedMessages(messages);
      } else {
        // Successfully dequeued messages
        g_handler->stats.incStoreQueueSize(-(int64_t)messages->size());
      }
      store_->flush();
    }

    if (!stop) {
      // set timeout to when we need to handle messages or do a periodic check
      uint64_t waitTime = 1000 * std::min(lastPeriodicCheck + checkPeriod_,
                                      lastHandleMessages + maxWriteInterval_);
      waitTime -= clock::nowInMsec();

      // wait until there's some work to do or we timeout
      {
        Synchronized s(hasWorkCond_);

        if (!hasWork_) {
          try {
            hasWorkCond_.wait(waitTime);
          } catch (TimedOutException&) {
            // wake up to do some work
          } catch (std::exception& e) {
            LOG_OPER("[%s] ERROR: thrift::Monitor::wait() throws exception: %s",
                     categoryHandled_.c_str(), e.what());
          }
        }
        hasWork_ = false;
      }
    }

  } // while (!stop)

  store_->close();
}

void StoreQueue::processFailedMessages(LogEntryVectorPtr messages) {
  // If the store was not able to process these messages, we will either
  // requeue them or give up depending on the value of mustSucceed_

  if (mustSucceed_) {
    // Save failed messages
    failedMessages_ = messages;

    LOG_OPER("[%s] WARNING: Re-queueing %lu messages!",
             categoryHandled_.c_str(), messages->size());
    g_handler->incCounter(categoryHandled_, "requeue", messages->size());
    g_handler->stats.addCounter(StatCounters::kStoreQueueRequeue,
                                messages->size());
  } else {
    // record messages as being lost
    LOG_OPER("[%s] WARNING: Lost %lu messages!",
             categoryHandled_.c_str(), messages->size());
    g_handler->incCounter(categoryHandled_, "lost", messages->size());
    g_handler->stats.addCounter(StatCounters::kStoreQueueLost,
                                messages->size());
  }
}

void StoreQueue::storeInitCommon() {
  // model store doesn't need this stuff
  if (!isModel_) {
    msgQueue_.reset(new LogEntryVector);

    storeThread_ = threadFactory_->newThread(
      shared_ptr<Runnable>(new StoreQueueTask(this))
    );

    storeThread_->start();
  }
}

void StoreQueue::configureInline(StoreConfPtr configuration) {
  // Constructor defaults are fine if these don't exist
  configuration->getUint64("target_write_size", &targetWriteSize_);
  configuration->getUnsigned("max_write_interval",
                             (unsigned long*) &maxWriteInterval_);
  if (maxWriteInterval_ == 0) {
    maxWriteInterval_ = 1;
  }

  string tmp;
  if (configuration->getString("must_succeed", &tmp) && tmp == "no") {
    mustSucceed_ = false;
  }

  store_->configure(configuration, StoreConfPtr());
}

void StoreQueue::openInline() {
  if (store_->isOpen()) {
    store_->close();
  }
  if (!isModel_) {
    store_->open();
  }
}

} //! namespace scribe
