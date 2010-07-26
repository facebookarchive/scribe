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
#include "BufferStore.h"
#include "ScribeServer.h"

using namespace std;

static const unsigned long kDefaultBufferStoreSendRate            = 1;
static const time_t        kDefaultBufferStoreAvgRetryInterval    = 300;
static const time_t        kDefaultBufferStoreRetryIntervalRange  = 60;
static const double        kDefaultBufferStoreBypassMaxQSizeRatio = 0.75;

// Parameters for adaptive_backoff
static const int           kDefaultMinRetry          = 5;
static const int           kDefaultMaxRetry          = 100;
static const time_t        kDefaultRandomOffsetRange = 20;
static const double        kMultIncFactor            = 1.414; //sqrt(2)
static const int           kAddDecFactor             = 2;
static const unsigned long kContSuccessThreshold     = 1;

namespace scribe {

BufferStore::BufferStore(StoreQueue* storeq,
                        const string& category,
                        bool multiCategory)
  : Store(storeq, category, "buffer", multiCategory),
    bufferSendRate_(kDefaultBufferStoreSendRate),
    avgRetryInterval_(kDefaultBufferStoreAvgRetryInterval),
    retryIntervalRange_(kDefaultBufferStoreRetryIntervalRange),
    replayBuffer_(true),
    adaptiveBackoff_(false),
    minRetryInterval_(kDefaultMinRetry),
    maxRetryInterval_(kDefaultMaxRetry),
    maxRandomOffset_(kDefaultRandomOffsetRange),
    retryInterval_(kDefaultMinRetry),
    numContSuccess_(0),
    state_(DISCONNECTED),
    flushStreaming_(false),
    maxByPassRatio_(kDefaultBufferStoreBypassMaxQSizeRatio) {

    lastOpenAttempt_ = time(NULL);

  // we can't open the client conection until we get configured
}

BufferStore::~BufferStore() {

}

void BufferStore::configure(StoreConfPtr configuration, StoreConfPtr parent) {
  Store::configure(configuration, parent);

  // Constructor defaults are fine if these don't exist
  configuration->getUnsigned("buffer_send_rate", &bufferSendRate_);

  // Used for linear backoff case
  configuration->getUnsigned("retry_interval",
                             (unsigned long*) &avgRetryInterval_);
  configuration->getUnsigned("retry_interval_range",
                             (unsigned long*) &retryIntervalRange_);

  // Used in case of adaptive backoff
  // max_random_offset should be some fraction of max_retry_interval
  // 20% of max_retry_interval should be a decent value
  // if you are using max_random_offset > max_retry_interval you should
  // probably not be using adaptive backoff and using retry_interval and
  // retry_interval_range parameters to do linear backoff instead
  configuration->getUnsigned("min_retry_interval",
                             (unsigned long*) &minRetryInterval_);
  configuration->getUnsigned("max_retry_interval",
                             (unsigned long*) &maxRetryInterval_);
  configuration->getUnsigned("max_random_offset",
                             (unsigned long*) &maxRandomOffset_);
  if (maxRandomOffset_ > maxRetryInterval_) {
    LOG_OPER("Warning max_random_offset > max_retry_interval look at using "
             "adaptive_backoff=no instead setting max_random_offset to "
             "max_retry_interval");
    maxRandomOffset_ = maxRetryInterval_;
  }

  string tmp;
  if (configuration->getString("replay_buffer", &tmp) && tmp != "yes") {
    replayBuffer_ = false;
  }

  if (configuration->getString("flush_streaming", &tmp) && tmp == "yes") {
    flushStreaming_ = true;
  }

  if (configuration->getString("buffer_bypass_max_ratio", &tmp)) {
    double d = strtod(tmp.c_str(), NULL);
    if (d > 0 && d <= 1) {
      maxByPassRatio_ = d;
    } else {
      LOG_OPER("[%s] Bad config - buffer_bypass_max_ratio <%s> range is (0, 1]",
          categoryHandled_.c_str(), tmp.c_str());
    }
  }

  if (configuration->getString("adaptive_backoff", &tmp) && tmp == "yes") {
    adaptiveBackoff_ = true;
  }

  if (retryIntervalRange_ > avgRetryInterval_) {
    LOG_OPER("[%s] Bad config - retry_interval_range must be less than "
             "retry_interval. Using <%d> as range instead of <%d>",
             categoryHandled_.c_str(), (int)avgRetryInterval_,
             (int)retryIntervalRange_);
    retryIntervalRange_ = avgRetryInterval_;
  }
  if (minRetryInterval_ > maxRetryInterval_) {
    LOG_OPER("[%s] Bad config - min_retry_interval must be less than "
             "max_retry_interval. Using <%d> and  <%d>, the default values "
             "instead",
             categoryHandled_.c_str(), kDefaultMinRetry, kDefaultMaxRetry );
    minRetryInterval_ = kDefaultMinRetry;
    maxRetryInterval_ = kDefaultMaxRetry;
  }

  StoreConfPtr secondaryStoreConf;
  if (!configuration->getStore("secondary", &secondaryStoreConf)) {
    string msg("Bad config - buffer store doesn't have secondary store");
    setStatus(msg);
    cout << msg << endl;
  } else {
    string type;
    if (!secondaryStoreConf->getString("type", &type)) {
      string msg("Bad config - buffer secondary store doesn't have a type");
      setStatus(msg);
      cout << msg << endl;
    } else {
      // If replayBuffer_ is true, then we need to create a readable store
      secondaryStore_ = createStore(storeQueue_, type, categoryHandled_,
                                   replayBuffer_, multiCategory_);
      secondaryStore_->configure(secondaryStoreConf, storeConf_);
    }
  }

  StoreConfPtr primaryStoreConf;
  if (!configuration->getStore("primary", &primaryStoreConf)) {
    string msg("Bad config - buffer store doesn't have primary store");
    setStatus(msg);
    cout << msg << endl;
  } else {
    string type;
    if (!primaryStoreConf->getString("type", &type)) {
      string msg("Bad config - buffer primary store doesn't have a type");
      setStatus(msg);
      cout << msg << endl;
    } else {
      primaryStore_ = createStore(storeQueue_, type, categoryHandled_, false,
                                  multiCategory_);
      primaryStore_->configure(primaryStoreConf, storeConf_);
    }
  }

  // If the config is bad we'll still try to write the data to a
  // default location on local disk.
  if (!secondaryStore_) {
    secondaryStore_ = createStore(storeQueue_, "file", categoryHandled_, true,
                                multiCategory_);
  }
  if (!primaryStore_) {
    primaryStore_ = createStore(storeQueue_, "file", categoryHandled_, false,
                               multiCategory_);
  }
}

bool BufferStore::isOpen() {
  return primaryStore_->isOpen() || secondaryStore_->isOpen();
}

bool BufferStore::open() {

  // try to open the primary store, and set the state accordingly
  if (primaryStore_->open()) {
    // in case there are files left over from a previous instance
    changeState(SENDING_BUFFER);

    // If we don't need to send buffers, skip to streaming
    if (!replayBuffer_) {
      // We still switch state to SENDING_BUFFER first just to make sure we
      // can open the secondary store
      changeState(STREAMING);
    }
  } else {
    secondaryStore_->open();
    changeState(DISCONNECTED);
  }

  return isOpen();
}

void BufferStore::close() {
  if (primaryStore_->isOpen()) {
    primaryStore_->flush();
    primaryStore_->close();
  }
  if (secondaryStore_->isOpen()) {
    secondaryStore_->flush();
    secondaryStore_->close();
  }
}

void BufferStore::flush() {
  if (primaryStore_->isOpen()) {
    primaryStore_->flush();
  }
  if (secondaryStore_->isOpen()) {
    secondaryStore_->flush();
  }
}

StorePtr BufferStore::copy(const string &category) {
  StorePtr copied(new BufferStore(storeQueue_, category, multiCategory_));

  BufferStore *store = static_cast<BufferStore*>(copied.get());
  store->bufferSendRate_ = bufferSendRate_;
  store->avgRetryInterval_ = avgRetryInterval_;
  store->retryIntervalRange_ = retryIntervalRange_;
  store->retryInterval_ = retryInterval_;
  store->numContSuccess_ = numContSuccess_;
  store->replayBuffer_ = replayBuffer_;
  store->minRetryInterval_ = minRetryInterval_;
  store->maxRetryInterval_ = maxRetryInterval_;
  store->maxRandomOffset_ = maxRandomOffset_;
  store->adaptiveBackoff_ = adaptiveBackoff_;

  store->primaryStore_ = primaryStore_->copy(category);
  store->secondaryStore_ = secondaryStore_->copy(category);

  return copied;
}

bool BufferStore::handleMessages(LogEntryVectorPtr messages) {

  if (state_ == STREAMING || (flushStreaming_ && state_ == SENDING_BUFFER)) {
    if (primaryStore_->handleMessages(messages)) {
      if (adaptiveBackoff_) {
        setNewRetryInterval(true);
      }
      return true;
    } else {
      changeState(DISCONNECTED);
      g_handler->stats.addCounter(StatCounters::kBufferPrimaryErr, 1);
    }
  }

  if (state_ != STREAMING) {
    // If this fails there's nothing else we can do here.
    return secondaryStore_->handleMessages(messages);
  }

  return false;
}

// handles entry and exit conditions for states
void BufferStore::changeState(BufferState newState) {

  // leaving this state
  switch (state_) {
  case STREAMING:
    secondaryStore_->open();
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
  switch (newState) {
  case STREAMING:
    if (secondaryStore_->isOpen()) {
      secondaryStore_->close();
    }
    break;
  case DISCONNECTED:
    // Do not set status here as it is possible to be in this frequently.
    // Whatever caused us to enter this state should have either set status
    // or chosen not to set status.
    g_handler->incCounter(categoryHandled_, "retries");
    setNewRetryInterval(false);
    lastOpenAttempt_ = time(NULL);
    if (!secondaryStore_->isOpen()) {
      secondaryStore_->open();
    }
    break;
  case SENDING_BUFFER:
    if (!secondaryStore_->isOpen()) {
      secondaryStore_->open();
    }
    break;
  default:
    break;
  }

  LOG_OPER("[%s] Changing state from <%s> to <%s>",
           categoryHandled_.c_str(), stateAsString(state_),
           stateAsString(newState));
  state_ = newState;
}

void BufferStore::periodicCheck() {

  // This class is responsible for checking its children
  primaryStore_->periodicCheck();
  secondaryStore_->periodicCheck();

  time_t now = time(NULL);
  struct tm nowInfo;
  localtime_r(&now, &nowInfo);

  if (state_ == DISCONNECTED) {
    if (now - lastOpenAttempt_ > retryInterval_) {
      if (primaryStore_->open()) {
        // Success.  Check if we need to send buffers from secondary to primary
        if (replayBuffer_) {
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
  if (state_ == SENDING_BUFFER) {
    // if queue size is getting large return so that there is time to forward
    // incoming messages directly to the primary store without buffering to
    // secondary store.
    if (flushStreaming_) {
      uint64_t queueSize = storeQueue_->getSize();
      if(queueSize >= maxByPassRatio_ * g_handler->getMaxQueueSize()) {
        return;
      }
    }

    // Read a group of messages from the secondary store and send them to
    // the primary store. Note that the primary store could tell us to try
    // again later, so this isn't very efficient if it reads too many
    // messages at once. (if the secondary store is a file, the number of
    // messages read is controlled by the max file size)
    // parameter max_size for filestores in the configuration
    try {
      for (unsigned sent = 0; sent < bufferSendRate_; ++sent) {
        LogEntryVectorPtr messages(new LogEntryVector);

        // Reads come complete buffered file
        // this file size is controlled by max_size in the configuration
        if (secondaryStore_->readOldest(messages, &nowInfo)) {

          unsigned long size = messages->size();
          if (size > 0) {
            if (primaryStore_->handleMessages(messages)) {
              secondaryStore_->deleteOldest(&nowInfo);
              if (adaptiveBackoff_) {
                setNewRetryInterval(true);
              }
            } else {

              if (messages->size() != size) {
                // We were only able to process some, but not all of this batch
                // of messages.  Replace this batch of messages with
                // just the messages that were not processed.
                LOG_OPER("[%s] buffer store primary store processed %lu/%lu "
                         "messages", categoryHandled_.c_str(),
                         size - messages->size(), size);

                // Put back un-handled messages
                if (!secondaryStore_->replaceOldest(messages, &nowInfo)) {
                  // Nothing we can do but try to remove oldest messages and
                  // report a loss
                  LOG_OPER("[%s] buffer store secondary store lost %lu messages",
                           categoryHandled_.c_str(), messages->size());
                  g_handler->incCounter(categoryHandled_, "lost",
                                        messages->size());
                  secondaryStore_->deleteOldest(&nowInfo);
                }
              }
              changeState(DISCONNECTED);
              break;
            }
          }  else {
            // else it's valid for read to not find anything but not error
            secondaryStore_->deleteOldest(&nowInfo);
          }
        } else {
          // This is bad news. We'll stay in the sending state
          // and keep trying to read.
          setStatus("Failed to read from secondary store");
          LOG_OPER("[%s] WARNING: buffer store can't read from secondary store",
              categoryHandled_.c_str());
          break;
        }

        if (secondaryStore_->empty(&nowInfo)) {
          LOG_OPER("[%s] No more buffer files to send, switching to "
                   "streaming mode", categoryHandled_.c_str());
          changeState(STREAMING);

          break;
        }
      }
    } catch(const std::exception& e) {
      LOG_OPER("[%s] Failed in secondary to primary transfer ",
          categoryHandled_.c_str());
      LOG_OPER("Exception: %s", e.what());
      setStatus("bufferstore sending_buffer failure");
      changeState(DISCONNECTED);
    }
  }// if state_ == SENDING_BUFFER
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
 * achieve kContSuccessThreshold continuous successful sends to the
 * primary store its retry interval is decreased by kAddDecFactor.
 * Whenever the buffer store fails to send to primary its retry interval
 * is increased by multiplying a kMultIncFactor to it. To avoid thundering
 * herds problems a random offset is added to this new retry interval
 * controlled by 'max_random_offset' config parameter.
 * The range of the retry interval is controlled by config parameters
 * 'min_retry_interval' and 'max_retry_interval'.
 * Currently kContSuccessThreshold, kAddDecFactor and kMultIncFactor
 * are not config parameters. This can be done later if need be.
 *
 *
 * In case adaptive_backoff=no, the new retry interval is calculated
 * using the config parameters 'avg_retry_interval' and
 * 'retry_interval_range'
 */
void BufferStore::setNewRetryInterval(bool success) {

  if (adaptiveBackoff_) {
    time_t prevRetryInterval = retryInterval_;
    if (success) {
      numContSuccess_++;
      if (numContSuccess_ >= kContSuccessThreshold) {
        if (retryInterval_ > kAddDecFactor) {
          retryInterval_ -= kAddDecFactor;
        }
        else {
          retryInterval_ = minRetryInterval_;
        }
        if (retryInterval_ < minRetryInterval_) {
          retryInterval_ = minRetryInterval_;
        }
        numContSuccess_ = 0;
      }
      else {
        return;
      }
    }
    else {
      retryInterval_ = static_cast <time_t> (retryInterval_*kMultIncFactor);
      retryInterval_ += (rand() % maxRandomOffset_);
      if (retryInterval_ > maxRetryInterval_) {
        retryInterval_ = maxRetryInterval_;
      }
      numContSuccess_ = 0;
    }
    // prevent unnecessary prints
    if (prevRetryInterval == retryInterval_) {
      return;
    }
  }
  else {
    retryInterval_ = avgRetryInterval_ - retryIntervalRange_/2
                    + rand() % retryIntervalRange_;
  }
  LOG_OPER("[%s] choosing new retry interval <%lu> seconds",
           categoryHandled_.c_str(),
           (unsigned long) retryInterval_);
}

const char* BufferStore::stateAsString(BufferState state) {
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

string BufferStore::getStatus() {

  // This order is intended to give precedence to the errors
  // that are likely to be the worst. We can handle a problem
  // with the primary store, but not the secondary.
  string returnStatus = secondaryStore_->getStatus();
  if (returnStatus.empty()) {
    returnStatus = Store::getStatus();
  }
  if (returnStatus.empty()) {
    returnStatus = primaryStore_->getStatus();
  }
  return returnStatus;
}

} //! namespace scribe
