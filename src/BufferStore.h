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

#ifndef SCRIBE_BUFFER_STORE_H
#define SCRIBE_BUFFER_STORE_H

#include "Common.h"
#include "Conf.h"
#include "Store.h"

namespace scribe {

/*
 * This store aggregates messages and sends them to another store
 * in larger groups. If it is unable to do this it saves them to
 * a secondary store, then reads them and sends them to the
 * primary store when it's back online.
 *
 * This actually involves two buffers. Messages are always buffered
 * briefly in memory, then they're buffered to a secondary store if
 * the primary store is down.
 */
class BufferStore : public Store {

 public:
  BufferStore(StoreQueue* storeq,
              const string& category,
              bool multiCategory);
  ~BufferStore();

  StorePtr copy(const string &category);
  bool handleMessages(LogEntryVectorPtr messages);
  bool open();
  bool isOpen();
  void configure(StoreConfPtr configuration, StoreConfPtr parent);
  void close();
  void flush();
  void periodicCheck();

  string getStatus();

 protected:
  // Store we're trying to get the messages to
  StorePtr primaryStore_;

  // Store to use as a buffer if the primary is unavailable.
  // The store must be of a type that supports reading.
  StorePtr secondaryStore_;

  // buffer state machine
  enum BufferState {
    STREAMING,       // connected to primary and sending directly
    DISCONNECTED,    // disconnected and writing to secondary
    SENDING_BUFFER,  // connected to primary and sending data from secondary
  };

  // handles state pre and post conditions
  void changeState(BufferState newState);
  const char* stateAsString(BufferState state);

  void setNewRetryInterval(bool);

  // configuration
  unsigned long bufferSendRate_;  // number of buffer files
                                  // sent each periodicCheck
  time_t avgRetryInterval_;       // in seconds, for retrying primary store open
  time_t retryIntervalRange_;     // in seconds
  bool   replayBuffer_;           // whether to send buffers from
                                  // secondary store to primary
  bool adaptiveBackoff_;          // Adaptive backoff mode indicator
  time_t minRetryInterval_;       // The min the retryInterval can become
  time_t maxRetryInterval_;       // The max the retryInterval can become
  time_t maxRandomOffset_;        // The max random offset added
                                  // to the retry interval


  // state
  time_t retryInterval_;          // the current retry interval in seconds
  unsigned long numContSuccess_;  // number of continuous successful sends
  BufferState state_;
  time_t lastOpenAttempt_;

  bool flushStreaming_;           // When flushStreaming is set to true,
                                  // incoming messages to a buffere store
                                  // that still has buffereed data in the
                                  // secondary store, i.e. buffer store in
                                  // SENDING_BUFFER phase, will be sent to the
                                  // primary store directly.  If false,
                                  // then in coming messages will first
                                  // be written to secondary store, and
                                  // later flushed out to the primary
                                  // store.

  double maxByPassRatio_;         // During the buffer flushing phase, if
                                  // flushStreaming is enabled, the max
                                  // size of message queued before buffer
                                  // flushing yielding to sending current
                                  // incoming messages is calculated by
                                  // multiple max_queue_size with
                                  // buffer_bypass_max_ratio.

 private:
  // disallow copy, assignment, and empty construction
  BufferStore();
  BufferStore(BufferStore& rhs);
  BufferStore& operator=(BufferStore& rhs);
};

} //! namespace scribe

#endif //! SCRIBE_BUFFER_STORE_H
