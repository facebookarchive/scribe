//  Copyright (c) 2007-2010 Facebook
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
// @author Ying-Yi Liang

#ifndef SCRIBE_STAT_COUNTERS_H
#define SCRIBE_STAT_COUNTERS_H

namespace scribe {

/*
 * Holding a set of fb303 statistic counters
 */
// note: this class uses global g_handler
class StatCounters : private boost::noncopyable {
public:
  StatCounters() {}
  void initCounters();
  void addCounter(const char* const key, int64_t value = 1);

  static const char* const kScribedDfqs;
  static const char* const kScribedDfrate;
  static const char* const kScribedIn;
  static const char* const kScribedAdmit;
  static const char* const kScribedIgnore;
  static const char* const kStoreQueue;
  static const char* const kStoreQueueIn;
  static const char* const kStoreQueueOut;
  static const char* const kStoreQueueRequeue;
  static const char* const kStoreQueueLost;
  static const char* const kBufferPrimaryErr;
  static const char* const kNullIn;
  static const char* const kFileIn;
  static const char* const kFileWritten;
  static const char* const kFileRead;
  static const char* const kFileWrittenBytes;
  static const char* const kFileReadBytes;
  static const char* const kFileLostBytes;
  static const char* const kFileOpenErr;
  static const char* const kFileWriteErr;
  static const char* const kNetworkIn;
  static const char* const kNetworkSent;
  static const char* const kNetworkDisconnectErr;

  /**
   * Change the value of the key by an amount, and report the
   * value.
   */
  void incStoreQueueSize(int64_t change);

private:
  // The total number of messages in all store queues.
  int64_t  totalStoreQueueSize_;
  Mutex    totalStoreQueueSizeMutex_;
};

} //! namespace scribe

#endif //! SCRIBE_STAT_COUNTERS_H
