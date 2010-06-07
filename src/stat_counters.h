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

#ifndef STAT_COUNTERS_H
#define STAT_COUNTERS_H

/*
 * Holding a set of fb303 statistic counters
 */
// note: this class uses global g_Handler
class StatCounters {
public:
  StatCounters() {}
  void initCounters();
  void addCounter(const char* const key, int64_t value = 1);

  static const char* const SCRIBED_IN;
  static const char* const SCRIBED_ADMIT;
  static const char* const SCRIBED_IGNORE;
  static const char* const STORE_QUEUE_IN;
  static const char* const STORE_QUEUE_OUT;
  static const char* const STORE_QUEUE_REQUEUE;
  static const char* const STORE_QUEUE_LOST;
  static const char* const BUFFER_PRIMARY_ERR;
  static const char* const FILE_IN;
  static const char* const FILE_WRITTEN;
  static const char* const FILE_READ;
  static const char* const FILE_WRITTEN_BYTES;
  static const char* const FILE_READ_BYTES;
  static const char* const FILE_LOST_BYTES;
  static const char* const FILE_OPEN_ERR;
  static const char* const FILE_WRITE_ERR;
  static const char* const NETWORK_IN;
  static const char* const NETWORK_SENT;
  static const char* const NETWORK_DISCONNECT_ERR;
};

#endif // STAT_COUNTERS_H
