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

#include "Common.h"
#include "ScribeServer.h"
#include "StatCounters.h"

using namespace facebook;
using namespace scribe::thrift;  // for Guards and Mutexes

namespace scribe {

void StatCounters::initCounters() {
  // initialize counters
  // adding kScribedIn rate, count, and histogram with bucket width of 500,
  // where max is 20000 messages
  g_handler->addHistAndStatExports(kScribedIn,
      "RATE,COUNT,75,95", 500, 100, 20000);
  g_handler->addStatExportType(kScribedAdmit,           stats::RATE);
  g_handler->addStatExportType(kScribedIgnore,          stats::SUM);
  g_handler->addStatExportType(kScribedDfqs,            stats::SUM);
  g_handler->addStatExportType(kScribedDfqs,            stats::COUNT);
  g_handler->addStatExportType(kScribedDfqs,            stats::RATE);
  g_handler->addStatExportType(kScribedDfrate,          stats::COUNT);
  g_handler->addStatExportType(kScribedDfrate,          stats::SUM);
  g_handler->addStatExportType(kScribedDfrate,          stats::RATE);

  g_handler->addStatExportType(kStoreQueue,             stats::AVG);
  g_handler->addStatExportType(kStoreQueueIn,           stats::RATE);
  g_handler->addStatExportType(kStoreQueueOut,          stats::RATE);
  g_handler->addStatExportType(kStoreQueueRequeue,      stats::RATE);
  g_handler->addStatExportType(kStoreQueueLost,         stats::SUM);

  g_handler->addStatExportType(kBufferPrimaryErr,       stats::SUM);

  g_handler->addStatExportType(kNullIn,                 stats::RATE);

  g_handler->addStatExportType(kFileIn,                 stats::RATE);
  g_handler->addStatExportType(kFileWritten,            stats::RATE);
  g_handler->addStatExportType(kFileWrittenBytes,       stats::RATE);
  g_handler->addStatExportType(kFileRead,               stats::RATE);
  g_handler->addStatExportType(kFileReadBytes,          stats::RATE);
  g_handler->addStatExportType(kFileLostBytes,          stats::SUM);
  g_handler->addStatExportType(kFileOpenErr,            stats::SUM);
  g_handler->addStatExportType(kFileWriteErr,           stats::SUM);

  g_handler->addStatExportType(kNetworkIn,              stats::RATE);
  g_handler->addStatExportType(kNetworkSent,            stats::RATE);
  g_handler->addStatExportType(kNetworkDisconnectErr,   stats::SUM);
  g_handler->addStatExportType(kNetworkDisconnectErr,   stats::COUNT);

  totalStoreQueueSize_ = 0;
}

void StatCounters::addCounter(const char* const key, int64_t value) {
  g_handler->addStatValue(key, value);
}
void StatCounters::incStoreQueueSize(int64_t change) {
  Guard g(totalStoreQueueSizeMutex_);

  totalStoreQueueSize_ += change;
  addCounter(kStoreQueue, totalStoreQueueSize_);
}

// defines the counter names
#define make_key(component, counter)   ("scribe." component "." counter)
const char* const StatCounters::kScribedIn =
  make_key("scribed", "msg_in");
const char* const StatCounters::kScribedAdmit =
  make_key("scribed", "msg_admitted");
const char* const StatCounters::kScribedIgnore =
  make_key("scribed", "msg_ignored");
const char* const StatCounters::kScribedDfqs =
  make_key("scribed", "dfqs");
const char* const StatCounters::kScribedDfrate =
  make_key("scribed", "dfrate");

const char* const StatCounters::kStoreQueue =
  make_key("store_queue", "msg_in_queue");
const char* const StatCounters::kStoreQueueIn =
  make_key("store_queue", "msg_enqueued");
const char* const StatCounters::kStoreQueueOut =
  make_key("store_queue", "msg_dequeued");
const char* const StatCounters::kStoreQueueRequeue =
  make_key("store_queue", "msg_requeued");
const char* const StatCounters::kStoreQueueLost =
  make_key("store_queue", "msg_lost");

const char* const StatCounters::kBufferPrimaryErr =
  make_key("buffer_store", "err_primary");

const char* const StatCounters::kNullIn =
  make_key("null_store", "msg_in");
const char* const StatCounters::kFileIn =
  make_key("file_store", "msg_in");
const char* const StatCounters::kFileWritten =
  make_key("file_store", "msg_written");
const char* const StatCounters::kFileRead =
  make_key("file_store", "msg_read");
const char* const StatCounters::kFileWrittenBytes =
  make_key("file_store", "bytes_written");
const char* const StatCounters::kFileReadBytes =
  make_key("file_store", "bytes_read");
const char* const StatCounters::kFileLostBytes =
  make_key("file_store", "bytes_lost");
const char* const StatCounters::kFileOpenErr =
  make_key("file_store", "err_open");
const char* const StatCounters::kFileWriteErr =
  make_key("file_store", "err_write");

const char* const StatCounters::kNetworkIn =
  make_key("network_store", "msg_in");
const char* const StatCounters::kNetworkSent =
  make_key("network_store", "msg_sent");
const char* const StatCounters::kNetworkDisconnectErr =
  make_key("network_store", "err_disconnect");
#undef make_key

} //! namespace scribe
