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

#include "common.h"
#include "stat_counters.h"
#include "scribe_server.h"

using namespace facebook;
using namespace facebook::fb303;

void StatCounters::initCounters() {
  // initialize counters
  g_Handler->addStatExportType(SCRIBED_IN,              stats::RATE);
  g_Handler->addStatExportType(SCRIBED_ADMIT,           stats::RATE);
  g_Handler->addStatExportType(SCRIBED_IGNORE,          stats::SUM);

  g_Handler->addStatExportType(STORE_QUEUE_IN,          stats::RATE);
  g_Handler->addStatExportType(STORE_QUEUE_OUT,         stats::RATE);
  g_Handler->addStatExportType(STORE_QUEUE_REQUEUE,     stats::RATE);
  g_Handler->addStatExportType(STORE_QUEUE_LOST,        stats::SUM);

  g_Handler->addStatExportType(BUFFER_PRIMARY_ERR,      stats::SUM);

  g_Handler->addStatExportType(FILE_IN,                 stats::RATE);
  g_Handler->addStatExportType(FILE_WRITTEN,            stats::RATE);
  g_Handler->addStatExportType(FILE_WRITTEN_BYTES,      stats::RATE);
  g_Handler->addStatExportType(FILE_READ,               stats::RATE);
  g_Handler->addStatExportType(FILE_READ_BYTES,         stats::RATE);
  g_Handler->addStatExportType(FILE_LOST_BYTES,         stats::SUM);
  g_Handler->addStatExportType(FILE_OPEN_ERR,           stats::SUM);
  g_Handler->addStatExportType(FILE_WRITE_ERR,          stats::SUM);

  g_Handler->addStatExportType(NETWORK_IN,              stats::RATE);
  g_Handler->addStatExportType(NETWORK_SENT,            stats::RATE);
  g_Handler->addStatExportType(NETWORK_DISCONNECT_ERR,  stats::SUM);
  g_Handler->addStatExportType(NETWORK_DISCONNECT_ERR,  stats::COUNT);
}

void StatCounters::addCounter(const char* const key, int64_t value) {
  g_Handler->addStatValue(key, value);
}

// defines the counter names
#define make_key(component, counter)   ("scribe." component "." counter)
const char* const StatCounters::SCRIBED_IN =
  make_key("scribed", "msg_in");
const char* const StatCounters::SCRIBED_ADMIT =
  make_key("scribed", "msg_admitted");
const char* const StatCounters::SCRIBED_IGNORE =
  make_key("scribed", "msg_ignored");

const char* const StatCounters::STORE_QUEUE_IN =
  make_key("store_queue", "msg_enqueued");
const char* const StatCounters::STORE_QUEUE_OUT =
  make_key("store_queue", "msg_dequeued");
const char* const StatCounters::STORE_QUEUE_REQUEUE =
  make_key("store_queue", "msg_requeued");
const char* const StatCounters::STORE_QUEUE_LOST =
  make_key("store_queue", "msg_lost");

const char* const StatCounters::BUFFER_PRIMARY_ERR =
  make_key("buffer_store", "err_primary");

const char* const StatCounters::FILE_IN =
  make_key("file_store", "msg_in");
const char* const StatCounters::FILE_WRITTEN =
  make_key("file_store", "msg_written");
const char* const StatCounters::FILE_READ =
  make_key("file_store", "msg_read");
const char* const StatCounters::FILE_WRITTEN_BYTES =
  make_key("file_store", "bytes_written");
const char* const StatCounters::FILE_READ_BYTES =
  make_key("file_store", "bytes_read");
const char* const StatCounters::FILE_LOST_BYTES =
  make_key("file_store", "bytes_lost");
const char* const StatCounters::FILE_OPEN_ERR =
  make_key("file_store", "err_open");
const char* const StatCounters::FILE_WRITE_ERR =
  make_key("file_store", "err_write");

const char* const StatCounters::NETWORK_IN =
  make_key("network_store", "msg_in");
const char* const StatCounters::NETWORK_SENT =
  make_key("network_store", "msg_sent");
const char* const StatCounters::NETWORK_DISCONNECT_ERR =
  make_key("network_store", "err_disconnect");
#undef make_key
