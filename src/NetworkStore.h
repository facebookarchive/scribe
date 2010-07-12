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

#ifndef SCRIBE_NETWORK_STORE_H
#define SCRIBE_NETWORK_STORE_H

#include "Common.h"
#include "Conf.h"
#include "ConnPool.h"
#include "NetworkDynamicConfig.h"
#include "Store.h"

namespace scribe {

/*
 * This store sends messages to another scribe server.
 * This class is really just an adapter to the global
 * connection pool g_connPool.
 */
class NetworkStore : public Store {

 public:
  NetworkStore(StoreQueue* storeq,
               const string& category,
               bool multiCategory);
  ~NetworkStore();

  StorePtr copy(const string &category);
  bool handleMessages(LogEntryVectorPtr messages);
  bool open();
  bool isOpen();
  void configure(StoreConfPtr configuration, StoreConfPtr parent);
  void close();
  void flush();
  void periodicCheck();

 protected:
  void incrementSentCounter(const string& host, unsigned long port,
                            unsigned long numMsg, unsigned long numBytes);
  // configuration
  bool useConnPool_;
  bool serviceBased_;
  long timeout_;
  string remoteHost_;
  unsigned long remotePort_; // long because it works with config code
  string serviceName_;
  string serviceOptions_;
  ServerVector servers_;
  unsigned long serviceCacheTimeout_;
  NetworkDynamicConfigMod* configMod_;

  // state
  bool opened_;
  shared_ptr<ScribeConn> unpooledConn_; // null if useConnPool
  time_t lastServiceCheck_;

 private:
  // disallow copy, assignment, and empty construction
  NetworkStore();
  NetworkStore(NetworkStore& rhs);
  NetworkStore& operator=(NetworkStore& rhs);
};

} //! namespace scribe

#endif //! SCRIBE_NETWORK_STORE_H
