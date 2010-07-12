//  Copyright (c) 2007-2009 Facebook
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
// @author Avinash Lakshman
// @author Anthony Giardullo

#ifndef SCRIBE_SERVER_H
#define SCRIBE_SERVER_H

#include "Store.h"
#include "StoreQueue.h"
#include "StatCounters.h"

namespace scribe {

typedef vector<StoreQueuePtr>       StoreList;
typedef shared_ptr<StoreList>       StoreListPtr;
typedef map<string, StoreListPtr>   CategoryMap;

class ScribeHandler : virtual public  thrift::scribeIf,
                              public  fb303::FacebookBase,
                              private boost::noncopyable {

 public:
  ScribeHandler(unsigned long port, const string& confFile);
  ~ScribeHandler();

  void shutdown();
  void initialize();
  void reinitialize();

  ResultCode Log(const vector<LogEntry>& messages);

  void getVersion(string& retVal) {retVal = kScribeVersion;}
  fb303::fb_status getStatus();
  void getStatusDetails(string& retVal);
  void setStatus(fb303::fb_status newStatus);
  void setStatusDetails(const string& newStatusDetails);

  unsigned long port; // it's long because that's all I implemented
                          // in the conf class

  // number of threads processing new Thrift connections
  size_t numThriftServerThreads;

  inline unsigned long getMaxConn() {
    return maxConn_;
  }
  inline unsigned long getMaxConcurrentRequests() {
    return maxConcurrentReq_;
  }

  inline uint64_t getMaxQueueSize() {
    return maxQueueSize_;
  }

  inline unsigned int getPendingRequestCount() {
    return server_->getThreadManager()->pendingTaskCount();
  }

  inline const StoreConf& getConfig() const {
    return config_;
  }

  void incCounter(const string& category, const string& counter);
  void incCounter(const string& category, const string& counter,
                  long amount);
  void incCounter(const string& counter);
  void incCounter(const string& counter, long amount);

  inline void setServer(
      shared_ptr<thrift::TNonblockingServer>& server) {
    this->server_ = server;
  }

  // Hop Latency is the latency between last hop receiving the message and this
  // scribe receiving the message.
  void reportLatencyHop(const string& category, long ms);
  // Writer Latency is the latency from receiving the message to writing it out
  void reportLatencyWriter(const string& category, long ms);

  // fb303 statistic counters
  StatCounters stats;

 private:
  shared_ptr<thrift::TNonblockingServer> server_;

  unsigned long checkPeriod_;// periodic check interval for all contained stores

  // This map has an entry for each configured category.
  // Each of these entries is a map of type->StoreQueue.
  // The StoreQueue contains a store, which could contain additional stores.
  CategoryMap categories_;
  CategoryMap categoryPrefixes_;

  // the default stores
  StoreList defaultStores_;

  string configFilename_;
  fb303::fb_status status_;
  string statusDetails_;
  Mutex statusLock_;
  time_t lastMsgTime_;
  unsigned long numMsgLastSecond_;
  unsigned long maxMsgPerSecond_;
  unsigned long maxConn_;
  unsigned long maxConcurrentReq_;
  uint64_t      maxQueueSize_;
  StoreConf config_;
  bool newThreadPerCategory_;
  float timeStampSampleRate_;

  /* mutex to syncronize access to ScribeHandler.
   * A single mutex is fine since it only needs to be locked in write mode
   * during start/stop/reinitialize or when we need to create a new category.
   */
  shared_ptr<ReadWriteMutex> scribeHandlerLock_;

  // disallow empty construction
  ScribeHandler();

 protected:
  bool throttleDeny(int numMessages); // returns true if overloaded
  void deleteCategoryMap(CategoryMap& cats);
  const char* statusAsString(fb303::fb_status newStatus);
  bool createCategoryFromModel(const string &category,
                               const StoreQueuePtr &model);
  StoreQueuePtr configureStoreCategory(StoreConfPtr storeConf,
                           const string &category,
                           const StoreQueuePtr &model,
                           bool categoryList=false);
  bool configureStore(StoreConfPtr storeConf, int* numStores);
  void stopStores();
  bool throttleRequest(const vector<LogEntry>&  messages);
  StoreListPtr createNewCategory(const string& category);
  void addMessage(const LogEntry& entry, const StoreListPtr& storeList);
  void reportLatency(const string& category, const string& type, long ms);
};
extern shared_ptr<ScribeHandler> g_handler;

} //! namespace scribe

#endif //! SCRIBE_SERVER_H
