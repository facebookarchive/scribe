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
// @author John Song

#ifndef SCRIBE_DYNAMIC_BUCKET_UPDATER_H
#define SCRIBE_DYNAMIC_BUCKET_UPDATER_H

#include "common.h"
#include "conf.h"

using std::string;
using std::map;

/**
  * DynamicBucketUpdater updates a bucket store's bucket id to host:port
  * mapping periodically using the bucketupdater.thrift interface.
  */
class DynamicBucketUpdater {
 public:
  // bucket updater connection error
  static const char *FB303_ERR_CONNECT;
  // error calling bucketupdater.thrift
  static const char *FB303_ERR_THRIFTCALL;
  // bucket updater return empty result
  static const char *FB303_ERR_EMPTYRESULT;
  // number of times a remote updater has been called
  static const char *FB303_REMOTEUPDATE;
  // number of buckets that have been updated
  static const char *FB303_BUCKETSUPDATED;
  // missing a bid mapping
  static const char *FB303_ERR_NOMAPPING;

  static bool getHost(const std::string& category, const StoreConf* pconf, std::string& host, uint32_t& port);

  static bool isConfigValid(const std::string& category, const StoreConf* pconf);

  /**
    * Return host, port given a key and bucket id, bid, combination.
    * If a mapping is found, the result will be returned in out parameter and
    * function returns true.  Otherwise, function returns false and output
    * parameters are not modified.
    *
    * @param fbBase ponter to FacebookBase
    * @param category the category name, or any identifier that uniquely
    *        identifies a bucket store.
    * @param ttl ttl in seconds
    * @param bid bucket id
    * @param host the output parameter that receives the host output.
    *        If no mapping is found, this variable is not modified.
    * @param port the output parameter that receives the host output.
    *        If no mapping is found, this variable is not modified.
    * @param service service name
    * @param connTimeout connection timeout
    * @param sendTimeout send timeout
    * @param recvTimeout receive timeout
    */
  static bool getHost(facebook::fb303::FacebookBase *fbBase,
                      const string &category,
                      uint32_t ttl,
                      uint64_t bid,
                      string &host,
                      uint32_t &port,
                      string service,
                      string serviceOption,
                      uint32_t connTimeout = 150,
                      uint32_t sendTimeout = 150,
                      uint32_t recvTimeout = 150);

  /**
    * Return host, port given a key and bucket id, bid, combination.
    * If a mapping is found, the result will be returned in out parameter and
    * function returns true.  Otherwise, function returns false and output
    * parameters are not modified.
    *
    * @param fbBase ponter to FacebookBase
    * @param category the category name, or any identifier that uniquely
    *        identifies a bucket store.
    * @param ttl ttl in seconds
    * @param bid bucket id
    * @param host the output parameter that receives the host output.
    *        If no mapping is found, this variable is not modified.
    * @param port the output parameter that receives the host output.
    *        If no mapping is found, this variable is not modified.
    * @param updateHost remote host to fetch mapping
    * @param updatePort remote port to fetch mapping
    * @param connTimeout connection timeout
    * @param sendTimeout send timeout
    * @param recvTimeout receive timeout
    */
  static bool getHost(facebook::fb303::FacebookBase *fbBase,
                      const string &category,
                      uint32_t ttl,
                      uint64_t bid,
                      string &host,
                      uint32_t &port,
                      string updateHost,
                      uint32_t updatePort,
                      uint32_t connTimeout = 150,
                      uint32_t sendTimeout = 150,
                      uint32_t recvTimeout = 150);

 protected:
  /**
    * actual implementation of getHost.
    *
    * @param category the category name, or any identifier that uniquely
    *        identifies a bucket store.
    * @param ttl ttl in seconds
    * @param bid bucket id
    * @param host the output parameter that receives the host output.
    *        If no mapping is found, this variable is not modified.
    * @param port the output parameter that receives the host output.
    *        If no mapping is found, this variable is not modified.
    */
  bool getHostInternal(const string &category,
                      uint32_t ttl,
                      uint64_t bid,
                      string &host,
                      uint32_t &port,
                      string updateHost,
                      uint32_t updatePort,
                      uint32_t connTimeout,
                      uint32_t sendTimeout,
                      uint32_t recvTimeout);
  /**
    * Given a category name, remote host:port, current time, and category
    * mapping, performs a periodic update.  The current mapping will be
    * removed first before update is performed.
    *
    * This function takes care of try/catch and locking.  The bulk of the
    * update logic is delegated to updateInternal.
    *
    * @param category category or key that uniquely identifies this updater.
    * @param ttl ttl in seconds
    * @param host remote host that will be used to retrieve bucket mapping
    * @param port remote port that will be used to retrieve bucket mapping
    * @param connTimeout connection timeout
    * @param sendTimeout send time out
    * @param recvTimeout receive time out
    *
    * @return true if successful. false otherwise.
    */
  bool periodicCheck(string category,
                     uint32_t ttl,
                     string host,
                     uint32_t port,
                     uint32_t connTimeout = 150,
                     uint32_t sendTimeout = 150,
                     uint32_t recvTimeout = 150);

  struct HostEntry {
    string     host_;
    uint32_t   port_;
  };

  struct CategoryEntry {
    CategoryEntry() {}
    CategoryEntry(string category, uint32_t ttl) : category_(category), ttl_(ttl),
                                                   lastUpdated_(0) {
    }

    string    category_;
    uint32_t  ttl_;
    time_t    lastUpdated_;
    map<uint64_t, HostEntry> bidMap_;
  };

  // category and bid to HostEntry map
  typedef map<string, CategoryEntry> CatBidToHostMap;

  /**
    * Given a category name, remote host and port, query bucket mapping
    * using bucketupdater thrift interface and update internal category,
    * bucket id to host mappings.
    *
    * @param category category or other uniquely identifiable key
    * @param ttl ttl in seconds
    * @param remoteHost remote host that will be used to retrieve bucket mapping
    * @param remotePort remote port that will be used to retrieve bucket mapping
    * @param connTimeout connection timeout
    * @param sendTimeout send time out
    * @param recvTimeout receive time out
    *
    * @return true if successful. false otherwise.
    */
  bool updateInternal(string category,
                      uint32_t ttl,
                      string remoteHost,
                      uint32_t remotePort,
                      uint32_t connTimeout,
                      uint32_t sendTimeout,
                      uint32_t recvTimeout);

  static DynamicBucketUpdater *getInstance(facebook::fb303::FacebookBase *fbBase);

  /**
    * Setup fb303 counters.
    */
  void initFb303Counters();

  static DynamicBucketUpdater *instance_;
  static apache::thrift::concurrency::Mutex instanceLock_;
  facebook::fb303::FacebookBase *fbBase_;
  apache::thrift::concurrency::Mutex lock_;
  CatBidToHostMap catMap_;

  void addStatValue(string name, uint64_t value) {
#ifdef FACEBOOK
    if (fbBase_) {
      fbBase_->addStatValue(name, value);
    }
#endif
  }

  // make singleton
  DynamicBucketUpdater(facebook::fb303::FacebookBase *fbBase)
      : fbBase_(fbBase) {
    initFb303Counters();
  }

  DynamicBucketUpdater(const DynamicBucketUpdater& other) {}
};

#endif // SCRIBE_DYNAMIC_BUCKET_UPDATER_H
