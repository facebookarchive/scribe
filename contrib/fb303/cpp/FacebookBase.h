/*
 * Copyright (c) 2006- Facebook
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#ifndef _FACEBOOK_TB303_FACEBOOKBASE_H_
#define _FACEBOOK_TB303_FACEBOOKBASE_H_ 1

#include "fb303/cpp/gen-cpp/FacebookService.h"

#include "thrift/server/TServer.h"
#include "thrift/concurrency/Mutex.h"
#include "stats/DynamicCounters.h"
#include "stats/ExportedTimeseries.h"
#include "stats/ExportedHistogram.h"

#include <map>
#include <string>
#include <time.h>

namespace facebook { namespace fb303 {

using std::map;
using std::vector;
using std::string;

using apache::thrift::concurrency::Mutex;
using apache::thrift::concurrency::ReadWriteMutex;
using apache::thrift::server::TServer;

struct ReadWriteInt : ReadWriteMutex {int64_t value;};
struct ReadWriteCounterMap : ReadWriteMutex,
                             map<string, ReadWriteInt> {};

struct ReadWriteString : ReadWriteMutex {string value;};
struct ReadWriteStringMap : ReadWriteMutex,
                            map<string, ReadWriteString> {};


/**
 * Base Facebook service implementation in C++.
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
class FacebookBase : virtual public FacebookServiceIf {
 public:
  void getName(string& _return);
  virtual void getVersion(string& _return) { _return = ""; }

  virtual fb_status getStatus() = 0;
  virtual void getStatusDetails(string& _return) { _return = ""; }

  /**
   * Returns a string representation of the given status.
   */
  void getStatusAsString(string& _return);
  static void getStatusAsString(string& _return, fb_status status);

  void setOption(const string& key, const string& value);
  void getOption(string& _return, const string& key);
  void getOptions(map<string, string> & _return);

  int64_t aliveSince();

  virtual void reinitialize() {}

  virtual void shutdown() {
    if (server_.get() != NULL) {
      server_->stop();
    }
  }

  stats::ExportedStatMap* getStatMap() { return &statsMap_; }
  stats::ExportedHistogramMap* getHistogramMap() { return &histMap_; }
  stats::DynamicCounters* getDynamicCounters() { return &dynamicCounters_; }
  stats::DynamicStrings* getDynamicStrings() { return &dynamicStrings_; }

  /**
   * Adds a value to the historical statistics for a given key.
   *
   * Please see the documentation for addStatExportType() to see how to make
   * these statistics accessible via calls to getCounters().
   *
   * Also, you can query these statistics via calls similar to:
   *
   *  facebookBaseObj->getStatMap()->getStat("keyName")->get{Sum,Avg,Rate,Count}
   *
   * See the documentation of ../../stats/Timeseries.h for more info.
   */
  virtual void addStatValue(const string& key, int64_t value = 1);
  virtual void addStatValueAggregated(const string& key,
                                      int64_t sum,
                                      int64_t numSamples);

  /**
   * Adds a value to the historical histograms for a given key.
   *
   * Please see the documentation for exportHistogramPercentile() and
   * addHistogram() to see how to configure histograms and their export
   * options.
   *
   * see the documenttion in ../../stats/ExportedHistogramMap.h for more info.
   */
  virtual void addHistogramValue(const string& key, int64_t value);

  /**
   * Convenience function for adding the same value to stats and histgrams.
   * Creates AVG stat if no stat exists, but fatals if no histogram has been
   * created yet.
   */
  virtual void addHistAndStatValue(const string& key, int64_t value);

  /**
   * Defines a histogram that can be used via calls to addHistogramValue() and
   * further configured via exportHistogramPercentile().  Registers the
   * histogram so that its buckets are retrievable via getExportedValues().
   *
   * The histogram will contain buckets 'bucketWidth' wide, with the lowest
   * bucket starting at 'min' and highest ending at 'max'.  Two additional
   * buckets will be implicitly created for data "below min" and "above max".
   *
   * The default histogram will have the standard 60/600/3600/inf bucketing;
   * if you desire different time buckets for your histogram, use the other
   * form of 'addHistogram()' which allows you to pass a prototype.
   */
  virtual void addHistogram(const string& key,
                            int64_t bucketWidth,
                            int64_t min,
                            int64_t max);
  /**
   * Defines a histogram that can be used via calls to addHistogramValue() and
   * further configured via exportHistogramPercentile().  Registers the
   * histogram so that its buckets are retrievable via getExportedValues().
   *
   * The new histogram's bucket arrangement (and the number and duration of
   * timeseries levels within the buckets) will be copied from the prototype
   * histogram.
   *
   * Please see the documentation for ExportedHistogram to see how to
   * construct different bucket arrangements and time series levels.
   */
  virtual void addHistogram(const string& key,
                            const stats::ExportedHistogram& prototype);

  /**
   *  Convience function for simultaneously adding and exporting stats and a
   *  histogram with several percentiles at once.  The 'stats' input string is a
   *  comma separated list of stats and integers, e.g. "AVG,75,95" would
   *  export the average as well as the 75th and 95th percentiles.  Throws a
   *  lexical cast exception on malformed 'stats' strings.
   */
   virtual void addHistAndStatExports(const string& key,
                               const string& stats,
                               int64_t bucketWidth,
                               int64_t min,
                               int64_t max,
                               const stats::ExportedStat* statPrototype = NULL);

  /**
   * Exports the given stat value to the counters, using the given export
   * type. In other words, after calling for key = "foo", calls to
   * getCounters() will contain several counters of the form:
   *
   * type AVG:   foo.avg    foo.avg.60    foo.avg.600    foo.avg.3600
   * type SUM:   foo.sum    foo.sum.60    foo.sum.600    foo.sum.3600
   * type RATE:  foo.rate   foo.rate.60   foo.rate.600   foo.rate.3600
   * type COUNT: foo.count  foo.count.60  foo.count.600  foo.count.3600
   *
   * The values for these counters will be computed, of course, from data
   * inserted via calls to addStatValue("foo", val).
   *
   * Note that this function can be called multiple times for a given key
   * to create multiple export counter types.  Also note that if this function
   * is not called at all for a given key prior to data insertion, the key will
   * be auto- exported as AVG by default.
   */
  virtual void addStatExportType(const string& key,
                               stats::ExportType exportType = stats::AVG,
                               const stats::ExportedStat* statPrototype = NULL);

  /**
   * Given a histogram already created with addHistogram(), exports a stat value
   * counter of the form: "<key>.hist.p<pct>.<duration>" which is very similar to
   * the counters defined by addStatExportType(), except that instead of other
   * aggregation methods like 'sum' or 'avg, the aggregation method if finding
   * the correct percentile in the dataset, such as 'p95' for example.
   *
   * For instance, a call like: exportHistogramPercentile("req_latency", 95)
   * will result in the following counters being exported:
   *
   *   req_latency.hist.p95
   *   req_latency.hist.p95.60
   *   req_latency.hist.p95.600
   *   req_latency.hist.p95.3600
   */
  virtual void exportHistogramPercentile(const string& key, int pct);




  /*** Increments a "regular-style" flat counter (no historical stats) */
  virtual int64_t incrementCounter(const string& key, int64_t amount = 1);
  /*** Sets a "regular-style" flat counter (no historical stats) */
  virtual int64_t setCounter(const string& key, int64_t value);

  /*** Retrieves all counters, both regular-style and dynamic counters */
  virtual void getCounters(map<string, int64_t>& _return);

  /*** Retrieves a counter value for given key (could be regular or dynamic) */
  virtual int64_t getCounter(const string& key);

  /*** Retrieves a list of counter values (could be regular or dynamic) */
  virtual void getSelectedCounters(std::map<std::string,int64_t>& _return,
                                   const std::vector<std::string>& keys);

  virtual void setExportedValue(const string& key,
                                const string& value);
  virtual void getExportedValues(map<string, string>& _return);
  virtual void getSelectedExportedValues(map<string, string>& _return,
                                         const vector<string>& keys);
  virtual void getExportedValue(string& _return, const string& key);

  /**
   * Set server handle for shutdown method
   */
  void setServer(boost::shared_ptr<TServer> server) {
    server_ = server;
  }

 protected:
  FacebookBase(const string& name);
  virtual ~FacebookBase();

 private:
  /*
   * Sets `out' to the value of a counter (regular or dynamic) named
   * `name', if one exists.  Returns true iff the counter exists.
   */
  bool getCounterValue(const std::string& name, int64_t* out) const;

 private:
  string name_;
  int64_t aliveSince_;

  map<string, string> options_;
  Mutex optionsLock_;

  ReadWriteCounterMap counters_;
  ReadWriteStringMap exported_values_;

  stats::DynamicCounters dynamicCounters_;
  stats::DynamicStrings dynamicStrings_;
  stats::ExportedStatMap statsMap_;
  stats::ExportedHistogramMap histMap_;

  boost::shared_ptr<TServer> server_;
};

}} // facebook::tb303

#endif // _FACEBOOK_TB303_FACEBOOKBASE_H_
