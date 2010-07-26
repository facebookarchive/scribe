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

#include "fb303/cpp/FacebookBase.h"

#include <fstream>
#include <iostream>
#include <sstream>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/scoped_array.hpp>

namespace facebook { namespace fb303 {

using namespace std;
using namespace facebook;
using namespace facebook::stats;
using apache::thrift::concurrency::Guard;
using apache::thrift::TException;

FacebookBase::FacebookBase(const string& name)
    : name_(name),
      dynamicCounters_(),
      statsMap_(&dynamicCounters_),
      histMap_(&dynamicCounters_, &dynamicStrings_,
               ExportedHistogram(1000, 0, 10000)) {
  aliveSince_ = (int64_t) time(NULL);
}

FacebookBase::~FacebookBase() {
}

inline void FacebookBase::getName(string& _return) {
  _return = name_;
}

void FacebookBase::getStatusAsString(string& _return) {
  FacebookBase::getStatusAsString(_return, getStatus());
}

void FacebookBase::getStatusAsString(string& _return, fb_status status) {
  switch(status) {
   case DEAD:
    _return = "DEAD";
    break;
   case STARTING:
    _return = "STARTING";
    break;
   case ALIVE:
    _return = "ALIVE";
    break;
   case STOPPING:
    _return = "STOPPING";
    break;
   case STOPPED:
    _return = "STOPPED";
    break;
   case WARNING:
    _return = "WARNING";
    break;
   default:
    _return = "";
  }
}

void FacebookBase::setOption(const string& key, const string& value) {
  Guard g(optionsLock_);
  options_[key] = value;
}

void FacebookBase::getOption(string& _return, const string& key) {
  Guard g(optionsLock_);
  _return = options_[key];
}

void FacebookBase::getOptions(map<string, string> & _return) {
  Guard g(optionsLock_);
  _return = options_;
}

void FacebookBase::addStatValue(const string& key, int64_t amount) {
  statsMap_.addValue(key, time(NULL), amount);
}

void FacebookBase::addStatValueAggregated(const string& key,
                                          int64_t sum, int64_t numSamples) {
  statsMap_.addValueAggregated(key, time(NULL), sum, numSamples);
}

void FacebookBase::addHistAndStatValue(const string& key, int64_t value) {
  time_t now = time(NULL);
  statsMap_.addValue(key, now, value);
  histMap_.addValue(key, now, value);
}

void FacebookBase::addHistogramValue(const string& key, int64_t value) {
  histMap_.addValue(key, time(NULL), value);
}

void FacebookBase::addStatExportType(const string& key,
                                     stats::ExportType type,
                                     const stats::ExportedStat* statPrototype) {
  statsMap_.exportStat(key, type, statPrototype);
}

void FacebookBase::addHistogram(const string& key,
                                int64_t bucketSize,
                                int64_t min,
                                int64_t max) {
  ExportedHistogram hist(bucketSize, min, max);
  histMap_.addHistogram(key, &hist);
}

void FacebookBase::addHistogram(const string& key,
                                const ExportedHistogram& hist) {
  histMap_.addHistogram(key, &hist);
}

void FacebookBase::addHistAndStatExports(const string& key,
                                         const string& stats,
                                         int64_t bucketSize,
                                         int64_t min,
                                         int64_t max,
                                         const ExportedStat* statPrototype) {
  if (statPrototype) {
    ExportedHistogram hist(bucketSize, min, max, *statPrototype);
    histMap_.addHistogram(key, &hist);
  } else {
    ExportedHistogram hist(bucketSize, min, max);
    histMap_.addHistogram(key, &hist);
  }
  vector<string> statsSplit;
  boost::split(statsSplit, stats, boost::is_any_of(","));
  for (vector<string>::const_iterator it = statsSplit.begin();
       it != statsSplit.end();
       ++ it) {
    const string& stat = *it;
    if (stat == "AVG") {
      statsMap_.exportStat(key, stats::AVG, statPrototype);
    } else if (stat == "RATE") {
      statsMap_.exportStat(key, stats::RATE, statPrototype);
    } else if (stat == "SUM") {
      statsMap_.exportStat(key, stats::SUM, statPrototype);
    } else if (stat == "COUNT") {
      statsMap_.exportStat(key, stats::COUNT, statPrototype);
    } else { // No match on stat type - assume it's a histogram percentile
      exportHistogramPercentile(key, boost::lexical_cast<int32_t>(it->c_str()));
    }
  }
}

void FacebookBase::exportHistogramPercentile(const string& key, int pct) {
  histMap_.exportPercentile(key, pct);
}

int64_t FacebookBase::incrementCounter(const string& key, int64_t amount) {
  counters_.acquireRead();

  // if we didn't find the key, we need to write lock the whole map to create
  // it
  ReadWriteCounterMap::iterator it = counters_.find(key);
  if (it == counters_.end()) {
    counters_.release();
    counters_.acquireWrite();

    // we need to check again to make sure someone didn't create this key
    // already while we released the lock
    it = counters_.find(key);
    if (it == counters_.end()) {
      counters_[key].value = amount;
      counters_.release();
      return amount;
    }
  }

  it->second.acquireWrite();
  int64_t count = it->second.value + amount;
  it->second.value = count;
  it->second.release();
  counters_.release();
  return count;
}

int64_t FacebookBase::setCounter(const string& key, int64_t value) {
  counters_.acquireRead();

  // if we didn't find the key, we need to write lock the whole map to create
  // it
  ReadWriteCounterMap::iterator it = counters_.find(key);
  if (it == counters_.end()) {
    counters_.release();
    counters_.acquireWrite();

    // we need to check again to make sure someone didn't create this key
    // already while we released the lock
    it = counters_.find(key);
    if (it == counters_.end()) {
      counters_[key].value = value;
      counters_.release();
      return value;
    }
  }

  it->second.acquireWrite();
  it->second.value = value;
  it->second.release();
  counters_.release();
  return value;
}

void FacebookBase::getCounters(map<string, int64_t>& _return) {
  // we need to lock the whole thing and actually build the map since we don't
  // want our read/write structure to go over the wire
  counters_.acquireRead();
  for (ReadWriteCounterMap::iterator it = counters_.begin();
       it != counters_.end(); it++) {
    _return[it->first] = it->second.value;
  }
  counters_.release();

  dynamicCounters_.getCounters(&_return);
}

int64_t FacebookBase::getCounter(const string& key) {
  int64_t ret;
  return getCounterValue(key, &ret) ? ret : 0;
}

void FacebookBase::getSelectedCounters(
    std::map<std::string,int64_t>& _return,
    const std::vector<std::string>& keys) {
  std::vector<std::string>::const_iterator keyIt;
  for (keyIt = keys.begin(); keyIt != keys.end(); ++ keyIt) {
    int64_t val;
    if (getCounterValue(*keyIt, &val)) {
      _return[*keyIt] = val;
    }
  }
}

void FacebookBase::setExportedValue(const string& key,
                                    const string& value) {
  exported_values_.acquireRead();

  // if we didn't find the key, we need to write lock the whole map to create
  // it
  ReadWriteStringMap::iterator it = exported_values_.find(key);
  if (it == exported_values_.end()) {
    exported_values_.release();
    exported_values_.acquireWrite();

    // we need to check again to make sure someone didn't create this key
    // already while we released the lock
    it = exported_values_.find(key);
    if (it == exported_values_.end()) {
      exported_values_[key].value = value;
      exported_values_.release();
      return;
    }
  }

  it->second.acquireWrite();
  it->second.value = value;
  it->second.release();
  exported_values_.release();
}

void FacebookBase::getExportedValues(map<string,
                                     string>& _return) {
  // we need to lock the whole thing and actually build the map since we don't
  // want our read/write structure to go over the wire
  exported_values_.acquireRead();
  for (ReadWriteStringMap::iterator it = exported_values_.begin();
       it != exported_values_.end(); it++) {
    _return[it->first] = it->second.value;
  }
  exported_values_.release();
  dynamicStrings_.getValues(&_return);
}

void FacebookBase::getSelectedExportedValues(map<string, string>& expValues,
                                             const vector<string>& keys) {
  exported_values_.acquireRead();
  for (vector<string>::const_iterator key = keys.begin();
       key != keys.end();
       ++ key) {
    ReadWriteStringMap::const_iterator it = exported_values_.find(*key);
    if (it != exported_values_.end()) {
      const ReadWriteString* val = &(it->second);
      val->acquireRead();
      expValues[*key] = val->value;
      val->release();
    }
  }
  exported_values_.release();

  for (vector<string>::const_iterator key = keys.begin();
       key != keys.end();
       ++ key) {
    string dynamicValue;
    if (dynamicStrings_.getValue(*key, &dynamicValue)) {
      expValues[*key] = dynamicValue;
    }
  }
}

void FacebookBase::getExportedValue(string& _return,
                                    const string& key) {
  if (dynamicStrings_.getValue(key, &_return)) {
    return;
  }

  exported_values_.acquireRead();
  ReadWriteStringMap::iterator it = exported_values_.find(key);
  if (it != exported_values_.end()) {
    it->second.acquireRead();
    _return = it->second.value;
    it->second.release();
  }
  exported_values_.release();
}

inline int64_t FacebookBase::aliveSince() {
  return aliveSince_;
}

bool FacebookBase::getCounterValue(const std::string& key,
                                   int64_t* out) const
{
  if (dynamicCounters_.getCounter(key, out)) {
    return true;
  }

  bool rv = false;
  counters_.acquireRead();
  ReadWriteCounterMap::const_iterator it = counters_.find(key);
  if (it != counters_.end()) {
    it->second.acquireRead();
    *out = it->second.value;
    it->second.release();
    rv = true;
  }
  counters_.release();
  return rv;
}

} }
