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
 * @author Mark Rabkin (mrabkin@facebook.com)
 *
 */

#include <sys/time.h>

#include <boost/lexical_cast.hpp>
#include <boost/scoped_ptr.hpp>

#include "stats/ExportedHistogram.h"

using namespace facebook;
using namespace boost;
using namespace std;

static inline void LOG(const string &level_string, const string &message)
{
  time_t now = time(NULL);
  char now_pretty[26];
  ctime_r(&now, now_pretty);
  now_pretty[24] = '\0';
  cout << '[' << level_string << "] [" << now_pretty << "] "
       << message << endl;
}

namespace facebook { namespace stats {

class HistogramExporter {
 public:
  static void exportPercentile(const shared_ptr<ExportedHistogram>& hist,
                               const string& name,
                               int percentile,
                               SpinLock* lock,
                               DynamicCounters* counters);

  static void exportBuckets(const shared_ptr<ExportedHistogram>& hist,
                            const string& name,
                            SpinLock* lock,
                            DynamicStrings* strings);
};



static
string getHistogramBuckets(const shared_ptr<ExportedHistogram>& hist,
                           int level,
                           SpinLock* lock) {
  assert(hist);

  boost::scoped_ptr<SpinLockHolder> holder;
  if (lock) {
    holder.reset(new SpinLockHolder(*lock));
  }

  // make sure the histogram is up to date and data is decayed appropriately
  hist->update(time(NULL));

  // return the serialized bucket info
  return hist->getString(level);
}

static
CounterType getHistogramPercentile(const shared_ptr<ExportedHistogram>& hist,
                                   int level,
                                   int percentile,
                                   SpinLock* lock) {
  assert(hist);

  boost::scoped_ptr<SpinLockHolder> holder;
  if (lock) {
    holder.reset(new SpinLockHolder(*lock));
  }

  // make sure the histogram is up to date and data is decayed appropriately
  hist->update(time(NULL));

  // return the estimated percentile value for the given percentile
  return hist->getPercentileEstimate(percentile, level);
}

/* static */
void HistogramExporter::exportBuckets(
      const shared_ptr<ExportedHistogram>& hist,
      const string& name,
      SpinLock* lock,
      DynamicStrings* strings) {
  assert(hist);
  assert(strings != NULL);

  // All the buckets in the histogram are guaranteed to have the same number
  // of levels and the same level durations, so we grab the first bucket.
  //
  // NOTE:  We access the histogram's stat object here without locking.  This
  // depends on the fact that getLevel(), and Level::alltime() and
  // Level::duration() are all non-volatile calls meaning they only read
  // things that are constant once the stat is constructed (number of levels
  // can never change, nor their durations).
  //
  //   - mrabkin
  assert(hist->getNumBuckets() > 0);
  const ExportedHistogram::ContainerType& stat = hist->getBucket(0);

  // now, export each level
  for (int level = 0; level < stat.numLevels(); ++level) {

    string valueName;
    if (stat.getLevel(level).alltime()) {
      // example name: ad_request_elapsed_time.hist
      valueName = name;
      valueName += ".hist";
    } else {
      // example name: ad_request_elapsed_time.hist.600
      valueName = name;
      valueName += ".hist.";
      valueName += lexical_cast<string>(stat.getLevel(level).duration());
    }

    // get the stat function callback and put it in a wrapper that will grab
    // the necessary lock and call update() on the stat
    // register the actual counter callback
    strings->registerCallback(valueName,
      tr1::bind(getHistogramBuckets, hist, level, lock));
  }
}


/* static */
void HistogramExporter::exportPercentile(
      const shared_ptr<ExportedHistogram>& hist,
      const string& name,
      int percentile,
      SpinLock* lock,
      DynamicCounters* counters) {
  assert(hist);
  assert(counters != NULL);
  assert(hist->getNumBuckets() > 0);
  assert(percentile >= 0);
  assert(percentile <= 100);

  const ExportedHistogram::ContainerType& stat = hist->getBucket(0);
  for (int level = 0; level < stat.numLevels(); ++level) {
    // NOTE:  We access the histogram's stat object here without locking.  This
    // depends on the fact that getLevel(), and Level::alltime() and
    // Level::duration() are all non-volatile calls meaning they only read
    // things that are constant once the stat is constructed (number of levels
    // can never change, nor their durations).
    //   - mrabkin

    string counterName;
    if (stat.getLevel(level).alltime()) {
      // example name: ad_request_elapsed_time.p95
      counterName = name;
      counterName += ".p";
      counterName += lexical_cast<string>(percentile);
    } else {
      // example name: ad_request_elapsed_time.p95.600
      counterName = name;
      counterName += ".p";
      counterName += lexical_cast<string>(percentile);
      counterName += '.';
      counterName += lexical_cast<string>(stat.getLevel(level).duration());
    }

    // get the stat function callback and put it in a wrapper that will grab
    // the necessary lock and call update() on the stat
    // register the actual counter callback
    counters->registerCallback(counterName,
      tr1::bind(getHistogramPercentile, hist, level, percentile, lock));
  }
}

ExportedHistogramMap::ExportedHistogramMap(DynamicCounters* counters,
                                           DynamicStrings* strings,
                                           const ExportedHistogram& copyMe)
  : dynamicCounters_(counters)
  , dynamicStrings_(strings)
  , defaultHist_(copyMe) { }

bool
ExportedHistogramMap::addHistogram(const string& name,
                                   const ExportedHistogram* copyMe) {
  bool created = false;
  HistMap::LockAndItem item = histMap_.getOrCreateUnlocked(
      name, (copyMe ? *copyMe : defaultHist_), &created);
  if (created) {
    if (copyMe) {
      SpinLockHolder h(*(item.first));
      item.second->clear();
    }
    HistogramExporter::exportBuckets(item.second, name, item.first.get(),
                                     dynamicStrings_);
  } else {
    LOG("ERROR",
        string("Attempted to export a histogram multiple times: ") + name);
  }
  return created;
}

bool
ExportedHistogramMap::exportPercentile(const string& name, int percentile) {
  HistMap::LockAndItem item = histMap_.getUnlocked(name);
  if (!item.second) {
    LOG("ERROR", string("Attempted to export non-existent histogram: ") + name);
    return false;
  }

  HistogramExporter::exportPercentile(item.second, name, percentile,
                                      item.first.get(), dynamicCounters_);
  return true;
}

ExportedHistogramMap::LockedHistogramPtr
ExportedHistogramMap::ensureExists(const string& name, bool crashIfMissing) {
  HistMap::LockAndItem item = histMap_.getUnlocked(name);
  if (!item.second) {
    if (crashIfMissing) {
      LOG("FATAL", string("Accessing non-existent histogram: ") + name);
      assert("Crashing..." && false);
    } else {
      return LockedHistogramPtr();
    }
  }
  return histMap_.createLockedValuePtr(&item);
}

} }  // !namespace facebook::stats
