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

#include <time.h>
#include <stdio.h>     // snprintf

#include "stats/ExportedTimeseries.h"

using namespace facebook;
using namespace boost;
using namespace std;

namespace facebook { namespace stats {

const char* const kTypeString[] = {
  "sum",
  "count",
  "avg",
  "rate"
};

static CounterType getStatValue(const shared_ptr<ExportedStat>& stat,
                                ExportType type,
                                int level,
                                SpinLock* lock) {
  // ensure the lock is held (if non-NULL)
  scoped_ptr<SpinLockHolder> holder;
  if (lock) {
    holder.reset(new SpinLockHolder(*lock));
  }

  // update the stat with the current time -- if no new items are being
  // inserted, the stats won't decay properly without this update()
  stat->update(time(NULL));

  // retrieve the correct type of info from the stat
  switch (type) {
    case SUM:
      return stat->getSum(level);
    case AVG:
      return stat->getAvg(level);
    case RATE:
      return stat->getRate(level);
    case COUNT:
      // getCount() returns int64_t, so we cast it to CounterType to be safe
      return static_cast<CounterType>(stat->getCount(level));
    default: {
      time_t now = time(NULL);
      char now_pretty[26];
      ctime_r(&now, now_pretty);
      now_pretty[24] = '\0';
      cout << "[FATAL] [" << now_pretty << "] "
           << "invalid stat export type: " << type << endl;
      assert("Crashing..." && false);
    }
  };
}

/* static */
void TimeseriesExporter::exportStat(const shared_ptr<ExportedStat>& stat,
                                    ExportType type,
                                    const string& stat_name,
                                    DynamicCounters* counters,
                                    SpinLock* lock) {
  assert(type >= 0);
  assert(type < NUM_TYPES);

  const int kNameSize = stat_name.size() + 50; // some extra space
  char counter_name[kNameSize + 1];
  counter_name[kNameSize] = '\0';

  for (int lev = 0; lev < stat->numLevels(); ++lev) {
    // NOTE:  We access the stat object here without locking.  This depends
    // on the fact that getLevel(), and Level::alltime() and Level::duration()
    // are all non-volatile calls meaning they only read things that are
    // constant once the stat is constructed (number of levels can never
    // change, nor their durations).
    //   - mrabkin

    if (stat->getLevel(lev).alltime()) {
      // typical name will be something like:
      //    ad_request.rate
      //    ad_request_elapsed_time.avg
      snprintf(counter_name, kNameSize, "%s.%s",
               stat_name.c_str(), kTypeString[type]);
    } else {
      // typical name will be something like:
      //    ad_request.rate.600
      //    ad_request_elapsed_time.avg.3600
      snprintf(counter_name, kNameSize, "%s.%s.%ld",
        stat_name.c_str(), kTypeString[type], stat->getLevel(lev).duration());
    }

    // bind the stat to the getStatValue() function, which takes care of
    // locking the lock and calling update() on the stat
    DynamicCounters::Callback cob = tr1::bind(getStatValue,
                                              stat, type, lev, lock);

    // register the actual counter callback with the DynamicCounters obj
    counters->registerCallback(counter_name, cob);
  }
}

void
ExportedStatMap::exportStat(const string& name,
                            ExportType type,
                            const ExportedStat* copyMe) {
  StatMap::LockAndItem item =
    statMap_.getOrCreateUnlocked(name, (copyMe ? *copyMe : defaultStat_));
  assert(item.first);
  assert(item.second);
  TimeseriesExporter::exportStat(item.second, type, name, dynamicCounters_,
                                 item.first.get());
}

shared_ptr<ExportedStat>
ExportedStatMap::getOrExport(const string& name) {
  // Create or find the stat
  bool created;
  StatMap::LockAndItem item = statMap_.getOrCreateUnlocked(
      name, defaultStat_, &created);
  assert(item.first);
  assert(item.second);

  if (created) {
    // if newly created, add the default export types
    for (vector<ExportType>::const_iterator type = defaultTypes_.begin();
         type != defaultTypes_.end();
         ++ type) {
      TimeseriesExporter::exportStat(item.second, *type, name,
                                     dynamicCounters_, item.first.get());
    }
  }

  return statMap_.createLockedValuePtr(&item);
}




} }  // !namespace facebook::stats
