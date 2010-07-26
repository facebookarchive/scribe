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

#ifndef __STATS_EXPORTED_TIMESERIES_H
#define __STATS_EXPORTED_TIMESERIES_H

#include <vector>
#include <tr1/functional>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>

#include "stats/Timeseries.h"
#include "stats/DynamicCounters.h"
#include "datastruct/SynchMap-inl.h"

namespace facebook { namespace stats {

namespace tr1 = std::tr1;
using datastruct::SynchMap;

enum ExportType {
  SUM,
  COUNT,
  AVG,
  RATE,
  NUM_TYPES,
};

typedef stats::MultiLevelTimeSeries<CounterType> ExportedStat;

class TimeseriesExporter {
 public:
  static void exportStat(const boost::shared_ptr<ExportedStat>& stat,
                         ExportType type,
                         const std::string& stat_name,
                         DynamicCounters* counters,
                         SpinLock* lock);
};

class ExportedStatMap : boost::noncopyable {
 public:
  explicit ExportedStatMap(DynamicCounters* counters,
                           ExportType defaultType = AVG,
                           const ExportedStat& defaultStat
                           = MinuteTenMinuteHourTimeSeries<CounterType>())
    : dynamicCounters_(counters),
      defaultTypes_(1, defaultType),
      defaultStat_(defaultStat) { }

 ExportedStatMap(DynamicCounters* counters,
                 const std::vector<ExportType>& defaultTypes,
                 const ExportedStat& defaultStat
                 = MinuteTenMinuteHourTimeSeries<CounterType>())
    : dynamicCounters_(counters),
      defaultTypes_(defaultTypes),
      defaultStat_(defaultStat) { }

  void setDefaultStat(const ExportedStat& defaultStat) {
    defaultStat_ = defaultStat;
  }

  DynamicCounters* dynamicCounters() const { return dynamicCounters_; }

  // creating and retrieving new stats
  boost::shared_ptr<ExportedStat> getStatPtr(const std::string& name) {
    return getOrExport(name);
  }

  void exportStat(const std::string& name) {
    std::vector<ExportType>::iterator type;
    for (type = defaultTypes_.begin(); type != defaultTypes_.end(); ++ type) {
      exportStat(name, *type, &defaultStat_);
    }
  }

  // adding values into the stats stored in the map
  void addValue(const std::string& name, time_t now, CounterType value) {
    getOrExport(name)->addValue(now, value);
  }
  void addValue(const std::string& name, time_t now,
                CounterType value, int64_t times) {
    getOrExport(name)->addValue(now, value, times);
  }
  void addValueAggregated(const std::string& name, time_t now,
                          CounterType sum, int64_t nsamples) {
    getOrExport(name)->addValueAggregated(now, sum, nsamples);
  }
  void clearValue(const std::string& name) {
    getOrExport(name)->clear();
  }

  void exportStat(const std::string& name, ExportType type,
                  const ExportedStat* copyMe = NULL);

 protected:
  boost::shared_ptr<ExportedStat> getOrExport(const std::string& name);

  typedef SynchMap<std::string, ExportedStat> StatMap;
  StatMap statMap_;
  DynamicCounters* dynamicCounters_;

  std::vector<ExportType> defaultTypes_;
  // note: We slice defaultStat by copying it to a ExportedStat
  // (non-pointer, non-reference), but that's according to plan: the
  // derived classes only set data members in the base class, nothing
  // more (they have no data members of their own).
  ExportedStat defaultStat_;
};


} }  // !namespace facebook::stats

#endif
