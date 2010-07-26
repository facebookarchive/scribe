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

#ifndef __STATS_EXPORTED_HISTOGRAM_H
#define __STATS_EXPORTED_HISTOGRAM_H

#include <string>
#include <tr1/functional>
#include <boost/shared_ptr.hpp>

#include "stats/TimeseriesHistogram.h"
#include "stats/DynamicCounters.h"
#include "datastruct/SynchMap-inl.h"

namespace facebook { namespace stats {

namespace tr1 = std::tr1;
using datastruct::SynchMap;

typedef stats::TimeseriesHistogram<CounterType> ExportedHistogram;

/**
 * class ExportedHistogramMap
 *
 * This class implements a map(string => TimeseriesHistogram), with the
 * ability to export data from these histograms to our FB303 counters
 * and exported values facilities so that this data can be collected in
 * an automated fashion by dashboards or ODS.
 *
 * You can create new histograms via 'exportHistogram()', which also exports
 * the full histogram to the fb303::getExportedValues() call via the
 * DynamicStrings connector object.
 *
 * You can also export a simple counter via the fb303::getCounters() call
 * for a particular percentile in your histogram via exportPercentile().
 *
 * After creation, you can use the addValue*() functions to insert values.
 *
 * The values can get queried by using 'getHistogram()' and then querying
 * functions on the histogram directly, or via the various counters exported.
 *
 * @author mrabkin
 */

class ExportedHistogramMap : boost::noncopyable {
 public:
  /**
   * Creates an ExportedHistogramMap and hooks it up to the given
   * DynamicCounters object for getCounters(), and the given DynamicStrings
   * object for getExportedValues().  The copyMe object provided will be used
   * as a blueprint for new histograms that are created; this is where you set
   * up your bucket ranges and time levels appropriately for your needs.  There
   * is no default set of bucket ranges, so a 'copyMe' object must be provided.
   */
  ExportedHistogramMap(
      DynamicCounters* counters,
      DynamicStrings* strings,
      const ExportedHistogram& copyMe);

  typedef boost::shared_ptr<ExportedHistogram> LockedHistogramPtr;

  /** Returns true if the given histogram exists in the map */
  bool contains(const std::string& name) const {
    return histMap_.contains(name);
  }

  /**
   * Returns a magic shared_ptr<> to the given histogram that holds a lock
   * while it exists.  Please destroy this pointer as soon as possible to
   * release the lock and allow updates to the histogram from other threads.
   *
   * If the histogram doesn't exist, returns a NULL ptr that holds no locks.
   */
  LockedHistogramPtr getHistogram(const std::string& name) {
    return ensureExists(name, false);
  }

  /**
   * Creates a new histogram with the given name and returns true on success.
   * If the histogram already existed, false is returned and nothing happens.
   *
   * If 'copyMe' is provided, the new histogram is copy-constructed from
   * 'copyMe' and then cleared.  Otherwise, the histogram is constructed in the
   * same way from the 'copyMe' argument provided to the ExportedHistogramMap
   * constructor.
   *
   * Then, all of the histogram's levels are all exported to DynamicStrings
   * with keys of the form:   <histogram_name>.hist.<level_duration>
   */
  bool addHistogram(const std::string& name,
                    const ExportedHistogram* copyMe = NULL);

  /**
   * Given a histogram, exports a counter representing our best estimate of the
   * given percentile's value in the histogram at all time levels. If the given
   * histogram doesn't exist, returns false.
   */
  bool exportPercentile(const std::string& name, int percentile);

  /** Adds a value into histogram 'name' at time 'now' */
  void addValue(const std::string& name, time_t now, CounterType value) {
    ensureExists(name)->addValue(now, value);
  }
  /** Adds value into histogram 'name' multiple times (for efficiency). */
  void addValue(const std::string& name, time_t now, CounterType value,
                int64_t times) {
    ensureExists(name)->addValue(now, value, times);
  }
  /** Clears the histogram with the given name. */
  void clearHistogram(const std::string& name) {
    ensureExists(name)->clear();
  }
 protected:
  boost::shared_ptr<ExportedHistogram> ensureExists(const std::string& name,
                                             bool crashIfMissing = true);

  typedef SynchMap<std::string, ExportedHistogram> HistMap;
  HistMap histMap_;

  DynamicCounters* dynamicCounters_;
  DynamicStrings* dynamicStrings_;
  ExportedHistogram defaultHist_;
};


} }  // !namespace facebook::stats

#endif
