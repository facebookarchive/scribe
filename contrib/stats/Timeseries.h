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

#ifndef __STATS_TIMESERIES_H
#define __STATS_TIMESERIES_H

#include <algorithm>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

namespace facebook { namespace stats {

/** struct HistoricalInterval
 *
 * This struct represents a historical interval relative to now.
 * HistoricalInterval(10, 5) means from 10 seconds ago to 5 seconds ago.
 */
struct HistoricalInterval {
  HistoricalInterval(int from, int to)
    : fromSecondsAgo(from), toSecondsAgo(to) {
    assert(fromSecondsAgo >= toSecondsAgo);
    assert(toSecondsAgo >= 0);
  }

  int length() const { return fromSecondsAgo - toSecondsAgo; }

  const int fromSecondsAgo;
  const int toSecondsAgo;
};

/** class BucketedTimeSeries
 *
 * This class represents a bucketed time series which keeps track of values
 * added in the recent past, and merges these values together into a fixed
 * number of buckets to keep a lid on memory use if the number of values
 * added is very large.
 *
 * For example, a BucketedTimeSeries() with duration == 60s and 10 buckets
 * will keep track of 10 6-second buckets, and discard all data added more
 * than 1 minute ago.  As time ticks by, a 6-second bucket at a time will
 * be discarded and new data will go into the newly opened bucket.  Internally,
 * it uses a circular array of buckets that it reuses as time advances.
 *
 * The class assumes that time advances forward --  you can't retroactively add
 * values for events in the past -- the 'now' argument is provided for better
 * efficiency and ease of unittesting.
 *
 *
 * Currently, only the total sum of the values in the TimeSeries is accessible.
 *
 * The class is not thread-safe -- use your own synchronization!
 *
 * @author Mark Rabkin (mrabkin@facebook.com)
 */

template <class T>
class BucketedTimeSeries {
 public:
  BucketedTimeSeries(int num_buckets, time_t duration)
    : empty_(true), first_time_(0), latest_time_(0), duration_(duration),
      sum_(T()), count_(0) {
    buckets_.resize(num_buckets, Bucket(T(), 0));
  }

  int numBuckets() const { return buckets_.size(); }
  time_t duration() const { return duration_; }
  bool alltime() const { return (duration_ == 0); }
  bool empty() const { return empty_; }

  time_t elapsed() const {
    if (empty()) {
      return 0;
    }

    time_t time_passed = latest_time_ - first_time_ + 1;
    return (alltime() ? time_passed : std::min(time_passed, duration_));
  }

  void setDuration(int d) {
    duration_ = d;
    clear();
  }

  const T& sum() const { return sum_; }
  int64_t count() const { return count_; }
  T avg() const { return (count_ ? (sum_ / count_) : T()); }
  T rate() const {
    time_t e = elapsed();
    return (e ? (sum_ / e) : T());
  }

  T sum(const HistoricalInterval& interval) const;
  int64_t count(const HistoricalInterval& interval) const;
  T avg(const HistoricalInterval& interval) const;
  T rate(const HistoricalInterval& interval) const;

  // TODO(mrabkin): add other accessors as needed

  // clears all the data stored in this bucketed time series
  void clear();
  std::string toString() const;

  // adds the value 'val' at time 'now'
  void addValue(time_t now, const T& val);
  // adds the value 'val' the given number of 'times' at time 'now'
  void addValue(time_t now, const T& val, int64_t times);
  // adds the value 'sum' as the sum of 'nsamples' samples
  void addValueAggregated(time_t now, const T& sum, int64_t nsamples);

  // "updates" the container to time 'now', doing all the necessary
  // cleanup of old data
  void update(time_t now);

  // The time of the last update
  time_t getLatestTime() const;

 private:
  int getBucket(time_t time) const;
  void getBucketInfo(const HistoricalInterval& interval,
                     int* startBucket,
                     int* numSpannedBuckets) const;

 private:
  // pair<sum, count>
  typedef std::pair<T, int> Bucket;

  bool empty_;           // have we had any update() calls since clear()?
  time_t first_time_;    // time of first update() since clear()/constructor
  time_t latest_time_;   // time of last update()
  time_t duration_;      // total duration ("window length") of the time series

  std::vector<Bucket> buckets_;    // actual buckets of values
  T sum_;                    // total sum of everything in time series
  int64_t count_;            // total count of everything in time series
};

/** class MultiLevelTimeSeries
 *
 * This class represents a timeseries which keeps several levels of data
 * granularity (similar in principle to the loads reported by the UNIX
 * 'uptime' command).  It uses several instances (one per level) of
 * BucketedTimeSeries as the underlying storage.
 *
 * This can easily be used to track sums (and thus rates or averages) over
 * several predetermined time periods, as well as all-time sums.  For example,
 * you would use to it to track query rate or response speed over the last
 * 5, 15, 30, and 60 minutes.
 *
 * The MultiLevelTimeSeries takes a list of level durations as an input; the
 * durations must be strictly increasing.  Furthermore a special level can be
 * provided with a duration of '0' -- this will be an "all-time" level.  If
 * an all-time level is provided, it MUST be the last level present.
 *
 * The class assumes that time advances forward --  you can't retroactively add
 * values for events in the past -- the 'now' argument is provided for better
 * efficiency and ease of unittesting.
 *
 * The class is not thread-safe -- use your own synchronization!
 *
 * @author Mark Rabkin (mrabkin@facebook.com)
 */

template <class T>
class MultiLevelTimeSeries {
 public:
  MultiLevelTimeSeries(int num_levels, int num_buckets,
                       const int* level_durations);

  // basic acessors
  int numLevels() const { return levels_.size(); }
  int numBuckets() const { return num_buckets_; }

  const BucketedTimeSeries<T>& getLevel(int level) const {
    assert(level >= 0);
    assert(level < levels_.size());
    return levels_[level];
  }

  const BucketedTimeSeries<T>& getLevel(const HistoricalInterval& interval)
  const {
    for (int i = 0; i < levels_.size(); i++) {
      int levelDuration = getLevel(i).duration();
      if (levelDuration != 0 && levelDuration >= interval.fromSecondsAgo ||
          levelDuration == 0) {
        return levels_[i];
      }
    }
    // we (should) always have alltime, this is never reached
    time_t now = time(NULL);
    char now_pretty[26];
    ctime_r(&now, now_pretty);
    now_pretty[24] = '\0';
    std::cout << "[FATAL] [" << now_pretty << "] "
              << "No level of timeseries covers internval"
              << " from " << interval.fromSecondsAgo
              << " to " << interval.toSecondsAgo
              << std::endl;
    return levels_.back();
  }

  T getSum(int level) const { return getLevel(level).sum(); }
  T getAvg(int level) const { return getLevel(level).avg(); }
  T getRate(int level) const { return getLevel(level).rate(); }
  int64_t getCount(int level) const { return getLevel(level).count(); }

  T getSum(const HistoricalInterval& interval) const {
    return getLevel(interval).sum(interval);
  }
  T getAvg(const HistoricalInterval& interval) const {
    return getLevel(interval).avg(interval);
  }
  T getRate(const HistoricalInterval& interval) const {
    return getLevel(interval).rate(interval);
  }
  int64_t getCount(const HistoricalInterval& interval) const {
    return getLevel(interval).count(interval);
  }

  // clears all the data in the time series
  void clear();

  // updates all the levels
  void update(time_t now);

  // inserts the value 'val' at time 'now' into the timeseries (with an
  // option to insert the same value multiple times)
  void addValue(time_t now, const T& val);
  void addValue(time_t now, const T& val, int64_t times);
  // adds the value 'sum' as the sum of 'nsamples' samples
  void addValueAggregated(time_t now, const T& sum, int64_t nsamples);

  std::string getAvgString() const;
  std::string getSumString() const;
  std::string getCountString() const;
  std::string getRateString() const;

 private:
  typedef BucketedTimeSeries<T> Level;
  int num_buckets_;
  std::vector<Level> levels_;
};

const int kMinuteHourDurations[] = { 60, 3600, 0 };

template <class T>
class MinuteHourTimeSeries : public MultiLevelTimeSeries<T> {
 public:
  enum Levels {
    MINUTE,
    HOUR,
    ALLTIME,
    NUM_LEVELS,
  };

  MinuteHourTimeSeries()
    : MultiLevelTimeSeries<T>(NUM_LEVELS, 60, kMinuteHourDurations) { }
};

const int kMinuteTenMinuteHourDurations[] = { 60, 600, 3600, 0 };

template <class T>
class MinuteTenMinuteHourTimeSeries : public MultiLevelTimeSeries<T> {
 public:
  enum Levels {
    MINUTE,
    TEN_MINUTE,
    HOUR,
    ALLTIME,
    NUM_LEVELS,
  };

  MinuteTenMinuteHourTimeSeries()
    : MultiLevelTimeSeries<T>(NUM_LEVELS, 60, kMinuteTenMinuteHourDurations) { }
};

const int kMinuteHourDayDurations[] = { 60, 3600, 86400, 0 };

template <class T>
class MinuteHourDayTimeSeries : public MultiLevelTimeSeries<T> {
 public:
  enum Levels {
    MINUTE,
    HOUR,
    DAY,
    ALLTIME,
    NUM_LEVELS,
  };

  MinuteHourDayTimeSeries()
    : MultiLevelTimeSeries<T>(NUM_LEVELS, 60, kMinuteHourDayDurations) { }
};

const int kMinuteOnlyDurations[] = { 60 };

template <class T>
class MinuteOnlyTimeSeries : public MultiLevelTimeSeries<T> {
 public:
  enum Levels {
    MINUTE,
    NUM_LEVELS,
  };

  MinuteOnlyTimeSeries()
    : MultiLevelTimeSeries<T>(NUM_LEVELS, 60, kMinuteOnlyDurations) { }
};

} } // ! namespace facebook::stats

#include "stats/Timeseries-inl.h"

#endif  // !__STATS_TIMESERIES_H
