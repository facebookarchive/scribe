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
 * @author Ning Li (nli@facebook.com)
 *
 */

#ifndef __STATS_TIMESERIES_HISTOGRAM_H
#define __STATS_TIMESERIES_HISTOGRAM_H

#include <limits>
#include <utility>
#include <vector>
#include <boost/static_assert.hpp>
#include "stats/Timeseries.h"


namespace facebook { namespace stats {

/**
 * TimeseriesHistogram is a class which allows you to track data distributions
 * as they change over time.
 *
 * Specifically, it is a bucketed histogram with different value ranges
 * assigned to each bucket.  Within each bucket is a MultiLevelTimeSeries as
 * from '../../stats/Timeseries.h'. This means that each bucket contains a
 * different set of data for different historical time periods, and one can
 * query data distributions over different trailing time windows.
 *
 * For example, this can answer questions: "What is the data distribution over
 * the last minute? Over the last 10 minutes?  Since I last cleared this
 * histogram?"
 *
 * The class can also estimate percentiles and answer questions like:
 *
 *   "What was the 99th percentile data value over the last 10 minutes?"
 *
 * However, note that depending on the size of your buckets and the smoothness
 * of your data distribution, the estimate may be way off from the actual
 * value.  In particular, if the given percentile falls outside of the bucket
 * range (i.e. your buckets range in 0 - 100,000 but the 99th percentile is around
 * 115,000) this estimate may be very wrong.
 *
 * The memory usage for a typical histogram is roughly 3k * (# of buckets).  All
 * insertion operations are amortized O(1), and all queries are O(# of buckets).
 */

template <class T>
class TimeseriesHistogram {
 private:
   // NOTE: T must be equivalent to _signed_ numeric type for our math
   BOOST_STATIC_ASSERT(std::numeric_limits<T>::is_signed);

 public:
  // values to be inserted into container
  typedef T ValueType;
  // the container type we use internally for each bucket
  typedef MultiLevelTimeSeries<T> ContainerType;

  /**
   * Creates a TimeSeries histogram and initializes the bucketing and levels.
   *
   * The buckets are created by chopping the range [min, max) into pieces
   * of size bucketSize, with the last bucket being potentially shorter.  Two
   * additional buckets are always created -- the "under" bucket for the range
   * (-inf, min) and the "over" bucket for the range [max, +inf).
   *
   * By default, the histogram will use levels of 60/600/3600/alltime (seconds),
   * but his can be overriden by passing in an already-constructed multilevel
   * timeseries with the desired level durations.
   *
   * @param bucketSize the width of each bucket
   * @param min the smallest value for the bucket range.
   * @param max the largest value for the bucket range
   * @param defaultContainer a pre-initialized timeseries with the desired
   *                         number of levels and their durations.
   */
  TimeseriesHistogram(ValueType bucketSize, ValueType min, ValueType max,
                      const ContainerType& defaultContainer =
                         MinuteTenMinuteHourTimeSeries<T>());

  /** Returns the bucket size of each bucket in the histogram. */
  ValueType getBucketSize() const { return bucketSize_; }

  /** Returns the min value at which bucketing begins. */
  ValueType getMin() const { return min_; }

  /** Returns the max value at which bucketing ends. */
  ValueType getMax() const { return max_; }

  /** Returns the number of levels of the Timeseries object in each bucket */
  int getNumLevels() const { return buckets_.front().second.numLevels(); }

  /** Returns the number of buckets */
  int getNumBuckets() const { return buckets_.size(); }

  /** Returns the bucket index into which the given value would fall. */
  int getBucketIdx(const ValueType& value) const;

  /**
   * Returns the threshold of the bucket for the given index in range
   * [0..numBuckets).  The bucket will have range [thresh, thresh + bucketSize)
   * or [thresh, max), whichever is shorter.
   */
  ValueType getBucketMin(int bucketIdx) const {
    return buckets_[bucketIdx].first;
  }

  /** Returns the actual timeseries in the given bucket (for reading only!) */
  const ContainerType& getBucket(int bucketIdx) const {
    return buckets_[bucketIdx].second;
  }

  /** Total count of values at the given timeseries level (all buckets). */
  int64_t getCount(int level) const;

  /** Total count of values added during the given interval (all buckets). */
  int64_t getCount(const HistoricalInterval& interval) const;

  /** Total sum of values at the given timeseries level (all buckets). */
  ValueType getSum(int level) const;

  /** Total sum of values added during the given interval (all buckets). */
  ValueType getSum(const HistoricalInterval& interval) const;

  /**
   * Updates every underlying timeseries object with the given timestamp. You
   * must call this directly before querying to ensure that the data in all
   * buckets is decayed properly.
   */
  void update(time_t now);

  /** clears all the data from the histogram. */
  void clear();

  /** Adds a value into the histogram with timestamp 'now' */
  void addValue(time_t now, const ValueType& value);
  /** Adds a value the given number of times with timestamp 'now' */
  void addValue(time_t now, const ValueType& value, int64_t times);

  /**
   * Returns an estimate of the value at the given percentile in the histogram
   * in the given timeseries level.  The percentile is estimated as follows:
   *
   * - We retrieve a count of the values in each bucket (at the given level)
   * - We determine via the counts which bucket the given percentile falls in.
   * - We assume the average value in the bucket is also its median
   * - We then linearly interpolate within the bucket, by assuming that the
   *   distribution is uniform in the two value ranges [left, median) and
   *   [median, right) where [left, right) is the bucket value range.
   *
   * Caveats:
   * - If the histogram is empty, this always returns ValueType(), usually 0.
   * - For the 'under' and 'over' special buckets, their range is unbounded
   *   on one side.  In order for the interpolation to work, we assume that
   *   the average value in the bucket is equidistant from the two edges of
   *   the bucket.  In other words, we assume that the distance between the
   *   average and the known bound is equal to the distance between the average
   *   and the unknown bound.
   */
  ValueType getPercentileEstimate(int pct, int level) const;
  /**
   * Returns an estimate of the value at the given percentile in the histogram
   * in the given historical interval.  Please see the documentation for
   * getPercentileEstimate(int pct, int level) for the explanation of the
   * estimation algorithm.
   */
  ValueType getPercentileEstimate(int pct, const HistoricalInterval& itv) const;

  /**
   * Returns the bucket index that the given percentile falls into (in the
   * given timeseries level).  This index can then be used to retrieve either
   * the bucket threshold, or other data from inside the bucket.
   */
  int getPercentileBucketIdx(int pct, int level) const;
  /**
   * Returns the bucket index that the given percentile falls into (in the
   * given historical interval).  This index can then be used to retrieve either
   * the bucket threshold, or other data from inside the bucket.
   */
  int getPercentileBucketIdx(int pct, const HistoricalInterval& itv) const;

  /** Gets the bucket threshold for the bucket containing the given pct. */
  int getPercentileBucketMin(int pct, int level) const {
    return getBucketMin(getPercentileBucketIdx(pct, level));
  }
  /** Gets the bucket threshold for the bucket containing the given pct. */
  int getPercentileBucketMin(int pct, const HistoricalInterval& itv) const {
    return getBucketMin(getPercentileBucketIdx(pct, itv));
  }

  /** Prints out the whole histogram timeseries in human-readable form */
  std::string debugString() const;

  /**
   * Prints out serialized data from all buckets at the given level.
   * Format is: BUCKET [',' BUCKET ...]
   * Where: BUCKET == bucketMin ':' count ':' avg
   */
  std::string getString(int level) const;

  /**
   * Prints out serialized data for all buckets in the historical interval.
   * For format, please see getString(int level).
   */
  std::string getString(const HistoricalInterval& itv) const;

 private:
  int getBucketForPct(const std::vector<int64_t>& counts, int pct,
                      std::pair<double, double>* bucketPctRange = NULL) const;

  ValueType estimatePctValue(int bucketIdx,
                             const std::pair<double, double>& pctRange,
                             const ValueType& bucketAvg,
                             int wantedPct) const;

  ValueType estimatePctValue(const std::pair<double, double>& pctRange,
                             const std::pair<ValueType, ValueType>& valRange,
                             double wantedPct) const;
 private:
  typedef std::pair<ValueType, ContainerType> Bucket;

  const ValueType bucketSize_;
  const ValueType min_;
  const ValueType max_;
  std::vector<Bucket> buckets_;
  const ContainerType defaultContainer_;
};

} }  // !namespace facebook::stats

#include "stats/TimeseriesHistogram-inl.h"

#endif
