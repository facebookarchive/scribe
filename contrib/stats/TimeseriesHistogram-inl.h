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

#ifndef __STATS_TIMESERIES_HISTOGRAM_INL_H
#define __STATS_TIMESERIES_HISTOGRAM_INL_H

#include <sstream>


namespace facebook { namespace stats {

template <typename T>
TimeseriesHistogram<T>::TimeseriesHistogram(ValueType bucketSize,
                                            ValueType min,
                                            ValueType max,
                                            const ContainerType& copyMe)
  : bucketSize_(bucketSize),
    min_(min),
    max_(max),
    defaultContainer_(copyMe) {
  // adding 2 to insert two extra 'below min' and 'above max' buckets
  int numBuckets = (max - min + bucketSize - 1) / bucketSize + 2;
  buckets_.assign(numBuckets, Bucket(ValueType(), defaultContainer_));

  assert(bucketSize_ > 0);
  assert(min_ < max_);
  // a very reasonable requirement, saves a lot of code
  assert(max_ - min_ >= bucketSize_);

  // TODO: using ValueType() is not ideal, but it does not affect correctness
  //       since buckets[0].first is not used for logic (e.g. comparison)
  buckets_[0].first = ValueType();

  // the range of each bucket is [bucket.first, next bucket.first)
  ValueType value = min_;
  for (int i = 1; i < buckets_.size() - 1; i++) {
    buckets_[i].first = value;
    value += bucketSize_;
  }
  buckets_.back().first = max_;
}

template <typename T>
void TimeseriesHistogram<T>::addValue(time_t now, const ValueType& value) {
  int bucketIdx = getBucketIdx(value);
  buckets_[bucketIdx].second.addValue(now, value);
}

template <typename T>
void TimeseriesHistogram<T>::addValue(time_t now,
                                      const ValueType& value,
                                      int64_t times) {
  int bucketIdx = getBucketIdx(value);
  buckets_[bucketIdx].second.addValue(now, value, times);
}

template <typename T>
T TimeseriesHistogram<T>::getPercentileEstimate(int pct, int level) const {
  std::vector<int64_t> counts(buckets_.size());
  for (size_t i = 0; i < buckets_.size(); i++) {
    counts[i] = buckets_[i].second.getCount(level);
  }

  std::pair<double, double> pctRange;
  int idx = getBucketForPct(counts, pct, &pctRange);
  ValueType avg = buckets_[idx].second.getAvg(level);
  return estimatePctValue(idx, pctRange, avg, pct);
}

template <typename T>
T TimeseriesHistogram<T>::getPercentileEstimate(
                             int pct, const HistoricalInterval& itv) const {
  std::vector<int64_t> counts(buckets_.size());
  for (size_t i = 0; i < buckets_.size(); i++) {
    counts[i] = buckets_[i].second.getCount(itv);
  }

  std::pair<double, double> pctRange;
  int idx = getBucketForPct(counts, pct, &pctRange);
  ValueType avg = buckets_[idx].second.getAvg(itv);
  return estimatePctValue(idx, pctRange, avg, pct);
}

template <typename T>
int TimeseriesHistogram<T>::getPercentileBucketIdx(int pct, int level) const {
  std::vector<int64_t> counts(buckets_.size());
  for (size_t i = 0; i < buckets_.size(); i++) {
    counts[i] = buckets_[i].second.getCount(level);
  }
  return getBucketForPct(counts, pct);
}

template <typename T>
int TimeseriesHistogram<T>::getPercentileBucketIdx(
                             int pct, const HistoricalInterval& itv) const {
  std::vector<int64_t> counts(buckets_.size());
  for (size_t i = 0; i < buckets_.size(); i++) {
    counts[i] = buckets_[i].second.getCount(itv);
  }

  return getBucketForPct(counts, pct);
}

template <typename T>
int64_t TimeseriesHistogram<T>::getCount(int level) const {
  int64_t total = 0;
  for (size_t b = 0; b < buckets_.size(); ++b) {
    total += buckets_[b].second.getCount(level);
  }
  return total;
}

template <typename T>
int64_t TimeseriesHistogram<T>::getCount(const HistoricalInterval& itv) const {
  int64_t total = 0;
  for (size_t b = 0; b < buckets_.size(); ++b) {
    total += buckets_[b].second.getCount(itv);
  }
  return total;
}

template <typename T>
T TimeseriesHistogram<T>::getSum(int level) const {
  ValueType total = ValueType();
  for (size_t b = 0; b < buckets_.size(); ++b) {
    total += buckets_[b].second.getSum(level);
  }
  return total;
}

template <typename T>
T TimeseriesHistogram<T>::getSum(const HistoricalInterval& itv) const {
  ValueType total = ValueType();
  for (size_t b = 0; b < buckets_.size(); ++b) {
    total += buckets_[b].second.getSum(itv);
  }
  return total;
}

template <typename T>
void TimeseriesHistogram<T>::clear() {
  for (size_t i = 0; i < buckets_.size(); i++) {
    buckets_[i].second.clear();
  }
}

template <typename T>
void TimeseriesHistogram<T>::update(time_t now) {
  for (size_t i = 0; i < buckets_.size(); i++) {
    buckets_[i].second.update(now);
  }
}

template <typename T>
int TimeseriesHistogram<T>::getBucketIdx(const ValueType& value) const {
  if (value < min_) {
    return 0;
  } else if (value >= max_) {
    return buckets_.size() - 1;
  } else {
    // the 1 is the below_min bucket
    return (value - min_) / bucketSize_ + 1;
  }
}

template <typename T>
int TimeseriesHistogram<T>::getBucketForPct(
               const std::vector<int64_t>& counts,
               int pct,
               std::pair<double, double>* bucketPctRange /* = NULL */) const {
  assert(pct >= 0);
  assert(pct <= 100);

  // compute total count in all buckets
  int64_t totalCount = 0;
  for (size_t i = 0; i < counts.size(); ++i) {
    totalCount += counts[i];
  }

  if (!totalCount) {
    // no items, say the lowest bucket
    if (bucketPctRange) {
      *bucketPctRange = std::make_pair(0.0, 0.0);
    }
    return 0;
  }

  // we loop through all the buckets, keeping track of each bucket's
  // percentile range: [0,10], [10,17], [17,45], etc.  When we find a range
  // that includes our desired percentile, we return that bucket index.
  std::pair<double, double> pctRange(0.0, 0.0);
  int64_t curCount = 0;
  for (int idx = 0; idx < counts.size(); ++idx) {
    if (!counts[idx]) {
      // skip empty buckets
      continue;
    }

    curCount += counts[idx];
    pctRange.first = pctRange.second;
    pctRange.second = double(curCount) * 100.0 / totalCount;
    if (pct <= pctRange.second || (idx + 1) == counts.size()) {
      // we've found our bucket; return bucket index (and the range, if desired)
      if (bucketPctRange) {
        *bucketPctRange = pctRange;
      }
      return idx;
    }
  }

  // should never get here
  assert(false);
  return -1;
}

template <typename T>
T TimeseriesHistogram<T>::estimatePctValue(
                                int bucketIdx,
                                const std::pair<double, double>& pctRange,
                                const ValueType& bucketAvg,
                                int wantedPct) const {
  if (pctRange.first == 0.0 && pctRange.second == 0.0) {
    // invalid range -- timeseries must be empty
    return T();
  }

  assert(wantedPct >= pctRange.first);
  assert(wantedPct <= pctRange.second);

  if (pctRange.first == pctRange.second) {
    // no real range available (this is almost always when range is [0.0, 0.0]
    // we return the bucketAvg, which will tend to be zero in this case
    return bucketAvg;
  }

  assert(buckets_.size() >= 2);
  T bucketLow;
  T bucketHigh;
  if (bucketIdx == 0) {
    // for the bottom bucket, assume the avg is equidistant between high, low
    bucketHigh = buckets_[bucketIdx + 1].first;
    bucketLow = bucketHigh - 2 * (bucketHigh - bucketAvg);
  } else if (bucketIdx == (buckets_.size() - 1)) {
    // for the top bucket, assume the avg is equidistant between high, low
    bucketLow = buckets_[bucketIdx].first;
    bucketHigh = bucketLow + 2 * (bucketAvg - bucketLow);
  } else {
    // for a middle bucket, just take the bucket range cleanly
    bucketLow = buckets_[bucketIdx].first;
    bucketHigh = buckets_[bucketIdx + 1].first;
  }

  double medianPct = (pctRange.first + pctRange.second) / 2.0;
  double pct = wantedPct;
  if (wantedPct <= medianPct) {
    // linearly interpolate between the low end and the middle of the bucket
    return estimatePctValue(std::make_pair(pctRange.first, medianPct),
                            std::make_pair(bucketLow, bucketAvg),
                            pct);
  } else {
    // linearly interpolate between the middle and the high end of the bucket
    return estimatePctValue(std::make_pair(medianPct, pctRange.second),
                            std::make_pair(bucketAvg, bucketHigh),
                            pct);
  }
}

template <typename T>
T TimeseriesHistogram<T>::estimatePctValue(
                           const std::pair<double, double>& pctRange,
                           const std::pair<ValueType, ValueType>& valRange,
                           double wantedPct) const {
  assert(pctRange.first != pctRange.second);
  double ratio = (wantedPct - pctRange.first) /
                 (pctRange.second - pctRange.first);
  return static_cast<T>(
    valRange.first + (valRange.second - valRange.first) * ratio);
}

template <typename T>
std::string TimeseriesHistogram<T>::debugString() const {
  std::ostringstream o;

  o << "num buckets: " << buckets_.size()
    << ", bucketSize: " << bucketSize_
    << ", min: " << min_
    << ", max: " << max_ << "\n";

  for (size_t i = 0; i < buckets_.size(); i++) {
    o << "  " << buckets_[i].first
      << ": " << buckets_[i].second.getCountString() << "\n";
  }

  return o;
}

template <typename T>
std::string TimeseriesHistogram<T>::getString(int level) const {
  std::ostringstream o;

  for (size_t i = 0; i < buckets_.size(); i++) {
    if (i > 0) {
      o << ",";
    }
    const ContainerType& cont = buckets_[i].second;
    o << buckets_[i].first
      << ":" << cont.getCount(level)
      << ":" << cont.getAvg(level);
  }

  return o.str();
}

template <typename T>
std::string TimeseriesHistogram<T>::getString(const HistoricalInterval& itv)
    const {
  std::ostringstream o;

  for (size_t i = 0; i < buckets_.size(); i++) {
    if (i > 0) {
      o << ",";
    }
    const ContainerType& cont = buckets_[i].second;
    o << buckets_[i].first
      << ":" << cont.getCount(itv)
      << ":" << cont.getAvg(itv);
  }

  return o.str();
}


template <typename T>
std::ostream& operator<<(std::ostream& o,
                         const TimeseriesHistogram<T>& h) {
  return o << h.debugString();
}

} }  // !namespace facebook::stats

#endif
