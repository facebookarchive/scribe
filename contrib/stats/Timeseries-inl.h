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

#ifndef __STATS_TIMESERIES_INL_H
#define __STATS_TIMESERIES_INL_H

#include <sstream>

namespace facebook { namespace stats {

template <class T>
T BucketedTimeSeries<T>::sum(const HistoricalInterval& interval) const {
  int startBucket = 0;
  int numSpannedBuckets = 0;

  // compute the index of the start bucket and the number of buckets
  // for the historical interval
  getBucketInfo(interval, &startBucket, &numSpannedBuckets);

  int numBuckets = buckets_.size();
  if (numSpannedBuckets == numBuckets) {
    // span all buckets
    return sum_;
  } else {
    T result = T();

    // This is an approximation: exclude the value from the start bucket,
    // include the value from the end bucket and include values in between.
    // The reason for excluding start bucket and including end bucket is that
    // when the start time wraps around maps to the same bucket as that of
    // the end time, the bucket actually contains values for the end time,
    // not the start time.
    int endBucket = startBucket + numSpannedBuckets;
    for (int i = startBucket + 1; i <= endBucket; i++) {
      // take mod in case of wrap-around
      result += buckets_[i % numBuckets].first;
    }
    return result;
  }
}

template <class T>
int64_t BucketedTimeSeries<T>::count(const HistoricalInterval& interval) const {
  int startBucket = 0;
  int numSpannedBuckets = 0;

  getBucketInfo(interval, &startBucket, &numSpannedBuckets);

  int numBuckets = buckets_.size();
  if (numSpannedBuckets == numBuckets) {
    // span all buckets
    return count_;
  } else {
    int64_t result = 0;

    int endBucket = startBucket + numSpannedBuckets;
    for (int i = startBucket + 1; i <= endBucket; i++) {
      // take mod in case of wrap-around
      result += buckets_[i % numBuckets].second;
    }
    return result;
  }
}

template <class T>
T BucketedTimeSeries<T>::avg(const HistoricalInterval& interval) const {
  int startBucket = 0;
  int numSpannedBuckets = 0;

  getBucketInfo(interval, &startBucket, &numSpannedBuckets);

  int numBuckets = buckets_.size();
  if (numSpannedBuckets == numBuckets) {
    // span all buckets
    return avg();
  } else {
    T sum = T();
    int64_t count = 0;

    int endBucket = startBucket + numSpannedBuckets;
    for (int i = startBucket + 1; i <= endBucket; i++) {
      // take mod in case of wrap-around
      sum += buckets_[i % numBuckets].first;
      count += buckets_[i % numBuckets].second;
    }
    return (count ? (sum / count) : T());
  }
}

template <class T>
T BucketedTimeSeries<T>::rate(const HistoricalInterval& interval) const {
  if (alltime()) {
    return T();
  }

  // we do not adjust the interval when latest_time_ < interval.fromSecondsAgo
  // since we only encounter this case in testing
  return (interval.length() ? sum(interval) / interval.length() : T());
}

template <class T>
std::string BucketedTimeSeries<T>::toString() const {
  // Note: Having this toString() assumes that type T has a function for <<
  //  defined.
  std::ostringstream ret;
  ret << "first_time:" << first_time_ << " latest_time:" << latest_time_
      << " duration:" << duration_ << " sum:" << sum_ << " count:" << count_
      << std::endl;
  int start_bucket = getBucket(latest_time_);
  for (int i=1; i<=buckets_.size(); i++) {
    const int index = (start_bucket + i) % buckets_.size();
    const Bucket& B = buckets_[index];
    ret << index << ": " << B.first << " -> " << B.second << std::endl;
  }
  return ret.str();
}


template <class T>
void BucketedTimeSeries<T>::getBucketInfo(const HistoricalInterval& interval,
                                          int* startBucketPtr,
                                          int* numSpannedBucketsPtr) const {
  assert(startBucketPtr != NULL);
  assert(numSpannedBucketsPtr != NULL);

  // if alltime, set the number of buckets to 0 for the interval
  // this way, all the accessors will return 0
  if (alltime()) {
    *numSpannedBucketsPtr = 0;
    return;
  }

  assert(interval.fromSecondsAgo <= duration_);

  // assume now is latest_time_, compute the absolute start time and end time
  // of the historical interval
  time_t startTime = latest_time_ - interval.fromSecondsAgo;
  if (startTime < 0) startTime = 0;
  time_t endTime = latest_time_ - interval.toSecondsAgo;
  if (endTime < 0) endTime = 0;

  // compute the start bucket and the end bucket
  int startBucket = getBucket(startTime);
  int endBucket = getBucket(endTime);

  // compute the number of buckets between the start bucket (inclusive)
  // and the end bucket (exclusive)
  // (endBucket - startBucket < 0) means wrap-around
  // (endBucket - startBucket == 0) means spanning 0 bucket or all buckets
  // Question: When startTime and endTime map to the same bucket, how to
  //           tell if they span all buckets?
  // Answer:   They span all buckets if the point-in-bucket that endTime maps
  //           to comes before the point-in-bucket that startTime maps to.
  //           We use the following approximation:
  // (interval * num buckets > duration) means spanning more than 1 buckets
  // TODO this does not work for num buckets = 1 or corner cases with
  //      uneven size buckets
  int numSpannedBuckets = endBucket - startBucket;
  if (numSpannedBuckets < 0 ||
      (numSpannedBuckets == 0 &&
       (endTime - startTime) * numBuckets() > duration_)) {
    numSpannedBuckets += numBuckets();
  }

  *startBucketPtr = startBucket;
  *numSpannedBucketsPtr = numSpannedBuckets;
}

template <class T>
void BucketedTimeSeries<T>::clear() {
  for (size_t i = 0; i < buckets_.size(); ++i) {
    buckets_[i].first = T();
    buckets_[i].second = 0;
  }
  empty_ = true;
  sum_ = T();
  count_ = 0;
  latest_time_ = 0;
  first_time_ = 0;
}

template <class T>
void BucketedTimeSeries<T>::update(time_t now) {
  if (empty_) {
    first_time_ = now;
    empty_ = false;
  }

  if (now == latest_time_) {
    return;
  }

  // make sure time doesn't go backwards
  now = std::max(now, latest_time_);

  int cur_bucket = getBucket(now);
  int last_bucket = getBucket(latest_time_);
  int time_since_last = (now - latest_time_);

  // update the time boundaries
  latest_time_ = now;

  if (duration_ == 0) {
    // if we have no duration (all-time timeseries), no more to do in update()
    return;
  }

  if (time_since_last >= duration_) {
    // it's been a while, clear it all
    for (size_t i = 0; i < buckets_.size(); ++i) {
      buckets_[i].first = T();
      buckets_[i].second = 0;
    }
    sum_ = T();
    count_ = 0;
  } else if (cur_bucket != last_bucket) {
    // clear all the buckets between the last time and current time, meaning
    // buckets in the range [(last_bucket+1), cur_bucket]. Note that
    // the bucket (last_bucket+1) is always the oldest bucket we have. Since
    // our array is circular, this cleanup range might drop off one end and
    // loop back around, so we use modular arithmetic.
    for (int b = cur_bucket; b != last_bucket;
         b = ((b + buckets_.size() - 1) % buckets_.size())) {
      sum_ -= buckets_[b].first;
      count_ -= buckets_[b].second;
      buckets_[b].first = T();
      buckets_[b].second = 0;
    }
  }
}

template <class T>
time_t BucketedTimeSeries<T>::getLatestTime() const {
  return latest_time_;
}


template <class T>
void BucketedTimeSeries<T>::addValue(time_t now, const T& val) {
  addValueAggregated(now, val, 1);
}

template <class T>
void BucketedTimeSeries<T>::addValue(time_t now, const T& val, int64_t times) {
  addValueAggregated(now, val * times, times);
}

template <class T>
void BucketedTimeSeries<T>::addValueAggregated(time_t now,
                                               const T& sum,
                                               int64_t nsamples) {
  // make sure time doesn't go backwards
  now = std::max(now, latest_time_);

  // clean up old data
  update(now);

  // add the new value
  int cur_bucket = getBucket(now);

  // update the bucket sum/count
  buckets_[cur_bucket].first += sum;
  buckets_[cur_bucket].second += nsamples;

  // update the aggregate sum/count
  sum_ += sum;
  count_ += nsamples;
}

template <class T>
int BucketedTimeSeries<T>::getBucket(time_t time) const {
  if (duration_ == 0) {
    // only 1 bucket for all-time timeseries
    return 0;
  }

  time %= duration_;
  int bucket = time * buckets_.size() / duration_;

  return bucket;
}

template <class T>
MultiLevelTimeSeries<T>::MultiLevelTimeSeries(int num_levels, int num_buckets,
                                              const int* level_durations)
      : num_buckets_(num_buckets),
        levels_(num_levels, Level(num_buckets, 0)) {
  assert(level_durations != NULL);
  assert(num_levels > 0);

  for (int i = 0; i < num_levels; ++i) {
    if (level_durations[i] == 0) {
      // all-time level has to be the last level present
      assert(i == num_levels - 1);
    } else if (i > 0) {
      // durations have to be strictly increasing
      assert(level_durations[i-1] < level_durations[i]);
    }
    levels_[i].setDuration(level_durations[i]);
  }
}

template <class T>
void MultiLevelTimeSeries<T>::clear() {
  for (size_t i = 0; i < levels_.size(); ++i) {
    levels_[i].clear();
  }
}

template <class T>
void MultiLevelTimeSeries<T>::update(time_t now) {
  for (size_t i = 0; i < levels_.size(); ++i) {
    levels_[i].update(now);
  }
}

template <class T>
void MultiLevelTimeSeries<T>::addValue(time_t now, const T& val) {
  addValueAggregated(now, val, 1);
}

template <class T>
void MultiLevelTimeSeries<T>::addValue(time_t now, const T& val, int64_t times) {
  addValueAggregated(now, val * times, times);
}

template <class T>
void MultiLevelTimeSeries<T>::addValueAggregated(time_t now,
                                                 const T& sum,
                                                 int64_t nsamples) {
  // update all the underlying levels
  for (size_t i = 0; i < levels_.size(); ++i) {
    levels_[i].addValueAggregated(now, sum, nsamples);
  }
}

template <class T>
std::string MultiLevelTimeSeries<T>::getAvgString() const {
  std::ostringstream o;
  for (int i = 0; i < numLevels(); ++i) {
    if (i > 0) { o << "/"; }
    o << getAvg(i);
  }
  return o.str();
}

template <class T>
std::string MultiLevelTimeSeries<T>::getSumString() const {
  std::ostringstream o;
  for (int i = 0; i < numLevels(); ++i) {
    if (i > 0) { o << "/"; }
    o << getSum(i);
  }
  return o.str();
}

template <class T>
std::string MultiLevelTimeSeries<T>::getCountString() const {
  std::ostringstream o;
  for (int i = 0; i < numLevels(); ++i) {
    if (i > 0) { o << "/"; }
    o << getCount(i);
  }
  return o.str();
}

template <class T>
std::string MultiLevelTimeSeries<T>::getRateString() const {
  std::ostringstream o;
  for (int i = 0; i < numLevels(); ++i) {
    if (i > 0) { o << "/"; }
    o << getRate(i);
  }
  return o.str();
}

// outputs the sumue of any MultiLevel timeseries as "sum0/sum1/sum2" where
// the vals are the .sum() of the corresponding level
template <class T>
std::ostream& operator<<(std::ostream& o, const MultiLevelTimeSeries<T>& mlts) {
  return o << mlts.getSumString();
}

} } // !namespace facebook::stats

#endif  // !__STATS_TIMESERIES_INL_H
