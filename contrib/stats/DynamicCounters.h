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
 * This class provides a "dynamic" fb303 counter system by
 * implementing a callback registry for each counter name -- at query
 * time for a counter, the callback will be called and should produce
 * the current counter value.
 *
 */

#ifndef __STATS_DYNAMIC_COUNTERS_H
#define __STATS_DYNAMIC_COUNTERS_H

#include <string>
#include <tr1/functional>
#include <boost/shared_ptr.hpp>

#include "datastruct/CallbackValuesMap.h"
#include "thrift/concurrency/Mutex.h"

namespace facebook { namespace stats {

// make tr1 stuff easier to read
namespace tr1 = std::tr1;

using apache::thrift::concurrency::Mutex;
using apache::thrift::concurrency::Guard;
using datastruct::CallbackValuesMap;

// a map of string-valued callbacks
typedef CallbackValuesMap<std::string> DynamicStrings;

// a map of int64_t (CounterType) valued callbacks, with some extra
// functions added for backwards compatibility with the previous version.
typedef int64_t CounterType;
class DynamicCounters : public CallbackValuesMap<CounterType> {
 public:
  typedef CallbackValuesMap<CounterType>::Callback Callback;

  // for backwards compat
  void getCounters(std::map<std::string, CounterType>* output) {
    return getValues(output);
  }

  // for backwards compat
  bool getCounter(const std::string& name, CounterType* output) const {
    return getValue(name, output);
  }
};


} }  // !namespace facebook::stats

#endif  // !__STATS_DYNAMIC_COUNTERS_H

