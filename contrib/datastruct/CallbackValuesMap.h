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
 *
 * @author Mark Rabkin (mrabkin@facebook.com)
 *
 * This class provides a "dynamic" callback registry for callbacks that return
 * a value of a given type.  There is a mechanism to invoke a single callback
 * (fetch a single value) and one to invoke all the callbacks and fetch all
 * values.
 *
 */

#ifndef __COMMON_DATASTRUCT_CALLBACK_VALUES_MAP_H
#define __COMMON_DATASTRUCT_CALLBACK_VALUES_MAP_H

#include <tr1/functional>
#include <map>
#include <string>

#include <boost/utility.hpp>

#include "thrift/concurrency/Mutex.h"

namespace facebook { namespace datastruct {

// make tr1 stuff easier to read
namespace tr1 = std::tr1;

using apache::thrift::concurrency::Mutex;
using apache::thrift::concurrency::Guard;

template<typename T>
class CallbackValuesMap : boost::noncopyable {
 public:
  typedef std::map<std::string, T> ValuesMap;     // output type to return all values
  typedef tr1::function<T()> Callback;  // callback type

   /** Returns all the values in the map by invoking all the callbacks */
   void getValues(ValuesMap* output) const;

   /**
    * If the name is present, invokes the callback and places the result
    * in 'output' and returns true; returns false otherwise.
    */
    bool getValue(const std::string& name, T* output) const;

   /** Returns true if the name is present in the map. */
   bool contains(const std::string& name) const;

   /**
    * Registers a given callback as associated with the given name.  Note
    * that a copy of the given cob is made, and that any previous registered
    * cob (if any) is replaced.
    */
    void registerCallback(const std::string& name, const Callback& cob);

 private:
   Mutex mutex_;
   typedef std::map<std::string, Callback> CallbackMap;
   CallbackMap callbackMap_;
};

} }  // !namespace facebook::datastruct

#include "datastruct/CallbackValuesMap-inl.h"

#endif  // !__COMMON_DATASTRUCT_CALLBACK_VALUES_MAP_H

