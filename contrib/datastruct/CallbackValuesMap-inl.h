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

#ifndef __COMMON_DATASTRUCT_CALLBACK_VALUES_MAP_INL_H
#define __COMMON_DATASTRUCT_CALLBACK_VALUES_MAP_INL_H

#include "datastruct/CallbackValuesMap.h"

namespace facebook { namespace datastruct {

template <typename T>
void CallbackValuesMap<T>::getValues(ValuesMap* output) const {
  assert(output != NULL);
  Guard g(mutex_);

  for (typename CallbackMap::const_iterator it = callbackMap_.begin();
       it != callbackMap_.end();
       ++ it) {
    (*output)[it->first] = it->second();
  }
}

template <typename T>
bool CallbackValuesMap<T>::getValue(const std::string& name, T* output) const {
  assert(output != NULL);
  Guard g(mutex_);

  typename CallbackMap::const_iterator it = callbackMap_.find(name);
  if (it != callbackMap_.end()) {
    const Callback callback = it->second;
    *output = callback();
    return true;
  } else {
    return false;
  }
}

template <typename T>
bool CallbackValuesMap<T>::contains(const std::string& name) const {
  Guard g(mutex_);
  return (callbackMap_.find(name) != callbackMap_.end());
}

template <typename T>
void CallbackValuesMap<T>::registerCallback(const std::string& name,
                                            const Callback& cob) {
  Guard g(mutex_);
  callbackMap_[name] = cob;
}


} }  // !namespace facebook::datastruct

#endif  // !__COMMON_DATASTRUCT_CALLBACK_VALUES_MAP_INL_H

