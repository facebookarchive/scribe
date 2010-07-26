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
 * @author Mark Rabkin <mrabkin@facebook.com>
 *
 */

#ifndef __DATASTRUCT_SYNCH_MAP_INL_H__
#define __DATASTRUCT_SYNCH_MAP_INL_H__

#include "datastruct/SynchMap.h"

namespace facebook { namespace datastruct {

/**
 * The SpinLockReleaser class is a special shared_ptr deallocator that, instead
 * of calling 'delete' on the given ptr, instead releases an associated
 * lock.
 *
 * Note: Each instance of this is designed to only be used ONCE!  Make sure
 * you make a new SpinLockReleaser() instance for each shared_ptr you create
 * (except, of course, when copy-constructing shared_ptr).
 */

template<class T>
class SpinLockReleaser {
 public:
  explicit SpinLockReleaser(SpinLock* lock, T* ptr) : ptr_(ptr), lock_(lock) { }

  void operator()(T* ptr) {
    assert(ptr == ptr_);
    assert(lock_ != NULL);
    lock_->unlock();
    lock_ = NULL;
  }
 private:
  T* ptr_;
  SpinLock* lock_;
};


template <class K, class V>
SynchMap<K, V>::SynchMap() { }

template <class K, class V>
SynchMap<K, V>::~SynchMap() { }

template <class K, class V>
bool SynchMap<K, V>::contains(const K& key) const {
  SpinLockHolder h(lock_);
  return (map_.find(key) != map_.end());
}

template <class K, class V>
typename SynchMap<K,V>::LockedValuePtr
SynchMap<K, V>::get(const K& key) {
  LockAndItem* value = getUnlockedImpl(key);
  return createLockedValuePtr(value);
}

template <class K, class V>
typename SynchMap<K,V>::LockedValuePtr
SynchMap<K, V>::getOrCreate(const K& key, const V& defaultVal,
                                          bool* createdPtr) {
  LockAndItem* value = getOrCreateUnlockedImpl(key, defaultVal, createdPtr);
  assert(value != NULL);
  assert(value->first);
  assert(value->second);
  return createLockedValuePtr(value);
}

template <class K, class V>
typename SynchMap<K,V>::LockAndItem
SynchMap<K, V>::getUnlocked(const K& key) {
  LockAndItem* value = getUnlockedImpl(key);
  if (value) {
    return LockAndItem(*value);  // make copy of the value
  } else {
    return LockAndItem(); // NULL for both parts
  }
}

template <class K, class V>
typename SynchMap<K,V>::LockAndItem
SynchMap<K, V>::getOrCreateUnlocked(const K& key, const V& defaultVal,
                                    bool* createdPtr) {
  LockAndItem* value = getOrCreateUnlockedImpl(key, defaultVal, createdPtr);
  assert(value != NULL);
  assert(value->first);
  assert(value->second);

  return LockAndItem(*value);  // make copy of the value
}

template <class K, class V>
void SynchMap<K, V>::set(const K& key, const V& val) {
  bool created;
  LockAndItem* value = getOrCreateUnlockedImpl(key, val, &created);
  if (!created) {
    // item already existing, need to set its value to the right thing
    SpinLockHolder h(*(value->first));
    (*value->second) = val;
  }
}


template <class K, class V>
typename SynchMap<K,V>::LockedValuePtr
SynchMap<K, V>::createLockedValuePtr(LockAndItem* value) {
  if (!value || !value->first || !value->second) {
    return LockedValuePtr();
  }

  // Acquire the value's lock and return a special shared_ptr<> that
  // will release the lock when it is destroyed.
  SpinLock* lock = value->first.get();
  lock->lock();
  V* rawItem = value->second.get();
  return LockedValuePtr(rawItem, SpinLockReleaser<V>(lock, rawItem));
}

template <class K, class V>
typename SynchMap<K,V>::LockAndItem*
SynchMap<K, V>::getUnlockedImpl(const K& key) {
  SpinLockHolder h(lock_);
  typename MapType::iterator it = map_.find(key);
  if (it == map_.end()) {
    return NULL;
  }

  LockAndItem* value = &(it->second);
  assert(value->first);
  assert(value->second);
  return value;
}

template <class K, class V>
typename SynchMap<K,V>::LockAndItem*
SynchMap<K, V>::getOrCreateUnlockedImpl(const K& key, const V& defaultVal,
                                        bool* createdPtr) {
  SpinLockHolder h(lock_);

  if (createdPtr) {
    *createdPtr = false;
  }

  // creates new entry if necessary
  LockAndItem* value = &(map_[key]);

  if (value->first) {
    // this item was already existing, return it
    assert(value->second);
    return value;
  }

  // initialize the new item
  assert(!value->second);
  value->first.reset(new SpinLock());
  value->second.reset(new V(defaultVal));

  if (createdPtr) {
    *createdPtr = true;
  }

  assert(value->first);
  assert(value->second);

  return value;
}

}}

#endif // __DATASTRUCT_SYNCH_MAP_INL_H__
