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
 * @author Mark Rabkin <mrabkin@facebook.com>
 *
 */

#ifndef __DATASTRUCT_SYNCH_MAP_H__
#define __DATASTRUCT_SYNCH_MAP_H__

#include <utility>
#include <tr1/unordered_map>
#include <boost/shared_ptr.hpp>

#include "thrift/concurrency/Mutex.h"

namespace facebook {

typedef apache::thrift::concurrency::Mutex SpinLock;
typedef apache::thrift::concurrency::Guard SpinLockHolder;
}

namespace facebook { namespace datastruct {

/**
 * SynchMap is a hash map that implements thread safety
 * by having a global mutex for the hash structure itself, as well
 * as a mutex that protects every value in the map.
 *
 * The get() call locks the value's mutex and returns a special shared_ptr<>
 * that, instead of deleting the value, releases the mutex when it is
 * destroyed.
 *
 * Note that currently this is a very simple wrapper, and thus doesn't expose
 * any iterators or other functionality.
 *
 * Also note that you cannot delete items from this map at this time; it's
 * harder to get the deletion code right when other people might have the
 * item checked out.
 *
 * Example:
 *
 *   SynchMap<int, string> map;
 *
 *   // -------
 *   // set two values
 *   // -------
 *   map.set(10, string("10"));
 *   map.set(20, string("20"));
 *
 *   // -------
 *   // lock a key and edit its contents
 *   // -------
 *   shared_ptr<string> ptr = map.get(10);   // lock for key == 10 is now held
 *   if (ptr) {
 *     *ptr = "X";
 *   }
 *   ptr.reset();  // release the lock on key == 10
 *
 *   // -------
 *   // alternate way to use a nested scope to lock a key
 *   // -------
 *   {
 *     shared_ptr<string> ptr = map.get(10);  // locks the key
 *     ...
 *   }  // key is unlocked here
 *
 *
 * TODO(mrabkin): Add iterators
 * TODO(mrabkin): Add multi-operation locking so you can keep map locked
 *   and do several lookups in a row to reduce contention
 */

template <class K, class V>
class SynchMap {
 public:  // type definitions
  typedef K KeyType;
  typedef V ValueType;

  // special shared_ptr<> that holds lock while it exists
  typedef boost::shared_ptr<ValueType> LockedValuePtr;

  // pair of map item and its associated lock
  typedef std::pair<boost::shared_ptr<SpinLock>, boost::shared_ptr<ValueType> >
      LockAndItem;

 public:
  SynchMap();
  ~SynchMap();

  /**
   * Checks if the map contains key.  Note that this state might change
   * at any time (immediately) after returning.
   */
  bool contains(const KeyType& key) const;

  /**
   * Returns a shared_ptr to the value stored in the map for the given key, and
   * locks the per-value mutex associated with that value.  Modifiying the
   * object pointed to by this shared_ptr will modify the value stored in the
   * map.  When the returned shared_ptr (and all of its copies, if any) are
   * reset, the per-value mutex is released.  In other words, a thread
   * will maintain exclusive control over the given key until it resets
   * (or destroys) the returned shared_ptr.
   *
   * If the value isn't present in the map, an empty shared_ptr is returned
   * and no locks are held.
   *
   * --
   * NOTE: Do _not_ store the LockedValuePtr for a long time.  This will
   * keep the mutex locked and not allow anyone else to access this item.
   * --
   *
   */
  LockedValuePtr get(const KeyType& key);

  /**
   * Behaves identically to get(), except that if the value is missing from the
   * map, an entry is created with the defaultVal provided and then returned.
   * V must be copy-constructible.  If createdPtr is not NULL, its contents are
   * set to 'true' if an item was just created and 'false' otherwise.
   *
   * --
   * NOTE: Do _not_ store the LockedValuePtr for a long time.  This will
   * keep the mutex locked and not allow anyone else to access this item.
   * --
   */
  LockedValuePtr getOrCreate(const KeyType& key,
                             const ValueType& defaultVal,
                             bool* createdPtr = NULL);

  /**
   * If the item exists in the map, returns a regular vanilla shared_ptr to
   * the item, and the item's associated spinlock.  This allows users to manage
   * their own locking of the item.  If the item is missing, a pair with NULL
   * shared_ptrs<> for the item and lock is returned.
   *
   * --
   * NOTE: You _must_ lock the spinlock with a SpinLockHolder before accessing
   * the value of the item.
   * --
   */
  LockAndItem getUnlocked(const KeyType& key);

  /**
   * Behaves identically to getUnlocked(), except that if the value is missing
   * from the map, an entry is created with the defaultVal provided and then
   * returned.  V must be copy-constructible.  If createdPtr is not NULL, its
   * contents are set to 'true' if an item was just created and 'false'
   * otherwise.
   *
   * --
   * NOTE: You _must_ lock the spinlock with a SpinLockHolder before accessing
   * the value of the item.
   * --
   */
  LockAndItem getOrCreateUnlocked(const KeyType& key,
                                  const ValueType& defaultVal,
                                  bool* createdPtr = NULL);

  /**
   * Given an item, returned by getUnlocked() or getOrCreatedUnlocked(), locks
   * its associated lock and returns a LockedValuePtr that will release the
   * lock when the pointer (and any copies that may have been made) are
   * destroyed.
   *
   * This can be used to convert items returned by the 'Unlocked' versions of
   * the getters to ones returned by the regular get() and getOrCreate().
   */
  LockedValuePtr createLockedValuePtr(LockAndItem* item);

  /**
   * Sets the value associated with the given key.  Note that no locks are held
   * after the set() is complete, so the value might change at any time
   * (immediately) after returning.
   */
  void set(const KeyType& key, const ValueType& val);
 private:

  LockAndItem* getUnlockedImpl(const KeyType& key);
  LockAndItem* getOrCreateUnlockedImpl(const KeyType& key,
                                           const ValueType& defaultVal,
                                           bool* createdPtr = NULL);

 private:
  typedef std::tr1::unordered_map<KeyType, LockAndItem>  MapType;

  MapType  map_;
  SpinLock lock_;
};


}}

#endif // __DATASTRUCT_SYNCH_MAP_H__
