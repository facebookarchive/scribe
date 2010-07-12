//  Copyright (c) 2007-2008 Facebook
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// See accompanying file LICENSE or visit the Scribe site at:
// http://developers.facebook.com/scribe/
//
// @author Bobby Johnson
// @author James Wang
// @author Jason Sobel
// @author Alex Moskalyuk
// @author Avinash Lakshman
// @author Anthony Giardullo
// @author Jan Oravec
// @author John Song

#ifndef SCRIBE_STORE_H
#define SCRIBE_STORE_H

#include "Common.h"
#include "Conf.h"
#include "StoreQueue.h"

namespace scribe {

class Store;
class StoreQueue;
typedef shared_ptr<Store>  StorePtr;

/*
 * Abstract class to define the interface for a store
 * and implement some basic functionality.
 */
class Store {
 public:
  // Creates an object of the appropriate subclass.
  static StorePtr
    createStore(StoreQueue* storeq,
                const string& type, const string& category,
                bool readable = false, bool multiCategory = false);

  Store(StoreQueue* storeq, const string& category,
        const string &type, bool multiCategory = false);
  virtual ~Store();

  virtual StorePtr copy(const string &category) = 0;
  virtual bool open() = 0;
  virtual bool isOpen() = 0;
  virtual void configure(StoreConfPtr configuration, StoreConfPtr parent);
  virtual void close() = 0;

  // Attempts to store messages and returns true if successful.
  // On failure, returns false and messages contains any un-processed messages
  virtual bool handleMessages(LogEntryVectorPtr messages) = 0;
  virtual void periodicCheck() {}
  virtual void flush() = 0;

  virtual string getStatus();

  // following methods must be overidden to make a store readable
  virtual bool readOldest(/*out*/ LogEntryVectorPtr messages,
                          struct tm* now);
  virtual void deleteOldest(struct tm* now);
  virtual bool replaceOldest(LogEntryVectorPtr messages,
                             struct tm* now);
  virtual bool empty(struct tm* now);

  // don't need to override
  virtual const string& getType();

 protected:
  virtual void setStatus(const string& newStatus);
  string status_;
  string categoryHandled_;
  bool   multiCategory_;             // Whether multiple categories are handled
  string storeType_;

  // Don't ever take this lock for multiple stores at the same time
  Mutex  statusMutex_;

  StoreQueue* storeQueue_;
  StoreConfPtr storeConf_;
 private:
  // disallow copy, assignment, and empty construction
  Store(Store& rhs);
  Store& operator=(Store& rhs);
};

} //! namespace scribe

#endif //! SCRIBE_STORE_H
