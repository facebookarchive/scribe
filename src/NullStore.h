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

#ifndef SCRIBE_NULL_STORE_H
#define SCRIBE_NULL_STORE_H

#include "Common.h"
#include "Conf.h"
#include "Store.h"

namespace scribe {

/*
 * This store intentionally left blank.
 */
class NullStore : public Store {

 public:
  NullStore(StoreQueue* storeq,
            const string& category,
            bool multiCategory);
  virtual ~NullStore();

  StorePtr copy(const string &category);
  bool open();
  bool isOpen();
  void configure(StoreConfPtr configuration, StoreConfPtr parent);
  void close();

  bool handleMessages(LogEntryVectorPtr messages);
  void flush();

  // null stores are readable, but you never get anything
  virtual bool readOldest(LogEntryVectorPtr messages,
                          struct tm* now);
  virtual bool replaceOldest(LogEntryVectorPtr messages,
                             struct tm* now);
  virtual void deleteOldest(struct tm* now);
  virtual bool empty(struct tm* now);


 private:
  // disallow empty constructor, copy and assignment
  NullStore();
  NullStore(Store& rhs);
  NullStore& operator=(Store& rhs);
};

} //! namespace scribe

#endif //! SCRIBE_NULL_STORE_H
