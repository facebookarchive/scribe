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

#include "BucketStore.h"
#include "BufferStore.h"
#include "Common.h"
#include "FileStore.h"
#include "NetworkStore.h"
#include "NullStore.h"
#include "ScribeServer.h"
#include "Store.h"
#include "ThriftFileStore.h"

using namespace scribe::thrift;

namespace scribe {

StorePtr Store::createStore(StoreQueue* storeq,
                            const string& type,
                            const string& category,
                            bool readable,
                            bool multiCategory) {
  if (0 == type.compare("file")) {
    return StorePtr(new FileStore(storeq, category, multiCategory, readable));

  } else if (0 == type.compare("buffer")) {
    return StorePtr(new BufferStore(storeq,category, multiCategory));

  } else if (0 == type.compare("network")) {
    return StorePtr(new NetworkStore(storeq, category, multiCategory));

  } else if (0 == type.compare("bucket")) {
    return StorePtr(new BucketStore(storeq, category, multiCategory));

  } else if (0 == type.compare("thriftfile")) {
    return StorePtr(new ThriftFileStore(storeq, category, multiCategory));

  } else if (0 == type.compare("null")) {
    return StorePtr(new NullStore(storeq, category, multiCategory));

  } else {
    return StorePtr();
  }
}

Store::Store(StoreQueue* storeq,
             const string& category,
             const string &type,
             bool multiCategory)
  : categoryHandled_(category),
    multiCategory_(multiCategory),
    storeType_(type),
    storeQueue_(storeq) {
}

Store::~Store() {
}

void Store::configure(StoreConfPtr configuration, StoreConfPtr parent) {
  storeConf_ = configuration;
  storeConf_->setParent(parent);
}

void Store::setStatus(const string& newStatus) {
  Guard g(statusMutex_);

  status_ = newStatus;
}

string Store::getStatus() {
  Guard g(statusMutex_);

  return status_;
}

bool Store::readOldest(/*out*/ LogEntryVectorPtr messages,
                       struct tm* now) {
  LOG_OPER("[%s] ERROR: attempting to read from a write-only store",
          categoryHandled_.c_str());
  return false;
}

bool Store::replaceOldest(LogEntryVectorPtr messages,
                          struct tm* now) {
  LOG_OPER("[%s] ERROR: attempting to read from a write-only store",
          categoryHandled_.c_str());
  return false;
}

void Store::deleteOldest(struct tm* now) {
   LOG_OPER("[%s] ERROR: attempting to read from a write-only store",
            categoryHandled_.c_str());
}

bool Store::empty(struct tm* now) {
  LOG_OPER("[%s] ERROR: attempting to read from a write-only store",
            categoryHandled_.c_str());
  return true;
}

const string& Store::getType() {
  return storeType_;
}

} //! namespace scribe
