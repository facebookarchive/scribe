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

#include "Common.h"
#include "NullStore.h"
#include "ScribeServer.h"

namespace scribe {

NullStore::NullStore(StoreQueue* storeq,
                     const string& category,
                     bool multiCategory)
  : Store(storeq, category, "null", multiCategory)
{}

NullStore::~NullStore() {
}

StorePtr NullStore::copy(const string& category) {
  StorePtr copied(new NullStore(storeQueue_, category, multiCategory_));
  return copied;
}

bool NullStore::open() {
  return true;
}

bool NullStore::isOpen() {
  return true;
}

void NullStore::configure(StoreConfPtr configuration, StoreConfPtr parent) {
  Store::configure(configuration, parent);
}

void NullStore::close() {
}

bool NullStore::handleMessages(LogEntryVectorPtr messages) {
  // add null in count.  this will great for testing.
  g_handler->stats.addCounter(StatCounters::kNullIn, messages->size());
  g_handler->incCounter(categoryHandled_, "ignored", messages->size());
  return true;
}

void NullStore::flush() {
}

bool NullStore::readOldest(/*out*/ LogEntryVectorPtr messages,
                       struct tm* now) {
  return true;
}

bool NullStore::replaceOldest(LogEntryVectorPtr messages,
                              struct tm* now) {
  return true;
}

void NullStore::deleteOldest(struct tm* now) {
}

bool NullStore::empty(struct tm* now) {
  return true;
}

} //! namespace scribe
