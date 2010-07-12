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

#ifndef SCRIBE_BUCKET_STORE_H
#define SCRIBE_BUCKET_STORE_H

#include "Common.h"
#include "Conf.h"
#include "Store.h"

namespace scribe {

/*
 * This store separates messages into many groups based on a
 * hash function, and sends each group to a different store.
 */
class BucketStore : public Store {

 public:
  BucketStore(StoreQueue* storeq,
              const string& category,
              bool multiCategory);
  ~BucketStore();

  StorePtr copy(const string &category);
  bool handleMessages(LogEntryVectorPtr messages);
  bool open();
  bool isOpen();
  void configure(StoreConfPtr configuration, StoreConfPtr parent);
  void close();
  void flush();
  void periodicCheck();

  string getStatus();

 protected:
  enum bucketizerType {
    CONTEXT_LOG,
    RANDOM,      // randomly hash messages without using any key
    KEY_HASH,    // use hashing to split keys into buckets
    KEY_MODULO,  // use modulo to split keys into buckets
    KEY_RANGE    // use bucketRange to compute modulo to split keys into buckets
  };

  bucketizerType bucketType_;
  char delimiter_;
  bool removeKey_;
  bool opened_;
  unsigned long bucketRange_; // used to compute KEY_RANGE bucketizing
  unsigned long numBuckets_;
  vector<StorePtr> buckets_;

  unsigned long bucketize(const string& message);
  string getMessageWithoutKey(const string& message);

 private:
  // disallow copy, assignment, and emtpy construction
  BucketStore();
  BucketStore(BucketStore& rhs);
  BucketStore& operator=(BucketStore& rhs);
  void createBucketsFromBucket(StoreConfPtr configuration,
                               StoreConfPtr bucketConf);
  void createBuckets(StoreConfPtr configuration);
};

} //! namespace scribe

#endif //! SCRIBE_BUCKET_STORE_H
