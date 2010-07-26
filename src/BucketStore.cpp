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
#include "BucketStore.h"
#include "ScribeServer.h"

using namespace std;

static const char kDefaultBucketStoreDelimiter = ':';

namespace scribe {

BucketStore::BucketStore(StoreQueue* storeq,
                        const string& category,
                        bool multiCategory)
  : Store(storeq, category, "bucket", multiCategory),
    bucketType_(CONTEXT_LOG),
    delimiter_(kDefaultBucketStoreDelimiter),
    removeKey_(false),
    opened_(false),
    bucketRange_(0),
    numBuckets_(1) {
}

BucketStore::~BucketStore() {

}

// Given a single bucket definition, create multiple buckets
void BucketStore::createBucketsFromBucket(StoreConfPtr configuration,
    StoreConfPtr bucketConf) {
  string errorMesg, bucketSubdir, type, path, failureBucket;
  bool needsBucketSubdir = false;
  unsigned long bucketOffset = 0;
  StoreConfPtr tmp;

  // check for extra bucket definitions
  if (configuration->getStore("bucket0", &tmp) ||
      configuration->getStore("bucket1", &tmp)) {
    errorMesg = "bucket store has too many buckets defined";
    goto handle_error;
  }

  bucketConf->getString("type", &type);
  if (type != "file" && type != "thriftfile") {
    errorMesg = "store contained in a bucket store must have a type of ";
    errorMesg += "either file or thriftfile if not defined explicitely";
    goto handle_error;
  }

  needsBucketSubdir = true;
  if (!configuration->getString("bucket_subdir", &bucketSubdir)) {
    errorMesg = "bucketizer containing file stores must have a bucket_subdir";
    goto handle_error;
  }
  if (!bucketConf->getString("file_path", &path)) {
    errorMesg = "file store contained by bucketizer must have a file_path";
    goto handle_error;
  }

  // set starting bucket number if specified
  configuration->getUnsigned("bucket_offset", &bucketOffset);

  // check if failure bucket was given a different name
  configuration->getString("failure_bucket", &failureBucket);

  // We actually create numBuckets_ + 1 stores. Messages are normally
  // hashed into buckets 1 through numBuckets_, and messages that can't
  // be hashed are put in bucket 0.

  for (unsigned int i = 0; i <= numBuckets_; ++i) {

    StorePtr newStore =
      createStore(storeQueue_, type, categoryHandled_, false, multiCategory_);

    if (!newStore) {
      errorMesg = "can't create store of type: ";
      errorMesg += type;
      goto handle_error;
    }

    // For file/thrift file buckets, create unique filepath for each bucket
    if (needsBucketSubdir) {
      if (i == 0 && !failureBucket.empty()) {
        bucketConf->setString("file_path", path + '/' + failureBucket);
      } else {
        // the bucket number is appended to the file path
        unsigned int bucketId = i + bucketOffset;

        ostringstream oss;
        oss << path << '/' << bucketSubdir << setw(3) << setfill('0')
            << bucketId;
        bucketConf->setString("file_path", oss.str());
      }
    }

    buckets_.push_back(newStore);
    newStore->configure(bucketConf, storeConf_);
  }

  return;

handle_error:
  setStatus(errorMesg);
  LOG_OPER("[%s] Bad config - %s", categoryHandled_.c_str(),
           errorMesg.c_str());
  numBuckets_ = 0;
  buckets_.clear();
}

// Checks for a bucket definition for every bucket from 0 to numBuckets_
// and configures each bucket
void BucketStore::createBuckets(StoreConfPtr configuration) {
  string errorMesg, tmpString;
  StoreConfPtr tmp;

  if (configuration->getString("bucket_subdir", &tmpString)) {
    errorMesg = "cannot have bucket_subdir when defining multiple buckets";
    goto handle_error;
  }

  if (configuration->getString("bucket_offset", &tmpString)) {
    errorMesg = "cannot have bucket_offset when defining multiple buckets";
    goto handle_error;
  }

  if (configuration->getString("failure_bucket", &tmpString)) {
    errorMesg = "cannot have failure_bucket when defining multiple buckets";
    goto handle_error;
  }

  // Configure stores named 'bucket0, bucket1, bucket2, ... bucket{numBuckets_}
  for (unsigned long i = 0; i <= numBuckets_; i++) {
    StoreConfPtr   bucketConf;
    string       type, bucketName;
    stringstream ss;

    ss << "bucket" << i;
    bucketName = ss.str();

    if (!configuration->getStore(bucketName, &bucketConf)) {
      errorMesg = "could not find bucket definition for " + bucketName;
      goto handle_error;
    }

    if (!bucketConf->getString("type", &type)) {
      errorMesg = "store contained in a bucket store must have a type";
      goto handle_error;
    }

    StorePtr bucket =
      createStore(storeQueue_, type, categoryHandled_, false, multiCategory_);

    buckets_.push_back(bucket);
    //add bucket id configuration
    bucketConf->setUnsigned("bucket_id", i);
    bucketConf->setUnsigned("network::bucket_id", i);
    bucketConf->setUnsigned("file::bucket_id", i);
    bucketConf->setUnsigned("thriftfile::bucket_id", i);
    bucketConf->setUnsigned("buffer::bucket_id", i);
    bucket->configure(bucketConf, storeConf_);
  }

  // Check if an extra bucket is defined
  if (configuration->getStore("bucket" + (numBuckets_ + 1), &tmp)) {
    errorMesg = "bucket store has too many buckets defined";
    goto handle_error;
  }

  return;

handle_error:
  setStatus(errorMesg);
  LOG_OPER("[%s] Bad config - %s", categoryHandled_.c_str(),
           errorMesg.c_str());
  numBuckets_ = 0;
  buckets_.clear();
}

/**
   * Buckets in a bucket store can be defined explicitly or implicitly:
   *
   * #Explicitly
   * <store>
   *   type=bucket
   *   num_buckets=2
   *   bucket_type=key_hash
   *
   *   <bucket0>
   *     ...
   *   </bucket0>
   *
   *   <bucket1>
   *     ...
   *   </bucket1>
   *
   *   <bucket2>
   *     ...
   *   </bucket2>
   * </store>
   *
   * #Implicitly
   * <store>
   *   type=bucket
   *   num_buckets=2
   *   bucket_type=key_hash
   *
   *   <bucket>
   *     ...
   *   </bucket>
   * </store>
   */
void BucketStore::configure(StoreConfPtr configuration, StoreConfPtr parent) {
  Store::configure(configuration, parent);

  string errorMesg, bucketizerStr, removeKeyStr;
  unsigned long delimLong = 0;
  StoreConfPtr bucketConf;
  //set this to true for bucket types that have a delimiter
  bool needDelimiter = false;

  configuration->getString("bucket_type", &bucketizerStr);

  // Figure out th bucket type from the bucketizer string
  if (0 == bucketizerStr.compare("context_log")) {
    bucketType_ = CONTEXT_LOG;
  } else if (0 == bucketizerStr.compare("random")) {
      bucketType_ = RANDOM;
  } else if (0 == bucketizerStr.compare("key_hash")) {
    bucketType_ = KEY_HASH;
    needDelimiter = true;
  } else if (0 == bucketizerStr.compare("key_modulo")) {
    bucketType_ = KEY_MODULO;
    needDelimiter = true;
  } else if (0 == bucketizerStr.compare("key_range")) {
    bucketType_ = KEY_RANGE;
    needDelimiter = true;
    configuration->getUnsigned("bucket_range", &bucketRange_);

    if (bucketRange_ == 0) {
      LOG_OPER("[%s] config warning - bucket_range is 0",
               categoryHandled_.c_str());
    }
  }

  // This is either a key_hash or key_modulo, not context log, figure out
  // the delimiter and store it
  if (needDelimiter) {
    configuration->getUnsigned("delimiter", &delimLong);
    if (delimLong > 255) {
      LOG_OPER("[%s] config warning - delimiter is too large to fit in a char, "
               "using default", categoryHandled_.c_str());
      delimiter_ = kDefaultBucketStoreDelimiter;
    } else if (delimLong == 0) {
      LOG_OPER("[%s] config warning - delimiter is zero, using default",
               categoryHandled_.c_str());
      delimiter_ = kDefaultBucketStoreDelimiter;
    } else {
      delimiter_ = (char)delimLong;
    }
  }

  // Optionally remove the key and delimiter of each message before bucketizing
  configuration->getString("remove_key", &removeKeyStr);
  if (removeKeyStr == "yes") {
    removeKey_ = true;

    if (bucketType_ == CONTEXT_LOG) {
      errorMesg = "Bad config - bucketizer store of type context_log do not "
                  "support remove_key";
      goto handle_error;
    }
  }

  if (!configuration->getUnsigned("num_buckets", &numBuckets_)) {
    errorMesg = "Bad config - bucket store must have num_buckets";
    goto handle_error;
  }

  // Buckets can be defined explicitely or by specifying a single "bucket"
  if (configuration->getStore("bucket", &bucketConf)) {
    createBucketsFromBucket(configuration, bucketConf);
  } else {
    createBuckets(configuration);
  }

  return;

handle_error:
  setStatus(errorMesg);
  LOG_OPER("[%s] %s", categoryHandled_.c_str(), errorMesg.c_str());
  numBuckets_ = 0;
  buckets_.clear();
}

bool BucketStore::open() {
  // we have one extra bucket for messages we can't hash
  if (numBuckets_ <= 0 || buckets_.size() != numBuckets_ + 1) {
    LOG_OPER("[%s] Can't open bucket store with <%d> of <%lu> buckets",
             categoryHandled_.c_str(), (int)buckets_.size(), numBuckets_);
    return false;
  }

  for (vector<StorePtr>::iterator iter = buckets_.begin();
       iter != buckets_.end();
       ++iter) {

    if (!(*iter)->open()) {
      close();
      opened_ = false;
      return false;
    }
  }
  opened_ = true;
  return true;
}

bool BucketStore::isOpen() {
  return opened_;
}

void BucketStore::close() {
  // don't check opened, because we can call this when some, but
  // not all, contained stores are opened. Calling close on a contained
  // store that's already closed shouldn't hurt anything.
  for (vector<StorePtr>::iterator iter = buckets_.begin();
       iter != buckets_.end();
       ++iter) {
    (*iter)->close();
  }
  opened_ = false;
}

void BucketStore::flush() {
  for (vector<StorePtr>::iterator iter = buckets_.begin();
       iter != buckets_.end();
       ++iter) {
    (*iter)->flush();
  }
}

string BucketStore::getStatus() {

  string retVal = Store::getStatus();

  vector<StorePtr>::iterator iter = buckets_.begin();
  while (retVal.empty() && iter != buckets_.end()) {
    retVal = (*iter)->getStatus();
    ++iter;
  }
  return retVal;
}

// Call periodicCheck on all containing stores
void BucketStore::periodicCheck() {
  // Call periodic check on all bucket stores in a random order
  uint32_t sz = buckets_.size();
  vector<uint32_t> storeIndex(sz);
  for (uint32_t i = 0; i < sz; ++i) {
    storeIndex[i] = i;
  }
  random_shuffle(storeIndex.begin(), storeIndex.end());

  for (uint32_t i = 0; i < sz; ++i) {
    uint32_t idx = storeIndex[i];
    buckets_[idx]->periodicCheck();
  }
}

StorePtr BucketStore::copy(const string &category) {
  StorePtr copied(new BucketStore(storeQueue_, category, multiCategory_));

  BucketStore* store = static_cast<BucketStore*>(copied.get());
  store->numBuckets_ = numBuckets_;
  store->bucketType_ = bucketType_;
  store->delimiter_ = delimiter_;

  for (vector<StorePtr>::iterator iter = buckets_.begin();
       iter != buckets_.end();
       ++iter) {
    store->buckets_.push_back((*iter)->copy(category));
  }

  return copied;
}

/*
 * Bucketize <messages> and try to send to each contained bucket store
 * At the end of the function <messages> will contain all the messages that
 * could not be processed
 * Returns true if all messages were successfully sent, false otherwise.
 */
bool BucketStore::handleMessages(LogEntryVectorPtr messages) {
  bool success = true;

  LogEntryVectorPtr failedMessages(new LogEntryVector);
  vector<LogEntryVectorPtr> bucketedMessages;
  // bucket numbers are 1-based while bucket #0 is reserved for
  // error handling
  bucketedMessages.resize(numBuckets_ + 1);

  if (numBuckets_ == 0) {
    LOG_OPER("[%s] Failed to write - no buckets configured",
             categoryHandled_.c_str());
    setStatus("Failed write to bucket store");
    return false;
  }

  // batch messages by bucket
  for (LogEntryVector::iterator iter = messages->begin();
       iter != messages->end();
       ++iter) {
    unsigned bucket = bucketize((*iter)->message);

    if (!bucketedMessages[bucket]) {
      bucketedMessages[bucket].reset(new LogEntryVector);
    }

    bucketedMessages[bucket]->push_back(*iter);
  }

  // handle all batches of messages
  for (unsigned long i = 0; i <= numBuckets_; i++) {
    LogEntryVectorPtr batch = bucketedMessages[i];

    if (batch) {

      if (removeKey_) {
        // Create new set of messages with keys removed
        LogEntryVectorPtr keyRemoved(new LogEntryVector);

        for (LogEntryVector::iterator iter = batch->begin();
             iter != batch->end();
             ++iter) {
          const LogEntryPtr& original = *iter;

          LogEntryPtr entry(new LogEntry);
          *entry = *original;
          // strip the key from message
          entry->message = getMessageWithoutKey(entry->message);
          keyRemoved->push_back(entry);
        }
        batch = keyRemoved;
      }

      if (!buckets_[i]->handleMessages(batch)) {
        // keep track of messages that were not handled
        failedMessages->insert(failedMessages->end(),
                               bucketedMessages[i]->begin(),
                               bucketedMessages[i]->end());
        success = false;
      }
    }
  }

  if (!success) {
    // return failed logentrys in messages
    messages->swap(*failedMessages);
  }

  return success;
}

// Return the bucket number a message must be put into;
// Return 0 on any errors
unsigned long BucketStore::bucketize(const string& message) {

  string::size_type length = message.length();

  if (bucketType_ == CONTEXT_LOG) {
    // the key is in ascii after the third delimiter
    char delim = 1;
    string::size_type pos = 0;
    for (int i = 0; i < 3; ++i) {
      pos = message.find(delim, pos);
      if (pos == string::npos || length <= pos + 1) {
        return 0;
      }
      ++pos;
    }
    if (message[pos] == delim) {
      return 0;
    }

    uint32_t id = strtoul(message.substr(pos).c_str(), NULL, 10);
    if (id == 0) {
      return 0;
    }

    if (numBuckets_ == 0) {
      return 0;
    } else {
      return (scribe::integerhash::hash32(id) % numBuckets_) + 1;
    }
  } else if (bucketType_ == RANDOM) {
    // return any random bucket
    return (rand() % numBuckets_) + 1;
  } else {
    // just hash everything before the first user-defined delimiter
    string::size_type pos = message.find(delimiter_);
    if (pos == string::npos) {
      // if no delimiter found, write to bucket 0
      return 0;
    }

    string key = message.substr(0, pos).c_str();
    if (key.empty()) {
      // if no key found, write to bucket 0
      return 0;
    }

    if (numBuckets_ == 0) {
      return 0;
    } else {
      switch (bucketType_) {
        case KEY_MODULO:
          // No hashing, just simple modulo
          return (atol(key.c_str()) % numBuckets_) + 1;
          break;
        case KEY_RANGE:
          if (bucketRange_ == 0) {
            return 0;
          } else {
            // Calculate what bucket this key would fall into if we used
            // bucket_range to compute the modulo
           double keyMod = atol(key.c_str()) % bucketRange_;
           return (unsigned long) ((keyMod / bucketRange_) * numBuckets_) + 1;
          }
          break;
        case KEY_HASH:
        default:
          // Hashing by default.
          return (scribe::strhash::hash32(key.c_str()) % numBuckets_) + 1;
          break;
      }
    }
  }

  return 0;
}

string BucketStore::getMessageWithoutKey(const string& message) {
  string::size_type pos = message.find(delimiter_);

  if (pos == string::npos) {
    return message;
  }

  return message.substr(pos+1);
}

} //! namespace scribe
