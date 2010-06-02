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
// @author Avinash Lakshman
// @author Anthony Giardullo

#include "scribe/src/common.h"
#include "scribe/src/scribe_server.h"

using namespace scribe::concurrency;

using boost::shared_ptr;

/*
 * Network configuration and directory services
 */

bool scribe::network_config::getService(const std::string& serviceName,
                                        const std::string& options,
                                        server_vector_t& _return) {
  return false;
}

/*
 * Concurrency mechanisms
 */

shared_ptr<ReadWriteMutex> scribe::concurrency::createReadWriteMutex() {
  return shared_ptr<ReadWriteMutex>(new ReadWriteMutex());
}

/*
 * Time functions
 */

unsigned long scribe::clock::nowInMsec() {
  // There is a minor race condition between the 2 calls below,
  // but the chance is really small.

  // Get current time in timeval
  struct timeval tv;
  gettimeofday(&tv, NULL);

  // Get current time in sec
  time_t sec = time(NULL);

  return ((unsigned long)sec) * 1000 + (tv.tv_usec / 1000);
}

/*
 * Hash functions
 */

uint32_t scribe::integerhash::hash32(uint32_t key) {
  return key;
}

uint32_t scribe::strhash::hash32(const char *s) {
  // Use the djb2 hash (http://www.cse.yorku.ca/~oz/hash.html)
  if (s == NULL) {
    return 0;
  }
  uint32_t hash = 5381;
  int c;
  while ((c = *s++)) {
    hash = ((hash << 5) + hash) + c; // hash * 33 + c
  }
  return hash;
}
