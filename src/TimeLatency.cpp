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
// @author Anthony Giardullo
// @author John Song

#include "TimeLatency.h"

using namespace boost;

namespace scribe {

// get timestamp in ms
// assumes user checked isTimeStampAndHopCountPresent
unsigned long getTimeStamp(const LogEntry& message) {

  if (!message.__isset.metadata) {
    LOG_OPER("Warning: Timestamp not present");
    return 0;
  }

  const map<string,string>& myMetadata = message.metadata;
  map<string,string>::const_iterator iter;
  iter = myMetadata.find(kMetadataTimestamp);
  if (iter == myMetadata.end()) {
    LOG_OPER("Warning: Timestamp not present");
    return 0;
  } else {
    const string& timestamp = iter->second;
    unsigned long retVal =  std::strtoul(timestamp.c_str(), NULL, 10);
    if (retVal == ULONG_MAX && errno == ERANGE) {
      LOG_OPER("Warning: Timestamp corrupted");
      return 0;
    } else {
      return retVal;
    }
  }
}

// update timeandhop
void updateTimeStamp(LogEntry& message, unsigned long ts) {
  map<string,string>& myMetadata = message.metadata;
  char buf[std::numeric_limits<unsigned long>::digits10 + 3];
  std::snprintf(buf, sizeof(buf), "%lu", ts);
  myMetadata[kMetadataTimestamp].assign(buf);
  message.__isset.metadata = true;
}

// remove timestamp from the message
void removeTimeStamp(LogEntry& message) {

  map<string,string>& myMetadata = message.metadata;
  myMetadata.erase(kMetadataTimestamp);
  if (myMetadata.empty()) {
    message.__isset.metadata = false;
  }
}

} //! namespace scribe
