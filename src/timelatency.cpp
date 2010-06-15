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

#include "timelatency.h"

const string METADATA_TIMESTAMP = string("timestamp");

using namespace apache::thrift;
using namespace scribe::thrift;
using namespace std;
using boost::shared_ptr;

// get timestamp in ms
// assumes user checked isTimeStampAndHopCountPresent
unsigned long getTimeStamp(const LogEntry& message) {

  if (!message.__isset.metadata) {
    LOG_OPER("Warning: Timestamp not present");
    return 0;
  }

  const map <string,string> & mymetadata = message.metadata;
  map <string,string>::const_iterator iter;
  iter = mymetadata.find(METADATA_TIMESTAMP);
  if (iter == mymetadata.end()) {
    LOG_OPER("Warning: Timestamp not present");
    return 0;
  } else {
    return boost::lexical_cast<unsigned long> (iter->second);
  }
}

// update timeandhop
void updateTimeStamp(LogEntry& message, unsigned long ts) {

  map <string,string> & mymetadata = message.metadata;
  mymetadata[METADATA_TIMESTAMP] = boost::lexical_cast<string> (ts);
  message.__isset.metadata = true;
}

// remove timestamp from the message
void removeTimeStamp(LogEntry& message) {

  map <string,string> & mymetadata = message.metadata;
  mymetadata.erase(METADATA_TIMESTAMP);
  if (mymetadata.empty()) {
    message.__isset.metadata = false;
  }
}


