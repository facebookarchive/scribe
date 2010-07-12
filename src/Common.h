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
// @author Jason Sobel

#ifndef SCRIBE_COMMON_H
#define SCRIBE_COMMON_H

#include <sstream>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <string>
#include <queue>
#include <vector>
#include <map>
#include <set>
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#include <stdexcept>
#include <limits>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <boost/lexical_cast.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/convenience.hpp>
#include <boost/algorithm/string.hpp>

#include "thrift/protocol/TBinaryProtocol.h"
#include "thrift/server/TNonblockingServer.h"
#include "thrift/concurrency/ThreadManager.h"
#include "thrift/concurrency/PosixThreadFactory.h"
#include "thrift/concurrency/Mutex.h"
#include "thrift/concurrency/Monitor.h"
#include "thrift/transport/TSocket.h"
#include "thrift/transport/TSocketPool.h"
#include "thrift/transport/TServerSocket.h"
#include "thrift/transport/TTransportUtils.h"
#include "thrift/transport/THttpClient.h"
#include "thrift/transport/TFileTransport.h"
#include "fb303/cpp/FacebookBase.h"

#include "gen-cpp/scribe.h"

namespace scribe {

// shortcuts to commonly used data structures
using boost::shared_ptr;

using std::map;
using std::pair;
using std::queue;
using std::string;
using std::vector;
using std::tr1::unordered_map;
using std::tr1::unordered_set;

// brings scribe::thrift:: stuffs into scribe::
using thrift::LogEntry;
using thrift::ResultCode;

// mutexes
using apache::thrift::concurrency::Monitor;
using apache::thrift::concurrency::Mutex;
using apache::thrift::concurrency::ReadWriteMutex;

// shortcuts to thrift namespaces
namespace thrift {
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::concurrency;
} //! namespace scribe::thrift

// shortcuts to fb303
namespace fb303 = facebook::fb303;

// short forms of certain data types
typedef shared_ptr<LogEntry>         LogEntryPtr;
typedef vector<LogEntryPtr>          LogEntryVector;
typedef shared_ptr<LogEntryVector>   LogEntryVectorPtr;
typedef vector< pair<string, int> >  ServerVector;

} //! namespace scribe

// For security reasons we can't release everything that's compiled
// in at facebook. Other users might find this useful as well for
// integrating to their environment.
// Things in this file include network based configuration and debug messages
#ifdef FACEBOOK
#include "EnvFacebook.h"
#else
#include "EnvDefault.h"
#endif

#endif //! SCRIBE_COMMON_H
