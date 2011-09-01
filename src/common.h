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
#include <pthread.h>
#include <semaphore.h>
#include <map>
#include <set>
#include <stdexcept>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <signal.h>
#include <boost/shared_ptr.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/convenience.hpp>

#include "thrift/protocol/TBinaryProtocol.h"
#include "thrift/server/TNonblockingServer.h"
#include "thrift/server/TThreadedServer.h"
#include "thrift/concurrency/ThreadManager.h"
#include "thrift/concurrency/PosixThreadFactory.h"
#include "thrift/concurrency/Mutex.h"
#include "thrift/transport/TSocket.h"
#include "thrift/transport/TSSLSocket.h"
#include "thrift/transport/TSSLServerSocket.h"
#include "thrift/transport/TSocketPool.h"
#include "thrift/transport/TServerSocket.h"
#include "thrift/transport/TTransportUtils.h"
#include "thrift/transport/THttpClient.h"
#include "thrift/transport/TFileTransport.h"
#include "fb303/FacebookBase.h"

#include "src/gen-cpp/scribe.h"

typedef boost::shared_ptr<scribe::thrift::LogEntry> logentry_ptr_t;
typedef std::vector<logentry_ptr_t> logentry_vector_t;
typedef std::vector<std::pair<std::string, int> > server_vector_t;

// For security reasons we can't release everything that's compiled
// in at facebook. Other users might find this useful as well for
// integrating to their environment.
// Things in this file include network based configuration and debug messages
#ifdef FACEBOOK
#include "env_facebook.h"
#else
#include "env_default.h"
#endif

#endif // !defined SCRIBE_COMMON_H
