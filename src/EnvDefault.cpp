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

#include "Common.h"
#include "ScribeServer.h"

using namespace scribe::thrift;
using namespace scribe::concurrency;

namespace scribe {

/*
 * Network configuration and directory services
 */

bool network_config::getService(const string& serviceName,
                                        const string& options,
                                        ServerVector* servers) {
  return false;
}

/*
 * Concurrency mechanisms
 */

shared_ptr<ReadWriteMutex> concurrency::createReadWriteMutex() {
  return shared_ptr<ReadWriteMutex>(new ReadWriteMutex());
}

/*
 * Time functions
 */

unsigned long clock::nowInMsec() {
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

size_t integerhash::operator()(const uint32_t x) const {
  return (size_t)hash32(x);
}

uint32_t integerhash::hash32(uint32_t key) {
  return key;
}

size_t strhash::operator()(const string &x) const {
  return (size_t)hash32(x.c_str());
}

size_t strhash::operator()(const char *x) const {
  return (size_t)hash32(x);
}

uint32_t strhash::hash32(const char *s) {
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

/*
 * Starting a scribe server.
 */
// note: this function uses global g_handler.
void startServer() {
  shared_ptr<TProcessor> processor(new scribeProcessor(g_handler));
  /* This factory is for binary compatibility. */
  shared_ptr<TProtocolFactory> protocolFactory(
    new TBinaryProtocolFactory(0, 0, false, false)
  );
  shared_ptr<ThreadManager> threadManager;

  if (g_handler->numThriftServerThreads > 1) {
    // create a ThreadManager to process incoming calls
    threadManager = ThreadManager::newSimpleThreadManager(
      g_handler->numThriftServerThreads,
      g_handler->getMaxConcurrentRequests()
    );

    shared_ptr<PosixThreadFactory> threadFactory(new PosixThreadFactory());
    threadManager->threadFactory(threadFactory);
    threadManager->start();
  }

  shared_ptr<TNonblockingServer> server(new TNonblockingServer(
                                          processor,
                                          protocolFactory,
                                          g_handler->port,
                                          threadManager
                                        ));
  g_handler->setServer(server);

  LOG_OPER("Starting scribe server on port %lu", g_handler->port);
  fflush(stderr);

  // throttle concurrent connections
  unsigned long mconn = g_handler->getMaxConn();
  if (mconn > 0) {
    LOG_OPER("Throttle max_conn to %lu", mconn);
    server->setMaxConnections(mconn);
    server->setOverloadAction(T_OVERLOAD_CLOSE_ON_ACCEPT);
  }

  server->serve();
  // this function never returns until server->stop() is called
}

/*
 * Stopping a scribe server.
 */
void stopServer(shared_ptr<TNonblockingServer>& server) {
  server->stop(); // actually does nothing
  // this is the only way to stop a TNonblockingServer for now
  exit(0);
}

/**
 * Starting a background thread to check system memory.  When memory exceeds
 * limit, crash server.
 */
void startMemCheckerThread(unsigned long cycle,
                           float rssRatio,
                           float swapRatio) {
}

} //! namespace scribe
