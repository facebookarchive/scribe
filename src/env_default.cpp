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

#include "common.h"
#include "scribe_server.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::concurrency;

using namespace scribe::thrift;
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

/*
 * Starting a scribe server.
 */
// note: this function uses global g_Handler.
void scribe::startServer() {
  boost::shared_ptr<TProcessor> processor(new scribeProcessor(g_Handler));
  /* This factory is for binary compatibility. */
  boost::shared_ptr<TProtocolFactory> protocol_factory(
    new TBinaryProtocolFactory(0, 0, false, false)
  );
  boost::shared_ptr<ThreadManager> thread_manager;

  if (g_Handler->numThriftServerThreads > 1) {
    // create a ThreadManager to process incoming calls
    thread_manager = ThreadManager::newSimpleThreadManager(
      g_Handler->numThriftServerThreads
    );

    shared_ptr<PosixThreadFactory> thread_factory(new PosixThreadFactory());
    thread_manager->threadFactory(thread_factory);
    thread_manager->start();
  }

  boost::shared_ptr<TServer> server;

  std::stringstream s;
  s << "Starting scribe server on ";

  if (g_Handler->sslOptions->sslIsEnabled()) {
    boost::shared_ptr<TSSLSocketFactory> sslFactory(g_Handler->sslOptions->createFactory());
    shared_ptr<TServerSocket> socket(new TSSLServerSocket(g_Handler->port, sslFactory));
    shared_ptr<TTransportFactory> framedTransportFactory(new TFramedTransportFactory());
    server.reset(
      new TThreadedServer(processor, socket, framedTransportFactory, protocol_factory)
    );
    s << "port " << g_Handler->port << " using SSL sockets";
  } else {
    TNonblockingServer* pserver;
    if (!g_Handler->path.empty()) {
      pserver =
        new TNonblockingServer(processor, protocol_factory, g_Handler->path, thread_manager);
      s << "unix domain socket '" << g_Handler->path;
    } else {
      pserver =
        new TNonblockingServer(processor, protocol_factory, g_Handler->port, thread_manager);
      s << "port " << g_Handler->port << " using INET sockets";
    }

    server.reset(pserver);

    // throttle concurrent connections
    unsigned long mconn = g_Handler->getMaxConn();
    if (mconn > 0) {
      LOG_OPER("Throttle max_conn to %lu", mconn);
      pserver->setMaxConnections(mconn);
      pserver->setOverloadAction(T_OVERLOAD_CLOSE_ON_ACCEPT);
    }

  }

  LOG_OPER("%s", s.str().c_str());

  fflush(stderr);
  server->serve();
 
  // this function never returns
}


/*
 * Stopping a scribe server.
 */
void scribe::stopServer() {
  exit(0);
}
