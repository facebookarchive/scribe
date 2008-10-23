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

#ifndef SCRIBE_CONN_POOL_H
#define SCRIBE_CONN_POOL_H

#include "common.h"

class scribeConn {
 public:
  scribeConn(const std::string& host, unsigned long port, int timeout);
  scribeConn(const std::string &service, const server_vector_t &servers, int timeout);
  virtual ~scribeConn();
  
  void addRef();
  void releaseRef();
  unsigned getRef();

  void lock();
  void unlock();

  bool open();
  void close();    
  bool send(boost::shared_ptr<logentry_vector_t> messages);

 private:
  std::string connectionString();
  
 protected:
  boost::shared_ptr<facebook::thrift::transport::TSocket> socket;
  boost::shared_ptr<facebook::thrift::transport::TFramedTransport> framedTransport;
  boost::shared_ptr<facebook::thrift::protocol::TBinaryProtocol> protocol;
  boost::shared_ptr<scribe::thrift::scribeClient> resendClient;

  unsigned refCount;
    
  bool smcBased;
  std::string smcService;
  server_vector_t serverList;
  std::string remoteHost;
  unsigned long remotePort;
  int timeout; // connection, send, and recv timeout
  pthread_mutex_t mutex;
};

// key is hostname:port
typedef std::map<std::string, boost::shared_ptr<scribeConn> > conn_map_t;

class ConnPool {
 public:
  ConnPool();
  virtual ~ConnPool();

  bool open(const std::string& host, unsigned long port, int timeout);
  bool open(const std::string &service, const server_vector_t &servers, int timeout);

  void close(const std::string& host, unsigned long port);
  void close(const std::string &service);

  bool send(const std::string& host, unsigned long port, 
            boost::shared_ptr<logentry_vector_t> messages);
  bool send(const std::string &service,
            boost::shared_ptr<logentry_vector_t> messages);
  
 private:
  bool openCommon(const std::string &key, boost::shared_ptr<scribeConn> conn);
  void closeCommon(const std::string &key);
  bool sendCommon(const std::string &key,
                  boost::shared_ptr<logentry_vector_t> messages);

 protected:
  std::string makeKey(const std::string& name, unsigned long port);

  pthread_mutex_t mapMutex;
  conn_map_t connMap;
};

#endif // !defined SCRIBE_CONN_POOL_H
