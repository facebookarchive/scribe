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

#include "Common.h"

namespace scribe {

/* return codes for ScribeConn and ConnPool */
enum SendResult {
  CONN_FATAL       = -1, // fatal error. close everything
  CONN_OK          = 0,  // success
  CONN_TRANSIENT   = 1   // transient error
};

// Basic scribe class to manage network connections. Used by network store
class ScribeConn {
 public:
  ScribeConn(const string& host, unsigned long port, int timeout);
  ScribeConn(const string& service, const ServerVector& servers,
             int timeout);

  virtual ~ScribeConn();

  void addRef();
  void releaseRef();
  unsigned getRef();
  void setRef(unsigned);

  bool isOpen();
  bool open();
  void close();
  SendResult send(LogEntryVectorPtr messages, unsigned long* bytesSend);

  inline const string& getRemoteHost() const {
    return remoteHost_;
  }
  inline unsigned long getRemotePort() const {
    return remotePort_;
  }
  Mutex mutex_;

 private:
  string connectionString();

 protected:
  shared_ptr<thrift::TSocket>          socket_;
  shared_ptr<thrift::TFramedTransport> framedTransport_;
  shared_ptr<thrift::TBinaryProtocol>  protocol_;
  shared_ptr<thrift::scribeClient>     resendClient_;

  unsigned refCount_;

  bool serviceBased_;
  string serviceName_;
  ServerVector serverList_;
  string remoteHost_;
  unsigned long remotePort_;
  int timeout_; // connection, send, and recv timeout
};

typedef shared_ptr<ScribeConn>     ScribeConnPtr;
// key is hostname:port or the service
typedef map<string, ScribeConnPtr> ConnectionMap;

// Scribe class to manage connection pooling
// Maintains a map of (<host,port> or service) to ScribeConn class.
// used to ensure that there is only one connection from one particular
// scribe server to any host,port or service.
// see the global g_connPool in ConnPool.cpp
class ConnPool : private boost::noncopyable {
 public:
  ConnPool();
  virtual ~ConnPool();

  bool open(const string& host, unsigned long port, int timeout);
  bool open(const string &service, const ServerVector &servers, int timeout);

  void close(const string& host, unsigned long port);
  void close(const string &service);

  SendResult send(const string& host,
                  unsigned long port,
                  LogEntryVectorPtr messages,
                  unsigned long* bytesSend);
  SendResult send(const string &service,
                  LogEntryVectorPtr messages,
                  unsigned long* bytesSend,
                  string* sendHost = NULL,
                  unsigned long* sendPort = NULL);

 private:
  bool openCommon(const string &key, shared_ptr<ScribeConn> conn);
  void closeCommon(const string &key);
  SendResult sendCommon(const string &key,
                        LogEntryVectorPtr messages,
                        unsigned long* bytesSend,
                        string* sendHost = NULL,
                        unsigned long* sendPort = NULL);

 protected:
  string makeKey(const string& name, unsigned long port);

// Mutex to protect ConnectionMap access
  Mutex         mapMutex_;
  ConnectionMap connMap_;
};

extern ConnPool g_connPool;

} //! namespace scribe

#endif //! SCRIBE_CONN_POOL_H
