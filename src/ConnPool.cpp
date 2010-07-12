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


#include "Common.h"
#include "ConnPool.h"
#include "ScribeServer.h"

using std::ostringstream;

using namespace scribe::thrift;

namespace scribe {

ConnPool g_connPool;

ConnPool::ConnPool() {
}

ConnPool::~ConnPool() {
}

string ConnPool::makeKey(const string& hostname, unsigned long port) {
  string key(hostname);
  key += ":";

  ostringstream oss;
  oss << port;
  key += oss.str();
  return key;
}

bool ConnPool::open(const string& hostname, unsigned long port, int timeout) {
  return openCommon(makeKey(hostname, port),
                    shared_ptr<ScribeConn>(
                      new ScribeConn(hostname, port, timeout)
                    ));
}

bool ConnPool::open(const string &service, const ServerVector &servers,
                    int timeout) {
  return openCommon(service,
                    shared_ptr<ScribeConn>(
                      new ScribeConn(service, servers, timeout)
                    ));
}

void ConnPool::close(const string& hostname, unsigned long port) {
  closeCommon(makeKey(hostname, port));
}

void ConnPool::close(const string &service) {
  closeCommon(service);
}

SendResult ConnPool::send(const string& hostname, unsigned long port,
                   LogEntryVectorPtr messages,
                   unsigned long* bytesSent) {
  return sendCommon(makeKey(hostname, port), messages, bytesSent);
}

SendResult ConnPool::send(const string &service,
                   LogEntryVectorPtr messages,
                   unsigned long* bytesSent,
                   string* sendHost,
                   unsigned long* sendPort) {
  return sendCommon(service, messages, bytesSent, sendHost, sendPort);
}

bool ConnPool::openCommon(const string &key, shared_ptr<ScribeConn> conn) {
  // note on locking:
  // The mapMutex_ locks all reads and writes to the connMap_.
  // The locks on each connection serialize writes and deletion.
  // The lock on a connection doesn't protect its refcount, as refcounts
  // are only accessed under the mapMutex_.
  // mapMutex_ MUST be held before attempting to lock particular connection

  Guard g(mapMutex_);

  ConnectionMap::iterator iter = connMap_.find(key);
  if (iter != connMap_.end()) {
    shared_ptr<ScribeConn> oldConn = (*iter).second;
    if (oldConn->isOpen()) {
      oldConn->addRef();
      return true;
    }
    if (conn->open()) {
      LOG_OPER("CONN_POOL: switching to a new connection <%s>", key.c_str());
      conn->setRef(oldConn->getRef());
      conn->addRef();
      // old connection will be magically deleted by shared_ptr
      connMap_[key] = conn;
      return true;
    }
    return false;
  }

  // don't need to lock the conn yet, because no one know about
  // it until we release the mapMutex_
  if (conn->open()) {
    // ref count starts at one, so don't addRef here
    connMap_[key] = conn;
    return true;
  }

  // conn object that failed to open is deleted
  return false;
}

void ConnPool::closeCommon(const string &key) {
  Guard g(mapMutex_);

  ConnectionMap::iterator iter = connMap_.find(key);
  if (iter != connMap_.end()) {
    ScribeConnPtr& conn = iter->second;

    conn->releaseRef();
    if (conn->getRef() <= 0) {
      conn->lock();
      conn->close();
      conn->unlock();
      connMap_.erase(iter);
    }
  } else {
    // This can be bad. If one client double closes then other clients
    // are screwed
    LOG_OPER("LOGIC ERROR: attempting to close connection <%s> that connPool "
             "has no entry for", key.c_str());
  }
}

SendResult ConnPool::sendCommon(const string &key,
                         LogEntryVectorPtr messages,
                         unsigned long* bytesSent,
                         string* sendHost,
                         unsigned long* sendPort) {
  mapMutex_.lock();
  ConnectionMap::iterator iter = connMap_.find(key);
  if (iter != connMap_.end()) {
    iter->second->lock();
    mapMutex_.unlock();
    SendResult result = iter->second->send(messages, bytesSent);
    if (sendHost) {
      *sendHost = iter->second->getRemoteHost();
    }
    if (sendPort) {
      *sendPort = iter->second->getRemotePort();
    }
    iter->second->unlock();
    return result;
  } else {
    mapMutex_.unlock();
    LOG_OPER("send failed. No connection pool entry for <%s>", key.c_str());
    return (CONN_FATAL);
  }
}

ScribeConn::ScribeConn(const string& hostname, unsigned long port, int timeout)
    : refCount_(1),
      serviceBased_(false),
      remoteHost_(hostname),
      remotePort_(port),
      timeout_(timeout) {
}

ScribeConn::ScribeConn(const string& service, const ServerVector& servers,
                       int timeout)
    : refCount_(1),
      serviceBased_(true),
      serviceName_(service),
      serverList_(servers),
      timeout_(timeout) {
}

ScribeConn::~ScribeConn() {
}

void ScribeConn::addRef() {
  ++ refCount_;
}

void ScribeConn::releaseRef() {
  -- refCount_;
}

unsigned ScribeConn::getRef() {
  return refCount_;
}

void ScribeConn::setRef(unsigned r) {
  refCount_ = r;
}

void ScribeConn::lock() {
  mutex_.lock();
}

void ScribeConn::unlock() {
  mutex_.unlock();
}

bool ScribeConn::isOpen() {
  return framedTransport_->isOpen();
}

bool ScribeConn::open() {
  try {

    socket_.reset(serviceBased_ ?
                  new TSocketPool(serverList_) :
                  new TSocket(remoteHost_, remotePort_)
    );

    if (!socket_) {
      throw std::runtime_error("Failed to create socket");
    }

    socket_->setConnTimeout(timeout_);
    socket_->setRecvTimeout(timeout_);
    socket_->setSendTimeout(timeout_);
    /*
     * We don't want to send resets to close the connection. Among
     * other badness it also reduces data reliability. On getting a
     * rest, the receiving socket will throw any data the receving
     * process has not yet read.
     *
     * echo 5 > /proc/sys/net/ipv4/tcp_fin_timeout to set the TIME_WAIT
     * timeout on a system.
     * sysctl -a | grep tcp
     */
    socket_->setLinger(0, 0);

    framedTransport_.reset(new TFramedTransport(socket_));
    if (!framedTransport_) {
      throw std::runtime_error("Failed to create framed transport");
    }
    protocol_.reset(new TBinaryProtocol(framedTransport_));
    if (!protocol_) {
      throw std::runtime_error("Failed to create protocol");
    }
    protocol_->setStrict(false, false);
    resendClient_.reset(new scribeClient(protocol_));
    if (!resendClient_) {
      throw std::runtime_error("Failed to create network client");
    }

    framedTransport_->open();
    if (serviceBased_) {
      remoteHost_ = socket_->getPeerHost();
    }
  } catch (const TTransportException& ttx) {
    LOG_OPER("failed to open connection to remote scribe server %s "
             "thrift error <%s>", connectionString().c_str(), ttx.what());
    return false;
  } catch (const std::exception& stx) {
    LOG_OPER("failed to open connection to remote scribe server %s "
             "std error <%s>", connectionString().c_str(), stx.what());
    return false;
  }
  LOG_OPER("Opened connection to remote scribe server %s",
           connectionString().c_str());
  return true;
}

void ScribeConn::close() {
  try {
    framedTransport_->close();
  } catch (const TTransportException& ttx) {
    LOG_OPER("error <%s> while closing connection to remote scribe server %s",
             ttx.what(), connectionString().c_str());
  }
}

SendResult
ScribeConn::send(LogEntryVectorPtr messages, unsigned long* bytesSent) {
  bool fatal;
  int size = messages->size();
  if (!isOpen()) {
    if (!open()) {
      return (CONN_FATAL);
    }
  }

  unsigned long msgSize = 0;
  // Copy the vector of pointers to a vector of objects
  // This is because thrift doesn't support vectors of pointers,
  // but we need to use them internally to avoid even more copies.
  vector<LogEntry> msgs;
  msgs.reserve(size);
  for (LogEntryVector::iterator iter = messages->begin();
       iter != messages->end();
       ++iter) {
    // This is just a count of message size.  Thrift has overhead.
    // However, as far as end to end app is concerned, most of the
    // time, only message body itself counts
    msgSize += (*iter)->message.size();
    msgs.push_back(**iter);
  }
  ResultCode result = TRY_LATER;
  try {
    result = resendClient_->Log(msgs);

    if (result == OK) {
      g_handler->incCounter("sent", size);
      LOG_OPER("Successfully sent <%d> messages to remote scribe server %s",
          size, connectionString().c_str());
      *bytesSent = msgSize;
      return (CONN_OK);
    }
    fatal = false;
    LOG_OPER("Failed to send <%d> messages, remote scribe server %s "
        "returned error code <%d>", size, connectionString().c_str(),
        (int) result);
  } catch (const TTransportException& ttx) {
    fatal = true;
    LOG_OPER("Failed to send <%d> messages to remote scribe server %s "
        "error <%s>", size, connectionString().c_str(), ttx.what());
  } catch (...) {
    fatal = true;
    LOG_OPER("Unknown exception sending <%d> messages to remote scribe "
        "server %s", size, connectionString().c_str());
  }
  /*
   * If this is a serviceBased connection then close it. We might
   * be lucky and get another service when we reopen this connection.
   * If the IP:port of the remote is fixed then no point closing this
   * connection ... we are going to get the same connection back.
   */
  if (serviceBased_ || fatal) {
    close();
    return (CONN_FATAL);
  }
  return (CONN_TRANSIENT);
}

string ScribeConn::connectionString() {
  if (serviceBased_) {
    return "<" + remoteHost_ + " Service: " + serviceName_ + ">";
  } else {
    char port[10];
    snprintf(port, 10, "%lu", remotePort_);
    return "<" + remoteHost_ + ":" + string(port) + ">";
  }
}

} //! namespace scribe
