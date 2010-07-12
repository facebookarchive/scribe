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
// @author Alex Moskalyuk
// @author Avinash Lakshman
// @author Anthony Giardullo
// @author Jan Oravec
// @author John Song

#include "Common.h"
#include "NetworkDynamicConfig.h"
#include "NetworkStore.h"
#include "ScribeServer.h"

using namespace boost;
using namespace scribe::thrift;
using namespace facebook;

static const unsigned long kDefaultNetworkStoreCacheTimeout   = 300;
static const long          kDefaultSocketTimeoutMs            = 5000; // 5 sec

// magic threshold
static const size_t        kDefaultNetworkStoreDummyThreshold = 4096;

namespace scribe {

// Checks if we should try sending a dummy Log in the n/w store
bool shouldSendDummy(LogEntryVectorPtr messages) {
  size_t size = 0;
  for (LogEntryVector::iterator iter = messages->begin();
      iter != messages->end(); ++iter) {
    size += (**iter).message.size();
    if (size > kDefaultNetworkStoreDummyThreshold) {
      return true;
    }
  }
  return false;
}

NetworkStore::NetworkStore(StoreQueue* storeq,
                           const string& category,
                           bool multiCategory)
  : Store(storeq, category, "network", multiCategory),
    useConnPool_(false),
    serviceBased_(false),
    timeout_(kDefaultSocketTimeoutMs),
    remotePort_(0),
    serviceCacheTimeout_(kDefaultNetworkStoreCacheTimeout),
    configMod_(NULL),
    opened_(false),
    lastServiceCheck_(0) {
  // we can't open the connection until we get configured

  // the bool for opened ensures that we don't make duplicate
  // close calls, which would screw up the connection pool's
  // reference counting.
}

NetworkStore::~NetworkStore() {
  close();
}

void NetworkStore::configure(StoreConfPtr configuration, StoreConfPtr parent) {
  Store::configure(configuration, parent);
  // Error checking is done on open()
  // Constructor defaults are fine if these conf settings don't exist

  if (configuration->getString("smc_service", &serviceName_)) {
    // Service takes precedence over host + port
    serviceBased_ = true;

    configuration->getString("service_options", &serviceOptions_);
    configuration->getUnsigned("service_cache_timeout", &serviceCacheTimeout_);
  } else {
    serviceBased_ = false;
    configuration->getString("remote_host", &remoteHost_);
    configuration->getUnsigned("remote_port", &remotePort_);
  }

  configuration->getInt("timeout", &timeout_);

  string temp;
  if (configuration->getString("use_conn_pool", &temp)) {
    if (0 == temp.compare("yes")) {
      useConnPool_ = true;
    }
  }

  // if this network store dynamic configured?
  // get network dynamic updater parameters
  string dynamicType;
  if (configuration->getString("dynamic_config_type", &dynamicType)) {
    // get dynamic config module
    configMod_ = getNetworkDynamicConfigMod(dynamicType.c_str());
    if (configMod_) {
      if (!configMod_->isConfigValidFunc(categoryHandled_,
                                         configuration.get())) {
        LOG_OPER("[%s] dynamic network configuration is not valid.",
                categoryHandled_.c_str());
        configMod_ = NULL;
      } else {
        // set remote host port
        string host;
        uint32_t port;
        if (configMod_->getHostFunc(categoryHandled_, storeConf_.get(),
                                    host, port)) {
          remoteHost_ = host;
          remotePort_ = port;
          LOG_OPER("[%s] dynamic configred network store destination "
                   "configured:<%s:%lu>",
                   categoryHandled_.c_str(), remoteHost_.c_str(), remotePort_);
        }
      }
    } else {
      LOG_OPER("[%s] dynamic network configuration is not valid. Unable to "
               "find network dynamic configuration module with name <%s>",
                categoryHandled_.c_str(), dynamicType.c_str());
    }
  }
}

void NetworkStore::periodicCheck() {
  if (configMod_) {
    // get the network updater type
    string host;
    uint32_t port;
    bool success = configMod_->getHostFunc(categoryHandled_, storeConf_.get(),
                                           host, port);
    if (success && (host != remoteHost_ || port != remotePort_)) {
      // if it is different from the current configuration
      // then close and open again
      LOG_OPER("[%s] dynamic configred network store destination changed. "
               "old value:<%s:%lu>, new value:<%s:%lu>",
               categoryHandled_.c_str(), remoteHost_.c_str(), remotePort_,
               host.c_str(), (unsigned long)port);
      remoteHost_ = host;
      remotePort_ = port;
      close();
    }
  }
}

bool NetworkStore::open() {
  if (isOpen()) {
    /* re-opening an already open NetworkStore can be bad. For example,
     * it can lead to bad reference counting on g_connpool connections
     */
    return (true);
  }
  if (serviceBased_) {
    bool success = true;
    time_t now = time(NULL);

    // Only get list of servers if we haven't already gotten them recently
    if (lastServiceCheck_ <= (time_t) (now - serviceCacheTimeout_)) {
      lastServiceCheck_ = now;

      success = scribe::network_config::getService(serviceName_,
                                                   serviceOptions_,
                                                   &servers_);
    }

    // Cannot open if we couldn't find any servers
    if (!success || servers_.empty()) {
      LOG_OPER("[%s] Failed to get servers from Service",
          categoryHandled_.c_str());

      setStatus("Could not get list of servers from Service");
      return false;
    }

    if (useConnPool_) {
      opened_ = g_connPool.open(serviceName_,
                                servers_,
                                static_cast<int>(timeout_));
    } else {
      if (unpooledConn_ != NULL) {
        LOG_OPER("Logic error: NetworkStore::open unpooledConn_ is not NULL"
            " service = %s", serviceName_.c_str());
      }
      unpooledConn_.reset(new ScribeConn(serviceName_,
                                         servers_,
                                         static_cast<int>(timeout_)));
      opened_ = unpooledConn_->open();
      if (!opened_) {
        unpooledConn_.reset();
      }
    }

  } else if (remotePort_ <= 0 || remoteHost_.empty()) {
    LOG_OPER("[%s] Bad config - won't attempt to connect to <%s:%lu>",
        categoryHandled_.c_str(), remoteHost_.c_str(), remotePort_);
    if (!configMod_) {
      // for dynamic config network store, the host:port will be emtpy
      // from time to time pending updates.  Log it but don't set
      // status
      setStatus("Bad config - invalid location for remote server");
    }

    return false;
  } else {
    if (useConnPool_) {
      opened_ = g_connPool.open(remoteHost_, remotePort_,
          static_cast<int>(timeout_));
    } else {
      // only open unpooled connection if not already open
      if (unpooledConn_ != NULL) {
        LOG_OPER("Logic error: NetworkStore::open unpooledConn_ is not NULL"
            " %s:%lu", remoteHost_.c_str(), remotePort_);
      }
      unpooledConn_.reset(new ScribeConn(remoteHost_,
          remotePort_, static_cast<int>(timeout_)));
      opened_ = unpooledConn_->open();
      if (!opened_) {
        unpooledConn_.reset();
      }
    }
  }

  if (opened_) {
    // clear status on success or if we should not signal error here
    setStatus("");
  } else {
    stringstream error;

    if (serviceBased_) {
      error << "Failed to connect to service: " << serviceName_;
    } else {
      error << "Failed to connect to host: "
            << remoteHost_ << ":" << remotePort_;
    }

    setStatus(error.str());
  }
  return opened_;
}

void NetworkStore::close() {
  if (!opened_) {
    return;
  }
  opened_ = false;
  if (useConnPool_) {
    if (serviceBased_) {
      g_connPool.close(serviceName_);
    } else {
      g_connPool.close(remoteHost_, remotePort_);
    }
  } else {
    if (unpooledConn_ != NULL) {
      unpooledConn_->close();
    }
    unpooledConn_.reset();
  }
}

bool NetworkStore::isOpen() {
  return opened_;
}

StorePtr NetworkStore::copy(const string &category) {
  StorePtr copied(new NetworkStore(storeQueue_, category, multiCategory_));

  NetworkStore* store = static_cast<NetworkStore*>(copied.get());
  store->useConnPool_ = useConnPool_;
  store->serviceBased_ = serviceBased_;
  store->timeout_ = timeout_;
  store->remoteHost_ = remoteHost_;
  store->remotePort_ = remotePort_;
  store->serviceName_ = serviceName_;
  store->serviceOptions_ = serviceOptions_;
  store->serviceCacheTimeout_ = serviceCacheTimeout_;

  return copied;
}


// If the size of messages is greater than a threshold
// first try sending an empty vector to catch dfqs
bool
NetworkStore::handleMessages(LogEntryVectorPtr messages) {
  int ret;

  if (!isOpen()) {
    if (!open()) {
    LOG_OPER("[%s] Could not open NetworkStore in handleMessages",
             categoryHandled_.c_str());
    g_handler->stats.addCounter(StatCounters::kNetworkDisconnectErr, 1);

    return false;
    }
  }

  g_handler->stats.addCounter(StatCounters::kNetworkIn, messages->size());

  bool tryDummySend = shouldSendDummy(messages);
  LogEntryVectorPtr dummyMessages(new LogEntryVector);

  string sendHost(remoteHost_);
  unsigned long sendPort = remotePort_;
  unsigned long sendBytes = 0;

  if (useConnPool_) {
    if (serviceBased_) {
      if (!tryDummySend ||
          ((ret = g_connPool.send(serviceName_,
                                  dummyMessages, &sendBytes)) == CONN_OK)) {
        ret = g_connPool.send(serviceName_, messages, &sendBytes, &sendHost,
                              &sendPort);
      }
    } else {
      if (!tryDummySend ||
          (ret = g_connPool.send(remoteHost_, remotePort_, dummyMessages,
                                 &sendBytes)) == CONN_OK) {
        ret = g_connPool.send(remoteHost_, remotePort_, messages, &sendBytes);
      }
    }
  } else if (unpooledConn_) {
    if (!tryDummySend ||
        ((ret = unpooledConn_->send(dummyMessages, &sendBytes)) == CONN_OK)) {
      ret = unpooledConn_->send(messages, &sendBytes);
      sendHost = unpooledConn_->getRemoteHost();
      sendPort = unpooledConn_->getRemotePort();
    }
  } else {
    ret = CONN_FATAL;
    LOG_OPER("[%s] Logic error: NetworkStore::handleMessages unpooledConn_ "
        "is NULL", categoryHandled_.c_str());
  }
  if (ret == CONN_FATAL) {
    close();
    g_handler->stats.addCounter(StatCounters::kNetworkDisconnectErr, 1);
  }

  if (ret == CONN_OK) {
    g_handler->stats.addCounter(StatCounters::kNetworkSent, messages->size());
    incrementSentCounter(sendHost, sendPort, messages->size(), sendBytes);
  }

  return (ret == CONN_OK);
}

void NetworkStore::incrementSentCounter(const string &host,
                                        unsigned long port,
                                        unsigned long numMsg,
                                        unsigned long numBytes) {
  static unordered_set<string> hostMap;
  static Mutex hostMapLock;
  char portBuf[12];
  snprintf(portBuf, sizeof(portBuf), "%d", (int)port);
  string key = categoryHandled_ + ".netsent.num_msg." + host + ":" + portBuf;
  string byteKey = categoryHandled_ + ".netsent.num_bytes." + host + ":"
                 + portBuf;
  {
    Guard g(hostMapLock);

    if (hostMap.find(host) == hostMap.end()) {
      hostMap.insert(host);
      g_handler->addStatExportType(key, stats::COUNT);
      g_handler->addStatExportType(key, stats::RATE);
      g_handler->addStatExportType(key, stats::SUM);
      g_handler->addStatExportType(byteKey, stats::RATE);
      g_handler->addStatExportType(byteKey, stats::SUM);
    }
  }
  g_handler->addStatValue(key, numMsg);
  g_handler->addStatValue(byteKey, numBytes);
}

void NetworkStore::flush() {
  // Nothing to do
}

} //! namespace scribe
