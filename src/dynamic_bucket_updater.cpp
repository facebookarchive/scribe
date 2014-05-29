#include <sstream>
#include <iostream>
#include "dynamic_bucket_updater.h"
#include "scribe_server.h"

using namespace std;
using namespace apache::thrift::concurrency;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace facebook;
using namespace facebook::fb303;
using namespace scribe::thrift;
using boost::shared_ptr;
extern shared_ptr<scribeHandler> g_Handler;

DynamicBucketUpdater* DynamicBucketUpdater::instance_ = NULL;
Mutex DynamicBucketUpdater::instanceLock_;

// bucket updater connection error
const char* DynamicBucketUpdater::FB303_ERR_CONNECT = "bucketupdater.err.update_connect";
// error calling bucketupdater.thrift
const char* DynamicBucketUpdater::FB303_ERR_THRIFTCALL = "bucketupdater.err.thrift_call";
// bucket updater return empty result
const char* DynamicBucketUpdater::FB303_ERR_EMPTYRESULT = "bucketupdater.err.empty_result";
// number of times a remote updater has been called
const char* DynamicBucketUpdater::FB303_REMOTEUPDATE = "bucketupdater.remove_updater";
// missing a bid mapping
const char* DynamicBucketUpdater::FB303_ERR_NOMAPPING = "bucketupdater.err.nobidmapping";
// number of buckets that have been updated
const char* DynamicBucketUpdater::FB303_BUCKETSUPDATED = "bucketupdater.bucket_updated";

bool DynamicBucketUpdater::getHost(const string& category,
                                   const StoreConf* pconf,
                                   string& host,
                                   uint32_t& port) {
  bool success = false;
  // getHost to check what the current category/bid to host:port
  // mapping is
  string service, serviceOptions, updaterHost, updaterPort;
  long int timeout = 1000;  // 1 second default timeout
  long int ttl = 60;        // update every minute
  long int bid;
  pconf->getString("bucket_updater_service_options", serviceOptions);
  pconf->getInt("bucket_updater_ttl", ttl);
  pconf->getInt("bucket_id", bid);
  pconf->getString("bucket_updater_service", service);
  pconf->getString("bucket_updater_host", updaterHost);
  pconf->getString("bucket_updater_port", updaterPort);
  pconf->getInt("timeout", timeout);

  if (!service.empty()) {
    success = DynamicBucketUpdater::getHost(g_Handler.get(),
                                            category,
                                            ttl,
                                            (uint32_t)bid,
                                            host,
                                            port,
                                            service,
                                            serviceOptions,
                                            timeout, timeout, timeout);
  } else {
    success = DynamicBucketUpdater::getHost(g_Handler.get(),
                                            category,
                                            ttl,
                                            (uint32_t)bid,
                                            host,
                                            port,
                                            updaterHost,
                                            atoi(updaterPort.c_str()),
                                            timeout, timeout, timeout);
  }

  if (!success) {
    LOG_OPER("[%s] dynamic bucket updater failed: bid=%ld",
             category.c_str(), bid);
  }
  return success;
}

bool DynamicBucketUpdater::isConfigValid(const string& category,
                                         const StoreConf* pconf) {
  // we need dynamic_updater_bid paramter
  string bid;
  if (!pconf->getString("bucket_id", bid)) {
    LOG_OPER("[%s] dynamic bucket updater configuration invalid. Missing bucket_id.  Is the network a descendant of a bucket store?", category.c_str());
    return false;
  }

  // requires either dynamic_updater_host and networ_updater_port, or
  // dynamic_updater_service
  string host, port, service;
  if (!pconf->getString("bucket_updater_service", service) &&
    (!pconf->getString("bucket_updater_host", host) ||
     !pconf->getString("bucket_updater_port", port))) {
    LOG_OPER("[%s] dynamic bucket updater configuration invalid. Either bucket_updater_service or bucket_updater_host and bucket_updater_port is needed.  Current values are: bucket_updater_service=<%s>, bucket_updater_host=<%s>, bucket_updater_port=<%s>",
        category.c_str(), service.c_str(), host.c_str(), port.c_str());
    return false;
  }
  return true;
}

/**
  * Return host, port given a key and bucket id, bid, combination.
  * If a mapping is found, the result will be returned in out parameter and
  * function returns true.  Otherwise, function returns false and output
  * parameters are not modified.
  *
  * @param fbBase ponter to FacebookBase
  * @param category the category name, or any identifier that uniquely
  *        identifies a bucket store.
  * @param ttl ttl in seconds
  * @param bid bucket id
  * @param host the output parameter that receives the host output.
  *        If no mapping is found, this variable is not modified.
  * @param port the output parameter that receives the host output.
  *        If no mapping is found, this variable is not modified.
  * @param service service name
  * @param connTimeout connection timeout
  * @param sendTimeout send timeout
  * @param recvTimeout receive timeout
  */
bool DynamicBucketUpdater::getHost(facebook::fb303::FacebookBase *fbBase,
                      const string &category,
                      uint32_t ttl,
                      uint64_t bid,
                      string &host,
                      uint32_t &port,
                      string updateHost,
                      uint32_t updatePort,
                      uint32_t connTimeout,
                      uint32_t sendTimeout,
                      uint32_t recvTimeout) {
  DynamicBucketUpdater *instance = DynamicBucketUpdater::getInstance(fbBase);

  instance->lock_.lock();

  bool ret = instance->getHostInternal(category, ttl, bid,
                                       host, port, updateHost,
                                       updatePort, connTimeout,
                                       sendTimeout, recvTimeout);
  instance->lock_.unlock();
  return ret;
}

/**
  * Return host, port given a key and bucket id, bid, combination.
  * If a mapping is found, the result will be returned in out parameter and
  * function returns true.  Otherwise, function returns false and output
  * parameters are not modified.
  *
  * @param fbBase ponter to FacebookBase
  * @param category the category name, or any identifier that uniquely
  *        identifies a bucket store.
  * @param ttl ttl in seconds
  * @param bid bucket id
  * @param host the output parameter that receives the host output.
  *        If no mapping is found, this variable is not modified.
  * @param port the output parameter that receives the host output.
  *        If no mapping is found, this variable is not modified.
  */
bool DynamicBucketUpdater::getHost(facebook::fb303::FacebookBase *fbBase,
                      const string &category,
                      uint32_t ttl,
                      uint64_t bid,
                      string &host,
                      uint32_t &port,
                      string serviceName,
                      string serviceOptions,
                      uint32_t connTimeout,
                      uint32_t sendTimeout,
                      uint32_t recvTimeout) {
    server_vector_t servers;
    bool success = scribe::network_config::getService(serviceName,
                                                      serviceOptions,
                                                      servers);

    // Cannot open if we couldn't find any servers
    if (!success || servers.empty()) {
      LOG_OPER("[%s] Failed to get servers from Service [%s] "
               "for dynamic bucket updater",
               category.c_str(), serviceName.c_str());

      return false;
    }

    // randomly pick one from the service
    int which = rand() % servers.size();
    string updateHost = servers[which].first;
    uint32_t updatePort = servers[which].second;
    return DynamicBucketUpdater::getHost(fbBase, category, ttl, bid,
                                         host, port,
                                         updateHost, updatePort,
                                         connTimeout, sendTimeout,
                                         recvTimeout);
}

/**
  * actual implementation of getHost.
  *
  * @param category the category name, or any identifier that uniquely
  *        identifies a bucket store.
  * @param ttl ttl in seconds
  * @param bid bucket id
  * @param host the output parameter that receives the host output.
  *        If no mapping is found, this variable is not modified.
  * @param port the output parameter that receives the host output.
  *        If no mapping is found, this variable is not modified.
  */
bool DynamicBucketUpdater::getHostInternal(const string &category,
                                           uint32_t ttl,
                                           uint64_t bid,
                                           string &host,
                                           uint32_t &port,
                                           string updateHost,
                                           uint32_t updatePort,
                                           uint32_t connTimeout,
                                           uint32_t sendTimeout,
                                           uint32_t recvTimeout) {
  time_t now = time(NULL);

  // periodic check whether we need to fetch the mapping, or need to
  // update
  CatBidToHostMap::const_iterator iter = catMap_.find(category);
  if (iter == catMap_.end() || iter->second.lastUpdated_ + ttl < now) {
    periodicCheck(category,
                  ttl,
                  updateHost,
                  updatePort,
                  connTimeout, sendTimeout, recvTimeout);
    iter = catMap_.find(category);
    // check again.
    if (iter == catMap_.end()) {
      return false;
    }
  }

  const CategoryEntry &catEnt = iter->second;
  map<uint64_t, HostEntry>::const_iterator bidIter = catEnt.bidMap_.find(bid);

  bool ret = false;
  if (bidIter != catEnt.bidMap_.end()) {
    const HostEntry &entry = bidIter->second;
    host = entry.host_;
    port = entry.port_;
    ret = true;
  } else {
    ostringstream oss;
    oss << "Missing mapping for category " << category << ", bid: " << bid
        << ", updateHost: " << updateHost << ", updatePort: " << updatePort;
    LOG_OPER(oss.str());
    addStatValue(DynamicBucketUpdater::FB303_ERR_NOMAPPING, 1);
  }

  return ret;
}

/**
  * Given a category name, remote host:port, current time, and category
  * mapping time to live (ttl), check whether we need to update the
  * category mapping.  If so query bucket mapping
  * using bucketupdater thrift interface and update internal category,
  * bucket id to host mappings.
  *
  * This function takes care of try/catch and locking.  The bulk of the
  * update logic is delegated to updateInternal.
  *
  * @param category category or key that uniquely identifies this updater.
  * @param ttl ttl in seconds
  * @param host remote host that will be used to retrieve bucket mapping
  * @param port remote port that will be used to retrieve bucket mapping
  * @param connTimeout connection timeout
  * @param sendTimeout send time out
  * @param recvTimeout receive time out
  *
  * @return true if successful. false otherwise.
  */
bool DynamicBucketUpdater::periodicCheck(string category,
                                         uint32_t ttl,
                                         string host,
                                         uint32_t port,
                                         uint32_t connTimeout,
                                         uint32_t sendTimeout,
                                         uint32_t recvTimeout) {
  bool ret = false;
  try {
    ret = updateInternal(category,
                         ttl,
                         host,
                         port,
                         connTimeout,
                         sendTimeout,
                         recvTimeout);
  } catch (const TTransportException& ttx) {
    LOG_OPER("periodicCheck(%s, %s, %u, %d, %d, %d) TTransportException: %s",
            category.c_str(), host.c_str(), port,
            connTimeout, sendTimeout, recvTimeout,
            ttx.what());
    addStatValue(DynamicBucketUpdater::FB303_ERR_THRIFTCALL, 1);
    ret = false;
  } catch (const BucketStoreMappingException& bex) {
    ostringstream oss;
    LOG_OPER("periodicCheck(%s, %s, %u, %d, %d, %d) TTransportException: %s",
            category.c_str(), host.c_str(), port,
            connTimeout, sendTimeout, recvTimeout,
            bex.message.c_str());
    addStatValue(DynamicBucketUpdater::FB303_ERR_THRIFTCALL, 1);
    ret = false;
  }

  return ret;
}

/**
  * Given a category name, remote host and port, query bucket mapping
  * using bucketupdater thrift interface and update internal category,
  * bucket id to host mappings.
  *
  * @param category category or other uniquely identifiable key
  * @param ttl ttl in seconds
  * @param remoteHost remote host that will be used to retrieve bucket mapping
  * @param remotePort remote port that will be used to retrieve bucket mapping
  * @param connTimeout connection timeout
  * @param sendTimeout send time out
  * @param recvTimeout receive time out
  *
  * @return true if successful. false otherwise.
  */
bool DynamicBucketUpdater::updateInternal(
                           string category,
                           uint32_t ttl,
                           string remoteHost,
                           uint32_t remotePort,
                           uint32_t connTimeout,
                           uint32_t sendTimeout,
                           uint32_t recvTimeout) {
  addStatValue(DynamicBucketUpdater::FB303_REMOTEUPDATE, 1);

  // remove the current mapping
  CatBidToHostMap::iterator catIter = catMap_.find(category);
  if (catIter != catMap_.end()) {
    catMap_.erase(catIter);
  }

  shared_ptr<TSocket> socket = shared_ptr<TSocket>(
                                new TSocket(remoteHost, remotePort));

  if (!socket) {
    addStatValue(DynamicBucketUpdater::FB303_ERR_CONNECT, 1);
    ostringstream oss;
    oss << "Failed to create socket in bucket updater("
        << category
        << ", "
        << remoteHost
        << ", "
        << remotePort
        << ")";
    LOG_OPER(oss.str());
    return false;
  }

  socket->setConnTimeout(connTimeout);
  socket->setRecvTimeout(recvTimeout);
  socket->setSendTimeout(sendTimeout);

  shared_ptr<TFramedTransport> framedTransport = shared_ptr<TFramedTransport>(
                new TFramedTransport(socket));
  framedTransport->open();
  shared_ptr<TBinaryProtocol> protocol = shared_ptr<TBinaryProtocol>(
                                      new TBinaryProtocol(framedTransport));

  // no strict version checking
  protocol->setStrict(false, false);
  BucketStoreMappingClient client(protocol);
  map<int32_t, HostPort> mapping;
  client.getMapping(mapping, category);

  if (mapping.size() == 0) {
    addStatValue(DynamicBucketUpdater::FB303_ERR_EMPTYRESULT, 1);

    return false;
  }

  CategoryEntry catEntry(category, ttl);
  catEntry.lastUpdated_ = time(NULL);
  // update bucket id host mappings
  for (map<int32_t, HostPort>::const_iterator iter = mapping.begin();
      iter != mapping.end(); ++iter) {
    uint32_t bid = iter->first;
    const HostPort &hp = iter->second;
    HostEntry hentry;
    hentry.host_ = hp.host;
    hentry.port_ = hp.port;
    catEntry.bidMap_[bid] = hentry;
  }
  catMap_[category] = catEntry;

  // increment the counter for number of buckets updated
  addStatValue(DynamicBucketUpdater::FB303_BUCKETSUPDATED, mapping.size());

  return true;
}

DynamicBucketUpdater* DynamicBucketUpdater::getInstance(
                                        FacebookBase *fbBase) {
  if (DynamicBucketUpdater::instance_) {
    return DynamicBucketUpdater::instance_;
  }

  Guard g(DynamicBucketUpdater::instanceLock_);
  if (!DynamicBucketUpdater::instance_) {
    DynamicBucketUpdater::instance_ = new DynamicBucketUpdater(fbBase);
  }
  return DynamicBucketUpdater::instance_;
}

/**
  * Setup fb303 counters.
  */
void DynamicBucketUpdater::initFb303Counters() {
#ifdef FACEBOOK
  if (fbBase_) {
    // initialize fb303 time series based counters
    fbBase_->addStatExportType(
        DynamicBucketUpdater::FB303_ERR_CONNECT, stats::SUM);
    fbBase_->addStatExportType(
        DynamicBucketUpdater::FB303_ERR_EMPTYRESULT, stats::SUM);
    fbBase_->addStatExportType(
        DynamicBucketUpdater::FB303_REMOTEUPDATE, stats::SUM);
    fbBase_->addStatExportType(
        DynamicBucketUpdater::FB303_BUCKETSUPDATED, stats::SUM);
    fbBase_->addStatExportType(
        DynamicBucketUpdater::FB303_ERR_NOMAPPING, stats::SUM);
  }
#endif
}
