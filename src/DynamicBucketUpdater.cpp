#include "DynamicBucketUpdater.h"
#include "ScribeServer.h"
#include "thrift/transport/TBufferTransports.h"

using namespace facebook;
using namespace facebook::fb303;
using namespace scribe::thrift;

using namespace boost;

namespace scribe {

DynamicBucketUpdater* DynamicBucketUpdater::instance_ = NULL;
Mutex DynamicBucketUpdater::instanceLock_;

// bucket updater connection error
const char* const DynamicBucketUpdater::kFb303ErrConnect
    = "bucketupdater.err.update_connect";
// error calling bucketupdater.thrift
const char* const DynamicBucketUpdater::kFb303ErrThriftCall
    = "bucketupdater.err.thrift_call";
// bucket updater return empty result
const char* const DynamicBucketUpdater::kFb303ErrEmptyResult
    = "bucketupdater.err.empty_result";
// number of times a remote updater has been called
const char* const DynamicBucketUpdater::kFb303RemoteUpdate
    = "bucketupdater.remote_updater";
// missing a bid mapping
const char* const DynamicBucketUpdater::kFb303ErrNoMapping
    = "bucketupdater.err.nobidmapping";
// number of buckets that have been updated
const char* const DynamicBucketUpdater::kFb303BucketsUpdated
    = "bucketupdater.bucket_updated";
// number of service calls
const char* const DynamicBucketUpdater::kFb303GetService
    = "bucketupdater.service_get";

bool DynamicBucketUpdater::getHost(const string& category,
                                   const StoreConf* pconf,
                                   string& host,
                                   uint32_t& port) {
  bool success = false;
  // getHost to check what the current category/bid to host:port
  // mapping is
  string service, serviceOptions, updaterHost, updaterPort;
  long timeout = 1000;  // 1 second default timeout
  long ttl = 60;        // update every minute
  long bid;
  pconf->getString("bucket_updater_service_options", &serviceOptions);
  pconf->getInt("bucket_updater_ttl", &ttl);
  pconf->getInt("bucket_id", &bid);
  pconf->getString("bucket_updater_service", &service);
  pconf->getString("bucket_updater_host", &updaterHost);
  pconf->getString("bucket_updater_port", &updaterPort);
  pconf->getInt("timeout", &timeout);

  if (!service.empty()) {
    success = DynamicBucketUpdater::getHostByService(
      g_handler.get(),
      category,
      ttl,
      (uint32_t)bid,
      host, port,
      service, serviceOptions,
      timeout, timeout, timeout);
  } else {
    success = DynamicBucketUpdater::getHostByRemoteHostPort(
      g_handler.get(),
      category,
      ttl,
      (uint32_t)bid,
      host, port,
      updaterHost, lexical_cast<uint32_t>(updaterPort),
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
  if (!pconf->getString("bucket_id", &bid)) {
    LOG_OPER("[%s] dynamic bucket updater configuration invalid. "
             "Missing bucket_id. Is the network a descendant of a bucket store?",
             category.c_str());
    return false;
  }

  // requires either dynamic_updaterHost and networ_updaterPort, or
  // dynamic_updater_service
  string host, port, service;
  if (!pconf->getString("bucket_updater_service", &service) &&
      (!pconf->getString("bucket_updater_host", &host) ||
       !pconf->getString("bucket_updater_port", &port))) {
    LOG_OPER("[%s] dynamic bucket updater configuration invalid. "
             "Either bucket_updater_service or bucket_updater_host and "
             "bucket_updater_port is needed. Current values are: "
             "bucket_updater_service=<%s>, bucket_updater_host=<%s>, "
             "bucket_updater_port=<%s>",
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
bool DynamicBucketUpdater::getHostByRemoteHostPort(
                      fb303::FacebookBase *fbBase,
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

  unsigned long now = scribe::clock::nowInMsec() / 1000;
  bool ret = true;
  bool needCheck = false;
  {
    Guard g(instance->lock_);
    // periodic check whether we need to fetch the mapping, or need to
    // update
    CatBidToHostMap::const_iterator iter = instance->catMap_.find(category);
    if (iter == instance->catMap_.end() ||
        iter->second.lastUpdated + ttl < now) {
      needCheck = true;
    }

    if (needCheck) {
      instance->periodicCheck(category,
          ttl,
          updateHost,
          updatePort,
          connTimeout, sendTimeout, recvTimeout);
      iter = instance->catMap_.find(category);
      // check again.
      if (iter == instance->catMap_.end()) {
        ret = false;
      }
    }

    if (ret) {
      const CategoryEntry &catEntry = iter->second;
      ret = instance->getHostCommon(bid, catEntry, host, port);
    } else {
      LOG_OPER("[%s] Error: Missing mapping for bid %lu, update host %s:%u",
          category.c_str(), bid, updateHost.c_str(),  updatePort);
      instance->addStatValue(DynamicBucketUpdater::kFb303ErrNoMapping, 1);
    }
  }
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
bool DynamicBucketUpdater::getHostByService(
                      fb303::FacebookBase *fbBase,
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
  DynamicBucketUpdater *instance = DynamicBucketUpdater::getInstance(fbBase);

  unsigned long now = scribe::clock::nowInMsec() / 1000;
  bool ret = true;
  bool needCheck = false;
  CatBidToHostMap::const_iterator iter;
  {
    Guard g(instance->lock_);
    // periodic check whether we need to fetch the mapping, or need to
    // update
    iter = instance->catMap_.find(category);
    if (iter == instance->catMap_.end() ||
        iter->second.lastUpdated + ttl < now) {
      needCheck = true;
    }
  }
  if (needCheck) {
    // update through service
    ServerVector servers;
    ret = scribe::network_config::getService(serviceName,
                                             serviceOptions,
                                             &servers);

    instance->addStatValue(DynamicBucketUpdater::kFb303GetService, 1);
    // Cannot open if we couldn't find any servers
    if (!ret || servers.empty()) {
      LOG_OPER("[%s] Failed to get servers from Service [%s] "
               "for dynamic bucket updater",
               category.c_str(), serviceName.c_str());

    } else {
      // randomly pick one from the service
      int which = rand() % servers.size();
      string updateHost = servers[which].first;
      uint32_t updatePort = servers[which].second;
      {
        Guard g(instance->lock_);
        instance->periodicCheck(category,
            ttl,
            updateHost,
            updatePort,
            connTimeout, sendTimeout, recvTimeout);
        // check again.
        iter = instance->catMap_.find(category);
        if (iter == instance->catMap_.end()) {
          ret = false;
        }
      }
    }
  }

  if (ret) {
    const CategoryEntry &catEntry = iter->second;
    {
      Guard g(instance->lock_);
      ret =  instance->getHostCommon(bid, catEntry, host, port);
    }
  } else {
    LOG_OPER("[%s] Error: Missing mapping for bid %lu of service %s",
             category.c_str(), bid, serviceName.c_str());
    instance->addStatValue(DynamicBucketUpdater::kFb303ErrNoMapping, 1);
  }

  return ret;
}


/**
 * Copy out host, port given a CategoryEntry object.
 */
bool DynamicBucketUpdater::getHostCommon(uint64_t bid,
                                         const CategoryEntry& catEntry,
                                         string &host,
                                         uint32_t &port) {
  map<uint64_t, HostEntry>::const_iterator bidIt = catEntry.bidMap.find(bid);

  bool ret = false;
  if (bidIt != catEntry.bidMap.end()) {
    const HostEntry &entry = bidIt->second;
    host = entry.host;
    port = entry.port;
    ret = true;
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
    addStatValue(DynamicBucketUpdater::kFb303ErrThriftCall, 1);
    ret = false;
  } catch (const BucketStoreMappingException& bex) {
    LOG_OPER("periodicCheck(%s, %s, %u, %d, %d, %d) TTransportException: %s",
            category.c_str(), host.c_str(), port,
            connTimeout, sendTimeout, recvTimeout,
            bex.message.c_str());
    addStatValue(DynamicBucketUpdater::kFb303ErrThriftCall, 1);
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
  addStatValue(DynamicBucketUpdater::kFb303RemoteUpdate, 1);

  shared_ptr<TSocket> socket(new TSocket(remoteHost, remotePort));

  if (!socket) {
    addStatValue(DynamicBucketUpdater::kFb303ErrConnect, 1);
    LOG_OPER("[%s] Error: Failed to create socket to %s:%u in bucket updater",
            category.c_str(), remoteHost.c_str(), remotePort);
    return false;
  }

  socket->setConnTimeout(connTimeout);
  socket->setRecvTimeout(recvTimeout);
  socket->setSendTimeout(sendTimeout);

  shared_ptr<TFramedTransport> framedTransport(new TFramedTransport(socket));
  framedTransport->open();

  shared_ptr<TBinaryProtocol> protocol(new TBinaryProtocol(framedTransport));

  // no strict version checking
  protocol->setStrict(false, false);
  BucketStoreMappingClient client(protocol);
  map<int32_t, HostPort> mapping;
  client.getMapping(mapping, category);

  if (mapping.size() == 0) {
    addStatValue(DynamicBucketUpdater::kFb303ErrEmptyResult, 1);
    return false;
  }

  // check whether we alreay have an old mapping or not
  CatBidToHostMap::iterator catIt = catMap_.find(category);
  if (catIt == catMap_.end()) {
    // if missing then create new one
    catIt = catMap_.insert(catMap_.end(),
                           make_pair(category, CategoryEntry(category, ttl)));
  }
  // update ttl
  catIt->second.lastUpdated = scribe::clock::nowInMsec() / 1000;
  // update bucket id host mappings
  for (map<int32_t, HostPort>::const_iterator iter = mapping.begin();
      iter != mapping.end(); ++iter) {
    uint32_t bid = iter->first;
    const HostPort &hp = iter->second;
    HostEntry hostEntry;
    hostEntry.host = hp.host;
    hostEntry.port = hp.port;
    catIt->second.bidMap[bid] = hostEntry;
    LOG_OPER("[%s] Dynamic bucket mapping: %u => %s:%u",
             category.c_str(), bid, hp.host.c_str(), hp.port);
  }

  // increment the counter for number of buckets updated
  addStatValue(DynamicBucketUpdater::kFb303BucketsUpdated, mapping.size());

  return true;
}

DynamicBucketUpdater* DynamicBucketUpdater::getInstance(FacebookBase *fbBase) {
  if (instance_) {
    return instance_;
  }

  Guard g(instanceLock_);

  if (!instance_) {
    instance_ = new DynamicBucketUpdater(fbBase);
  }
  return instance_;
}

/**
  * Setup fb303 counters.
  */
void DynamicBucketUpdater::initFb303Counters() {
  if (fbBase_) {
    // initialize fb303 time series based counters
    fbBase_->addStatExportType(
        DynamicBucketUpdater::kFb303ErrConnect,     stats::SUM);
    fbBase_->addStatExportType(
        DynamicBucketUpdater::kFb303ErrEmptyResult, stats::SUM);
    fbBase_->addStatExportType(
        DynamicBucketUpdater::kFb303RemoteUpdate,   stats::SUM);
    fbBase_->addStatExportType(
        DynamicBucketUpdater::kFb303BucketsUpdated, stats::SUM);
    fbBase_->addStatExportType(
        DynamicBucketUpdater::kFb303ErrNoMapping,   stats::SUM);
    fbBase_->addStatExportType(
        DynamicBucketUpdater::kFb303GetService,     stats::SUM);
  }
}

} //! namespace scribe
