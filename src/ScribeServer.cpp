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
#include "TimeLatency.h"

using namespace facebook;
using namespace facebook::fb303;
using namespace scribe::thrift;

static const unsigned long kDefaultCheckPeriod       = 5;
static const unsigned long kDefaultMaxMsgPerSecond   = 0;
static const uint64_t kDefaultMaxQueueSize           = 5000000;
static const unsigned long kDefaultServerThreads     = 3;
static const unsigned long kDefaultMaxConn           = 0;
static const unsigned long kDefaultMaxConcurrentReq  = 0;

static const string kOverallCategory     = "scribe_overall";
static const string kLogSeparator        = ".";
static const string kLogLatency          = "latency";
static const string kLogHop              = "hop";
static const string kLogWriter           = "writer";

namespace scribe {

shared_ptr<ScribeHandler> g_handler;

void ScribeHandler::incCounter(const string& category, const string& counter) {
  incCounter(category, counter, 1);
}

void ScribeHandler::incCounter(const string& category, const string& counter,
                               long amount) {
  // fb303 has the following types of counters:
  // count, sum, avg, and rate
  // if not explicitly specified, then avg is assumed.
  // since we are usually adding 1 when call this function, the avg will
  // always be 1 since avg = sum(amount) / count and count increment by 1
  // everytime we call it. This is probably not what we want it. So we
  // need to export it as of type sum
  static unordered_set<string> catMap;
  static Mutex catMapLock;

  string catKey = category + kLogSeparator + counter;
  string overallKey = kOverallCategory + kLogSeparator + counter;

  {
    Guard g(catMapLock);

    if (catMap.find(catKey) == catMap.end()) {
      catMap.insert(catKey);
      addStatExportType(catKey,     stats::SUM);
      addStatExportType(overallKey, stats::SUM);
      addStatExportType(catKey,     stats::RATE);
      addStatExportType(overallKey, stats::RATE);
    }
  }

  incrementCounter(catKey, amount);
  incrementCounter(overallKey, amount);
  addStatValue(catKey, amount);
  addStatValue(overallKey, amount);
}

void ScribeHandler::incCounter(const string& counter) {
  incCounter(counter, 1);
}

void ScribeHandler::incCounter(const string& counter, long amount) {
  incrementCounter(kOverallCategory + kLogSeparator + counter, amount);
}

void ScribeHandler::reportLatencyHop(const string& category, long ms) {
  reportLatency(category, kLogHop, ms);
}

void ScribeHandler::reportLatencyWriter(const string& category, long ms) {
  reportLatency(category, kLogWriter, ms);
}

void ScribeHandler::reportLatency(const string& category, const string& type,
                                  long ms) {
  // export for histogram
  static unordered_set<string> catMap;
  static Mutex                 catMapLock;

  string catKey = category + kLogSeparator + kLogLatency + kLogSeparator +type;
  string overallKey = kOverallCategory + kLogSeparator + kLogLatency
                     + kLogSeparator + type;

  {
    Guard g(catMapLock);

    if (catMap.find(catKey) == catMap.end()) {
      // export a histogram with Avg, 75%, 95%, 99%, with latency range
      // from [0, 10] seconds, and 100 buckets distribution
      // TODO: make max configurable. currently hard coded to 10sec.
      addHistAndStatExports(catKey, "AVG,COUNT,SUM,75,95,99,0,100", 100, 0,
                            10000);
      catMap.insert(catKey);
    }

    if (catMap.find(overallKey) == catMap.end()) {
      addHistAndStatExports(overallKey, "AVG,COUNT,SUM,75,95,99,0,100", 100, 0,
                            10000);
      catMap.insert(overallKey);
    }
  }

  addHistAndStatValue(catKey, ms);
  addHistAndStatValue(overallKey, ms);
}

ScribeHandler::ScribeHandler(unsigned long serverPort,
                             const string& configFile)
  : FacebookBase("Scribe"),
    port(serverPort),
    numThriftServerThreads(kDefaultServerThreads),
    checkPeriod_(kDefaultCheckPeriod),
    configFilename_(configFile),
    status_(STARTING),
    statusDetails_("initial state"),
    numMsgLastSecond_(0),
    maxMsgPerSecond_(kDefaultMaxMsgPerSecond),
    maxConn_(kDefaultMaxConn),
    maxConcurrentReq_(kDefaultMaxConcurrentReq),
    maxQueueSize_(kDefaultMaxQueueSize),
    newThreadPerCategory_(true),
    timeStampSampleRate_(0) {
  time(&lastMsgTime_);
  scribeHandlerLock_ = scribe::concurrency::createReadWriteMutex();
}

ScribeHandler::~ScribeHandler() {
  deleteCategoryMap(categories_);
  deleteCategoryMap(categoryPrefixes_);
}

// Returns the handler status, but overwrites it with WARNING if it's
// ALIVE and at least one store has a nonempty status.
fb_status ScribeHandler::getStatus() {
  fb_status returnStatus;

  {
    Guard g(statusLock_);

    returnStatus = status_;
  }

  if (returnStatus == ALIVE) {
    RWGuard rGuard(*scribeHandlerLock_);

    for (CategoryMap::iterator catIt = categories_.begin();
         catIt != categories_.end();
         ++catIt) {
      for (StoreList::iterator storeIt = catIt->second->begin();
           storeIt != catIt->second->end();
           ++storeIt) {
        if (!(*storeIt)->getStatus().empty()) {
          returnStatus = WARNING;
          return returnStatus;
        }
      } // for each store
    } // for each category
  } // if we don't have an interesting top level status

  return returnStatus;
}

void ScribeHandler::setStatus(fb_status newStatus) {
  LOG_OPER("STATUS: %s", statusAsString(newStatus));
  statusLock_.lock();
  status_ = newStatus;
  statusLock_.unlock();
}

// Returns the handler status details if non-empty,
// otherwise the first non-empty store status found
void ScribeHandler::getStatusDetails(string& retVal) {

  statusLock_.lock();
  retVal = statusDetails_;
  statusLock_.unlock();
  if (retVal.empty()) {
    RWGuard rGuard(*scribeHandlerLock_);

    for (CategoryMap::iterator catIt = categories_.begin();
         catIt != categories_.end();
         ++catIt) {
      for (StoreList::iterator storeIt = catIt->second->begin();
           storeIt != catIt->second->end();
           ++storeIt) {

        if (!(retVal = (*storeIt)->getStatus()).empty()) {
          return;
        }
      } // for each store
    } // for each category
  } // if we don't have an interesting top level status
  return;
}

void ScribeHandler::setStatusDetails(const string& newStatusDetails) {
  LOG_OPER("STATUS: %s", newStatusDetails.c_str());
  statusLock_.lock();
  statusDetails_ = newStatusDetails;
  statusLock_.unlock();
}

const char* ScribeHandler::statusAsString(fb_status status) {
  switch (status) {
  case DEAD:
    return "DEAD";
  case STARTING:
    return "STARTING";
  case ALIVE:
    return "ALIVE";
  case STOPPING:
    return "STOPPING";
  case STOPPED:
    return "STOPPED";
  case WARNING:
    return "WARNING";
  default:
    return "unknown status code";
  }
}


// Should be called while holding a writeLock on scribeHandlerLock_
bool ScribeHandler::createCategoryFromModel(
  const string &category, const StoreQueuePtr &model) {

  StoreQueuePtr pstore;
  if (newThreadPerCategory_) {
    // Create a new thread/StoreQueue for this category
    pstore.reset(new StoreQueue(model, category));
    LOG_OPER("[%s] Creating new category store from model %s",
             category.c_str(), model->getCategoryHandled().c_str());

    // queue a command to the store to open it
    pstore->open();
  } else {
    // Use existing StoreQueue
    pstore = model;
    LOG_OPER("[%s] Using existing store for the config categories %s",
             category.c_str(), model->getCategoryHandled().c_str());
  }

  StoreListPtr pstores;
  CategoryMap::iterator catIt = categories_.find(category);
  if (catIt == categories_.end()) {
    pstores.reset(new StoreList);
    categories_[category] = pstores;
  } else {
    pstores = catIt->second;
  }
  pstores->push_back(pstore);

  return true;
}


// Check if we need to deny this request due to throttling
bool ScribeHandler::throttleRequest(const vector<LogEntry>&  messages) {
  // Check if we need to rate limit
  if (throttleDeny(messages.size())) {
    incCounter("denied for rate");
    stats.addCounter(StatCounters::kScribedDfrate, messages.size());
    return true;
  }

  // Throttle based on store queues getting too long.
  // Note that there's one decision for all categories, because the whole array
  // passed to us must either succeed or fail together. Checking before we've
  // queued anything also has the nice property that any size array will succeed
  // if we're unloaded before attempting it, so we won't hit a case where
  // there's a client request that will never succeed. Also note that we always
  // check all categories, not just the ones in this request. This is a
  // simplification based on the assumption that most Log() calls contain most
  // categories.
  for (CategoryMap::iterator catIt = categories_.begin();
       catIt != categories_.end();
       ++catIt) {
    const StoreListPtr& stores = catIt->second;
    if (!stores) {
      throw std::logic_error(
        "throttle check: iterator in category map holds null pointer"
      );
    }

    for (StoreList::iterator storeIt = stores->begin();
         storeIt != stores->end();
         ++storeIt) {
      const StoreQueuePtr& queue = *storeIt;

      if (queue == NULL) {
        throw std::logic_error(
          "throttle check: iterator in store map holds null pointer"
        );
      } else {
        if (queue->getSize() > maxQueueSize_) {
          incCounter((*storeIt)->getCategoryHandled(), "denied for queue size");
          stats.addCounter(StatCounters::kScribedDfqs, 1);
          return true;
        }
      }
    }
  }

  return false;
}

// Should be called while holding a writeLock on scribeHandlerLock_
StoreListPtr ScribeHandler::createNewCategory(
  const string& category) {

  StoreListPtr storeList;

  // First, check the list of category prefixes for a model
  CategoryMap::iterator prefixIt = categoryPrefixes_.begin();
  while (prefixIt != categoryPrefixes_.end()) {
    string::size_type len = prefixIt->first.size();
    if (prefixIt->first.compare(0, len-1, category, 0, len-1) == 0) {
      // Found a matching prefix model

      StoreListPtr pstores = prefixIt->second;
      for (StoreList::iterator storeIt = pstores->begin();
          storeIt != pstores->end(); ++storeIt) {
        createCategoryFromModel(category, *storeIt);
      }
      CategoryMap::iterator catIt = categories_.find(category);

      if (catIt != categories_.end()) {
        storeList = catIt->second;
      } else {
        LOG_OPER("failed to create new prefix store for category <%s>",
                 category.c_str());
      }

      break;
    }
    ++ prefixIt;
  }


  // Then try creating a store if we have a default store defined
  if (storeList == NULL && !defaultStores_.empty()) {
    for (StoreList::iterator storeIt = defaultStores_.begin();
        storeIt != defaultStores_.end(); ++storeIt) {
      createCategoryFromModel(category, *storeIt);
    }
    CategoryMap::iterator catIt = categories_.find(category);
    if (catIt != categories_.end()) {
      storeList = catIt->second;
    } else {
      LOG_OPER("failed to create new default store for category <%s>",
          category.c_str());
    }
  }

  return storeList;
}

// Add this message to every store in list
void ScribeHandler::addMessage(
  const LogEntry& entry,
  const StoreListPtr& storeList) {

  int numstores = 0;

  // Add message to storeList
  for (StoreList::iterator storeIt = storeList->begin();
       storeIt != storeList->end();
       ++storeIt) {
    ++numstores;
    const StoreQueuePtr& queue = *storeIt;

    LogEntryPtr ptr(new LogEntry);
    *ptr = entry;
    queue->addMessage(ptr);
  }

  if (numstores) {
    incCounter(entry.category, "received good");
  } else {
    incCounter(entry.category, "received bad");
  }
}

ResultCode ScribeHandler::Log(const vector<LogEntry>&  messagesConst) {
  ResultCode result = TRY_LATER;
  vector <LogEntry> & messages = const_cast<vector<LogEntry>&> (messagesConst);

  // local variables to reduce accounting overhead
  int64_t msgIgnored = 0, msgEnqueued = 0, msgAdmitted = 0;

  addHistAndStatValue(StatCounters::kScribedIn, messages.size());

  scribeHandlerLock_->acquireRead();

  // this is a time that we have unmarshaled all messages.
  // hop-by-hop latency is defined as the difference of time between two hops
  // at this exact points.
  unsigned long currTimeStamp = getCurrentTimeStamp();

  if(status_ == STOPPING) {
    result = TRY_LATER;
    msgIgnored += messages.size();
    goto end;
  }

  if (throttleRequest(messages)) {
    result = TRY_LATER;
    msgIgnored += messages.size();
    goto end;
  }

  for (vector<LogEntry>::iterator mesgIt = messages.begin();
       mesgIt != messages.end();
       ++ mesgIt) {
    LogEntry& mesg = *mesgIt;

    // disallow blank category from the start
    if (mesg.category.empty()) {
      incCounter("received blank category");
      ++ msgIgnored;
      continue;
    }

    // Disallow category names that are not valid filenames
    // This create problems in Filestore file creation
    if (!boost::filesystem::portable_posix_name(mesg.category)) {
      incCounter("received invalid category name");
      ++ msgIgnored;
      continue;
    }

    StoreListPtr storeList;
    string category = mesg.category;

    CategoryMap::iterator catIt;
    // First look for an exact match of the category
    if ((catIt = categories_.find(category)) != categories_.end()) {
      storeList = catIt->second;
    }

    // Try creating a new store for this category if we didn't find one
    if (storeList == NULL) {
      // Need write lock to create a new category
      scribeHandlerLock_->release();
      scribeHandlerLock_->acquireWrite();

      // This may cause some duplicate messages if some messages in this batch
      // were already added to queues
      if(status_ == STOPPING) {
        result = TRY_LATER;
        msgIgnored += distance(mesgIt, messages.end());
        goto end;
      }

      if ((catIt = categories_.find(category)) != categories_.end()) {
        storeList = catIt->second;
      } else {
        storeList = createNewCategory(category);
      }
    }

    if (storeList == NULL) {
      LOG_OPER("log entry has invalid category <%s>", category.c_str());
      incCounter(category, "received bad");
      ++ msgIgnored;

      continue;
    }

    // Log the latency
    if (isTimeStampPresent(mesg)) {
      unsigned long mesgTimestamp = getTimeStamp(mesg);
      long latency = currTimeStamp - mesgTimestamp;
      reportLatencyHop(category, latency);
      removeTimeStamp(mesg);
    }
    // Add the timestamp?
    if (timeStampSampleRate_ > 0 && drand48() < timeStampSampleRate_) {
      updateTimeStamp(mesg, currTimeStamp);
    }

    // Log this message
    addMessage(mesg, storeList);
    msgEnqueued += storeList->size();

    if (storeList->empty()) {
      ++ msgIgnored;
    } else {
      ++ msgAdmitted;
    }
  }

  result = OK;

 end:
  scribeHandlerLock_->release();

  stats.addCounter(StatCounters::kScribedIgnore, msgIgnored);
  stats.addCounter(StatCounters::kScribedAdmit,  msgAdmitted);
  stats.addCounter(StatCounters::kStoreQueueIn, msgEnqueued);
  stats.incStoreQueueSize(msgEnqueued);

  return result;
}

// Returns true if overloaded.
// Allows a fixed number of messages per second.
bool ScribeHandler::throttleDeny(int numMessages) {
  time_t now;
  if (0 == maxMsgPerSecond_)
    return false;

  time(&now);
  if (now != lastMsgTime_) {
    lastMsgTime_ = now;
    numMsgLastSecond_ = 0;
  }

  // If we get a single huge packet it's not cool, but we'd better
  // accept it or we'll keep having to read it and deny it indefinitely
  if (numMessages > (int)maxMsgPerSecond_/2) {
    LOG_OPER("throttle allowing rediculously large packet with <%d> messages",
             numMessages);
    return false;
  }

  if (numMsgLastSecond_ + numMessages > maxMsgPerSecond_) {
    LOG_OPER("throttle denying request with <%d> messages. It would exceed max of <%lu> messages this second",
             numMessages, maxMsgPerSecond_);
    return true;
  } else {
    numMsgLastSecond_ += numMessages;
    return false;
  }
}

void ScribeHandler::stopStores() {
  scribeHandlerLock_->acquireWrite();
  setStatus(STOPPING);
  scribeHandlerLock_->release();
  /*
   * Only server thread left operating. Others will see the STOPPING
   * status and will return with TRY_LATER. This thread will now block
   * multiple times waiting to pthread_join other store threads.
   */
  StoreListPtr storeList;
  for (StoreList::iterator storeIt = defaultStores_.begin();
      storeIt != defaultStores_.end(); ++storeIt) {
    if (!(*storeIt)->isModelStore()) {
      (*storeIt)->stop();
    }
  }
  defaultStores_.clear();
  deleteCategoryMap(categories_);
  deleteCategoryMap(categoryPrefixes_);
}

void ScribeHandler::shutdown() {
  stopStores();
  // calling stop to allow thrift to clean up client states and exit
  server->stop();
  scribe::stopServer();
}

void ScribeHandler::reinitialize() {

  // reinitialize() will re-read the config file and re-configure the stores.
  // This is done without shutting down the Thrift server, so this will not
  // reconfigure any server settings such as port number.
  LOG_OPER("reinitializing");
  stopStores();
  initialize();
}

void ScribeHandler::initialize() {

  /*
   * Don't change status until you are done. Otherwise other threads
   * will march in
   */
  stats.initCounters();

  setStatusDetails("configuring");

  bool perfectConfig = true;
  bool enoughConfigToRun = true;
  int numStores = 0;

  // Get the config data and parse it.
  // If a file has been explicitly specified we'll take the conf from there,
  // which is very handy for testing and one-off applications.
  // Otherwise we'll try to get it from the service management console and
  // fall back to a default file location. This is for production.
  StoreConf localConfig;
  string configFile;

  if (configFilename_.empty()) {
    configFile = kDefaultConfFileLocation;
  } else {
    configFile = configFilename_;
  }
  localConfig.parseConfig(configFile);

  // overwrite the current StoreConf
  config_ = localConfig;

  // load the global config
  config_.getUnsigned("max_msg_per_second", &maxMsgPerSecond_);
  config_.getUint64("max_queue_size", &maxQueueSize_);
  config_.getUnsigned("check_interval", &checkPeriod_);
  if (checkPeriod_ == 0) {
    checkPeriod_ = 1;
  }
  config_.getUnsigned("max_conn", &maxConn_);
  config_.getUnsigned("max_concurrent_request", &maxConcurrentReq_);
  config_.getFloat("timestamp_sample_rate", &timeStampSampleRate_);

  // If new_thread_per_category, then we will create a new thread/StoreQueue
  // for every unique message category seen.  Otherwise, we will just create
  // one thread for each top-level store defined in the config file.
  string temp;
  config_.getString("new_thread_per_category", &temp);
  if (0 == temp.compare("no")) {
    newThreadPerCategory_ = false;
  } else {
    newThreadPerCategory_ = true;
  }

  unsigned long oldPort = port;
  config_.getUnsigned("port", &port);
  if (oldPort != 0 && port != oldPort) {
    LOG_OPER("port %lu from conf file overriding old port %lu", port, oldPort);
  }
  if (port <= 0) {
    throw runtime_error("No port number configured");
  }

  // check if config sets the size to use for the ThreadManager
  unsigned long numThreads;
  if (config_.getUnsigned("num_thrift_server_threads", &numThreads)) {
    numThriftServerThreads = (size_t) numThreads;

    if (numThriftServerThreads <= 0) {
      LOG_OPER("invalid value for num_thrift_server_threads: %lu",
          numThreads);
      throw runtime_error("invalid value for num_thrift_server_threads");
    }
  }


  // Build a new map of stores, and move stores from the old map as
  // we find them in the config file. Any stores left in the old map
  // at the end will be deleted.
  vector<StoreConfPtr> storeConfs;
  config_.getAllStores(&storeConfs);
  for (vector<StoreConfPtr>::iterator iter = storeConfs.begin();
      iter != storeConfs.end();
      ++iter) {
    StoreConfPtr storeConf = (*iter);

    bool success = configureStore(storeConf, &numStores);

    if (!success) {
      perfectConfig = false;
    }
  }

  if (numStores) {
    LOG_OPER("configured <%d> stores", numStores);
  } else {
    setStatusDetails("No stores configured successfully");
    perfectConfig = false;
    enoughConfigToRun = false;
  }

  if (!enoughConfigToRun) {
    // If the new configuration failed we'll run with
    // nothing configured and status set to WARNING
    deleteCategoryMap(categories_);
    deleteCategoryMap(categoryPrefixes_);
  }

  /*
   * Set the status to something other than STOPPING to let other
   * threads in
   */
  if (!perfectConfig || !enoughConfigToRun) {
    // perfect should be a subset of enough, but just in case
    setStatus(WARNING); // status details should have been set above
  } else {
    setStatusDetails("");
    setStatus(ALIVE);
  }
}


// Configures the store specified by the store configuration.
// Returns false if failed.
bool ScribeHandler::configureStore(StoreConfPtr storeConf, int *numStores) {
  string category;
  StoreQueuePtr pstore;
  vector<string> categoryList;
  StoreQueuePtr model;
  bool singleCategory = true;


  // Check if a single category is specified
  if (storeConf->getString("category", &category)) {
    categoryList.push_back(category);
  }

  // Check if multiple categories are specified
  string categories;
  if (storeConf->getString("categories", &categories)) {
    // We want to set up to configure multiple categories, even if there is
    // only one category specified here so that configuration is consistent
    // for the 'categories' keyword.
    singleCategory = false;

    // Parse category names, separated by whitespace
    stringstream ss(categories);

    while (ss >> category) {
      categoryList.push_back(category);
    }
  }

  if (categoryList.size() == 0) {
    setStatusDetails("Bad config - store with no category");
    return false;
  }
  else if (singleCategory) {
    // configure single store
    StoreQueuePtr result =
      configureStoreCategory(storeConf, categoryList[0], model);

    if (result == NULL) {
      return false;
    }

    (*numStores)++;
  } else {
    // configure multiple stores
    string type;

    if (!storeConf->getString("type", &type) ||
        type.empty()) {
      string errorMesg("Bad config - no type for store with category: ");
      errorMesg += categories;
      setStatusDetails(errorMesg);
      return false;
    }

    // create model so that we can create stores as copies of this model
    model = configureStoreCategory(storeConf, categories, model, true);

    if (model == NULL) {
      string errorMesg("Bad config - could not create store for category: ");
      errorMesg += categories;
      setStatusDetails(errorMesg);
      return false;
    }

    // create a store for each category
    vector<string>::iterator iter;
    for (iter = categoryList.begin(); iter != categoryList.end(); ++ iter) {
       StoreQueuePtr result =
         configureStoreCategory(storeConf, *iter, model);

      if (!result) {
        return false;
      }

      (*numStores)++;
    }
  }

  return true;
}


// Configures the store specified by the store configuration and category.
StoreQueuePtr ScribeHandler::configureStoreCategory(
  StoreConfPtr storeConf,                       //configuration for store
  const string &category,                      //category name
  const StoreQueuePtr &model,  //model to use (optional)
  bool categoryList) {                        //is a list of stores?

  bool isDefault = false;
  bool alreadyCreated = false;

  if (category.empty()) {
    setStatusDetails("Bad config - store with blank category");
    return StoreQueuePtr();
  }

  LOG_OPER("CATEGORY : %s", category.c_str());
  if (0 == category.compare("default")) {
    isDefault = true;
  }

  bool isPrefixCategory = (!category.empty() &&
                             category[category.size() - 1] == '*' &&
                             !categoryList);

  string type;
  if (!storeConf->getString("type", &type) ||
      type.empty()) {
    string errorMesg("Bad config - no type for store with category: ");
    errorMesg += category;
    setStatusDetails(errorMesg);
    return StoreQueuePtr();
  }

  // look for the store in the current list
  StoreQueuePtr pstore;

  try {
    if (model != NULL) {
      // Create a copy of the model if we want a new thread per category
      if (newThreadPerCategory_ && !isDefault && !isPrefixCategory) {
        pstore.reset(new StoreQueue(model, category));
      } else {
        pstore = model;
        alreadyCreated = true;
      }
    } else {
      string storeName;
      bool isModel, multiCategory, categories;

      /* remove any *'s from category name */
      if (isPrefixCategory)
        storeName = category.substr(0, category.size() - 1);
      else
        storeName = category;

      // Does this store define multiple categories
      categories = (isDefault || isPrefixCategory || categoryList);

      // Determine if this store will actually handle multiple categories
      multiCategory = !newThreadPerCategory_ && categories;

      // Determine if this store is just a model for later stores
      isModel = newThreadPerCategory_ && categories;

      pstore.reset(
        new StoreQueue(type, storeName, checkPeriod_, isModel, multiCategory)
      );
    }
  } catch (...) {
    pstore.reset();
  }

  if (!pstore) {
    string errorMesg("Bad config - can't create a store of type: ");
    errorMesg += type;
    setStatusDetails(errorMesg);
    return StoreQueuePtr();
  }

  // open store. and configure it if not copied from a model
  if (model == NULL) {
    pstore->configureAndOpen(storeConf);
  } else if (!alreadyCreated) {
    pstore->open();
  }

  if (categoryList) {
    return (pstore);
  }
  if (isDefault) {
    LOG_OPER("Creating default store");
    defaultStores_.push_back(pstore);
  } else if (isPrefixCategory) {
    StoreListPtr pstores;
    CategoryMap::iterator catIt = categoryPrefixes_.find(category);
    if (catIt != categoryPrefixes_.end()) {
      pstores = catIt->second;
    } else {
      pstores.reset(new StoreList);
      categoryPrefixes_[category] = pstores;
    }
    pstores->push_back(pstore);
  } else if (!pstore->isModelStore()) {
    // push the new store onto the new map if it's not just a model
    StoreListPtr pstores;
    CategoryMap::iterator catIt = categories_.find(category);
    if (catIt != categories_.end()) {
      pstores = catIt->second;
    } else {
      pstores.reset(new StoreList);
      categories_[category] = pstores;
    }
    pstores->push_back(pstore);
  }

  return pstore;
}


// delete everything in cats
void ScribeHandler::deleteCategoryMap(CategoryMap& cats) {
  for (CategoryMap::iterator catIt = cats.begin();
       catIt != cats.end();
       ++catIt) {
    StoreListPtr pstores = catIt->second;
    if (!pstores) {
      throw std::logic_error("deleteCategoryMap: "
          "iterator in category map holds null pointer");
    }
    for (StoreList::iterator storeIt = pstores->begin();
         storeIt != pstores->end();
         ++storeIt) {
      if (!*storeIt) {
        throw std::logic_error("deleteCategoryMap: "
            "iterator in store map holds null pointer");
      }

      if (!(*storeIt)->isModelStore()) {
        (*storeIt)->stop();
      }
    } // for each store
    pstores->clear();
  } // for each category
  cats.clear();
}

} //! namespace scribe
