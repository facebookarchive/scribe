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

using namespace facebook::fb303;

using namespace scribe::thrift;
using namespace std;
using boost::shared_ptr;

shared_ptr<scribeHandler> g_Handler;

#define DEFAULT_CHECK_PERIOD       5
#define DEFAULT_MAX_MSG_PER_SECOND 100000
#define DEFAULT_MAX_QUEUE_SIZE     5000000
#define DEFAULT_SERVER_THREADS     1

void print_usage(const char* program_name) {
  cout << "Usage: " << program_name << " [-p port] [-c config_file]" << endl;
}

int main(int argc, char **argv) {

  try {
    /* Increase number of fds */
    struct rlimit r_fd = {65535,65535};
    if (-1 == setrlimit(RLIMIT_NOFILE, &r_fd)) {
      LOG_OPER("setrlimit error (setting max fd size)");
    }

    int next_option;
    const char* const short_options = "hp:c:";
    const struct option long_options[] = {
      { "help",   0, NULL, 'h' },
      { "port",   0, NULL, 'p' },
      { "config", 0, NULL, 'c' },
      { NULL,     0, NULL, 'o' },
    };

    unsigned long int port = 0;  // this can also be specified in the conf file, which overrides the command line
    std::string config_file;
    while (0 < (next_option = getopt_long(argc, argv, short_options, long_options, NULL))) {
      switch (next_option) {
      default:
      case 'h':
        print_usage(argv[0]);
        exit(0);
      case 'c':
        config_file = optarg;
        break;
      case 'p':
        port = strtoul(optarg, NULL, 0);
        break;
      }
    }

    // assume a non-option arg is a config file name
    if (optind < argc && config_file.empty()) {
      config_file = argv[optind];
    }

    // seed random number generation with something reasonably unique
    srand(time(NULL) ^ getpid());

    g_Handler = shared_ptr<scribeHandler>(new scribeHandler(port, config_file));
    g_Handler->initialize();

    shared_ptr<TProcessor> processor(new scribeProcessor(g_Handler));
    /* This factory is for binary compatibility. */
    shared_ptr<TProtocolFactory>
      binaryProtocolFactory(new TBinaryProtocolFactory(0, 0, false, false));
    shared_ptr<ThreadManager> thread_manager;

    if (g_Handler->numThriftServerThreads > 1) {
      // create a ThreadManager to process incoming calls
      thread_manager = ThreadManager::
        newSimpleThreadManager(g_Handler->numThriftServerThreads);

      shared_ptr<PosixThreadFactory> thread_factory(new PosixThreadFactory());
      thread_manager->threadFactory(thread_factory);
      thread_manager->start();
    }

    TNonblockingServer server(processor, binaryProtocolFactory,
                              g_Handler->port, thread_manager);

    LOG_OPER("Starting scribe server on port %lu", g_Handler->port);
    fflush(stderr);

    server.serve();

  } catch(std::exception const& e) {
    LOG_OPER("Exception in main: %s", e.what());
  }

  LOG_OPER("scribe server exiting");
  return 0;
}

scribeHandler::scribeHandler(unsigned long int server_port, const std::string& config_file)
  : FacebookBase("Scribe"),
    port(server_port),
    numThriftServerThreads(DEFAULT_SERVER_THREADS),
    checkPeriod(DEFAULT_CHECK_PERIOD),
    pcategories(NULL),
    pcategory_prefixes(NULL),
    configFilename(config_file),
    status(STARTING),
    statusDetails("initial state"),
    numMsgLastSecond(0),
    maxMsgPerSecond(DEFAULT_MAX_MSG_PER_SECOND),
    maxQueueSize(DEFAULT_MAX_QUEUE_SIZE),
    newThreadPerCategory(true) {
  time(&lastMsgTime);
}

scribeHandler::~scribeHandler() {
  deleteCategoryMap(pcategories);
  if (pcategory_prefixes) {
    delete pcategory_prefixes;
    pcategory_prefixes = NULL;
  }
}

// Returns the handler status, but overwrites it with WARNING if it's
// ALIVE and at least one store has a nonempty status.
fb_status scribeHandler::getStatus() {
  RWGuard monitor(scribeHandlerLock);
  Guard status_monitor(statusLock);

  fb_status return_status(status);
  if (status == ALIVE) {
    for (category_map_t::iterator cat_iter = pcategories->begin();
        cat_iter != pcategories->end();
        ++cat_iter) {
      for (store_list_t::iterator store_iter = cat_iter->second->begin();
           store_iter != cat_iter->second->end();
           ++store_iter)
      {
        if (!(*store_iter)->getStatus().empty())
        {
          return_status = WARNING;
          return return_status;
        }
      } // for each store
    } // for each category
  } // if we don't have an interesting top level status
  return return_status;
}

void scribeHandler::setStatus(fb_status new_status) {
  LOG_OPER("STATUS: %s", statusAsString(new_status));
  Guard status_monitor(statusLock);
  status = new_status;
}

// Returns the handler status details if non-empty,
// otherwise the first non-empty store status found
void scribeHandler::getStatusDetails(std::string& _return) {
  RWGuard monitor(scribeHandlerLock);
  Guard status_monitor(statusLock);

  _return = statusDetails;
  if (_return.empty()) {
    if (pcategories) {
      for (category_map_t::iterator cat_iter = pcategories->begin();
          cat_iter != pcategories->end();
          ++cat_iter) {
        for (store_list_t::iterator store_iter = cat_iter->second->begin();
            store_iter != cat_iter->second->end();
            ++store_iter) {

          if (!(_return = (*store_iter)->getStatus()).empty()) {
            return;
          }
        } // for each store
      } // for each category
    }
  } // if we don't have an interesting top level status
  return;
}

void scribeHandler::setStatusDetails(const string& new_status_details) {
  LOG_OPER("STATUS: %s", new_status_details.c_str());
  Guard status_monitor(statusLock);
  statusDetails = new_status_details;
}

const char* scribeHandler::statusAsString(fb_status status) {
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


// Should be called while holding a writeLock on scribeHandlerLock
bool scribeHandler::createCategoryFromModel(
  const string &category, const boost::shared_ptr<StoreQueue> &model) {

  if ((pcategories == NULL) ||
      (pcategories->find(category) != pcategories->end())) {
    return false;
  }

  LOG_OPER("[%s] Creating new category from model %s", category.c_str(),
           model->getCategoryHandled().c_str());

  // Make sure the category name is sane.
  try {
    string clean_path = boost::filesystem::path(category).string();

    if (clean_path.compare(category) != 0) {
      LOG_OPER("Category not a valid boost filename");
      return false;
    }

  } catch(std::exception const& e) {
    LOG_OPER("Category not a valid boost filename.  Boost exception:%s", e.what());
    return false;
  }

  shared_ptr<StoreQueue> pstore;
  if (newThreadPerCategory) {
    // Create a new thread/StoreQueue for this category
    pstore = shared_ptr<StoreQueue>(new StoreQueue(model, category));

    // queue a command to the store to open it
    pstore->open();
  } else {
    // Use existing StoreQueue
    pstore = model;
  }

  shared_ptr<store_list_t> pstores =
    shared_ptr<store_list_t>(new store_list_t);

  (*pcategories)[category] = pstores;
  pstores->push_back(pstore);

  return true;
}


// Check if we need to deny this request due to throttling
bool scribeHandler::throttleRequest(const vector<LogEntry>&  messages) {
  // Check if we need to rate limit
  if (throttleDeny(messages.size())) {
    incrementCounter("denied for rate");
    return true;
  }

  if (!pcategories || !pcategory_prefixes) {
    // don't bother to spam anything for this, our status should already
    // be showing up as WARNING in the monitoring tools.
    incrementCounter("invalid requests");
    return true;
  }

  // Throttle based on store queues getting too long.
  // Note that there's one decision for all categories, because the whole array passed to us
  // must either succeed or fail together. Checking before we've queued anything also has
  // the nice property that any size array will succeed if we're unloaded before attempting
  // it, so we won't hit a case where there's a client request that will never succeed.
  // Also note that we always check all categories, not just the ones in this request.
  // This is a simplification based on the assumption that most Log() calls contain most
  // categories.
  unsigned long max_count = 0;
  for (category_map_t::iterator cat_iter = pcategories->begin();
       cat_iter != pcategories->end();
       ++cat_iter) {
    shared_ptr<store_list_t> pstores = cat_iter->second;
    if (!pstores) {
      throw std::logic_error("throttle check: iterator in category map holds null pointer");
    }
    for (store_list_t::iterator store_iter = pstores->begin();
         store_iter != pstores->end();
         ++store_iter) {
      if (*store_iter == NULL) {
        throw std::logic_error("throttle check: iterator in store map holds null pointer");
      } else {
        unsigned long size = (*store_iter)->getSize();
        if (size > max_count) {
          max_count = size;
        }
      }
    }
  }

  if (max_count > maxQueueSize) {
    incrementCounter("denied for queue size");
    return true;
  }

  return false;
}

// Should be called while holding a writeLock on scribeHandlerLock
shared_ptr<store_list_t> scribeHandler::createNewCategory(
  const string& category) {

  shared_ptr<store_list_t> store_list;

  // First, check the list of category prefixes for a model
  category_prefix_map_t::iterator cat_prefix_iter = pcategory_prefixes->begin();
  while (cat_prefix_iter != pcategory_prefixes->end()) {
    string::size_type len = cat_prefix_iter->first.size();
    if (cat_prefix_iter->first.compare(0, len-1, category, 0, len-1) == 0) {
      // Found a matching prefix model

      createCategoryFromModel(category, cat_prefix_iter->second);
      category_map_t::iterator cat_iter = pcategories->find(category);

      if (cat_iter != pcategories->end()) {
        store_list = cat_iter->second;
      } else {
        LOG_OPER("failed to create new prefix store for category <%s>",
                 category.c_str());
      }

      break;
    }
    cat_prefix_iter++;
  }

  // Then try creating a store if we have a default store defined
  if (store_list == NULL) {
    if (defaultStore != NULL) {

      createCategoryFromModel(category, defaultStore);
      category_map_t::iterator cat_iter = pcategories->find(category);

      if (cat_iter != pcategories->end()) {
        store_list = cat_iter->second;
      } else {
        LOG_OPER("failed to create new default store for category <%s>",
                 category.c_str());
      }
    }
  }

  return store_list;
}

// Add this message to every store in list
void scribeHandler::addMessage(
  const LogEntry& entry,
  const shared_ptr<store_list_t>& store_list) {

  int numstores = 0;

  // Add message to store_list
  for (store_list_t::iterator store_iter = store_list->begin();
       store_iter != store_list->end();
       ++store_iter) {
    ++numstores;
    boost::shared_ptr<LogEntry> ptr(new LogEntry);
    ptr->category = entry.category;
    ptr->message = entry.message;

    (*store_iter)->addMessage(ptr);
  }

  if (numstores) {
    incrementCounter("received good");
  } else {
    incrementCounter("received bad");
  }
}


ResultCode scribeHandler::Log(const vector<LogEntry>&  messages) {
  ResultCode result;

  scribeHandlerLock.acquireRead();

  if (throttleRequest(messages)) {
    result = TRY_LATER;
    goto end;
  }

  for (vector<LogEntry>::const_iterator msg_iter = messages.begin();
       msg_iter != messages.end();
       ++msg_iter) {

    // disallow blank category from the start
    if ((*msg_iter).category.empty()) {
      incrementCounter("received blank category");
      continue;
    }

    shared_ptr<store_list_t> store_list;
    string category = (*msg_iter).category;

    // First look for an exact match of the category
    if (pcategories) {
      category_map_t::iterator cat_iter = pcategories->find(category);
      if (cat_iter != pcategories->end()) {
        store_list = cat_iter->second;
      }
    }

    // Try creating a new store for this category if we didn't find one
    if (store_list == NULL) {
      // Need write lock to create a new category
      scribeHandlerLock.release();
      scribeHandlerLock.acquireWrite();

      store_list = createNewCategory(category);

      scribeHandlerLock.release();
      scribeHandlerLock.acquireRead();
    }

    if (store_list == NULL) {
       LOG_OPER("log entry has invalid category <%s>",
                (*msg_iter).category.c_str());
      incrementCounter("received bad");
      continue;
    }

    // Log this message
    addMessage(*msg_iter, store_list);
  }

  result = OK;

 end:
  scribeHandlerLock.release();
  return result;
}

// Returns true if overloaded.
// Allows a fixed number of messages per second.
bool scribeHandler::throttleDeny(int num_messages) {
  time_t now;
  time(&now);
  if (now != lastMsgTime) {
    lastMsgTime = now;
    numMsgLastSecond = 0;
  }

  // If we get a single huge packet it's not cool, but we'd better
  // accept it or we'll keep having to read it and deny it indefinitely
  if (num_messages > (int)maxMsgPerSecond/2) {
    LOG_OPER("throttle allowing rediculously large packet with <%d> messages", num_messages);
    return false;
  }

  if (numMsgLastSecond + num_messages > maxMsgPerSecond) {
    LOG_OPER("throttle denying request with <%d> messages. It would exceed max of <%lu> messages this second",
           num_messages, maxMsgPerSecond);
    return true;
  } else {
    numMsgLastSecond += num_messages;
    return false;
  }
}

void scribeHandler::stopStores() {
  setStatus(STOPPING);

  // Thrift doesn't currently support stopping the server from the handler,
  // so this could leave clients in weird states.
  deleteCategoryMap(pcategories);
  pcategories = NULL;
  if (pcategory_prefixes) {
    delete pcategory_prefixes;
    pcategory_prefixes = NULL;
  }
}

void scribeHandler::shutdown() {
  RWGuard monitor(scribeHandlerLock, true);

  stopStores();
  exit(0);
}

void scribeHandler::reinitialize() {
  RWGuard monitor(scribeHandlerLock, true);

  // reinitialize() will re-read the config file and re-configure the stores.
  // This is done without shutting down the Thrift server, so this will not
  // reconfigure any server settings such as port number.
  LOG_OPER("reinitializing");
  stopStores();
  initialize();
}

void scribeHandler::initialize() {

  // This clears out the error state, grep for setStatus below for details
  setStatus(STARTING);
  setStatusDetails("configuring");

  bool perfect_config = true;
  bool enough_config_to_run = true;
  int numstores = 0;

  pnew_categories = new category_map_t;
  pnew_category_prefixes = new category_prefix_map_t;
  tmpDefault.reset();

  try {
    // Get the config data and parse it.
    // If a file has been explicitly specified we'll take the conf from there,
    // which is very handy for testing and one-off applications.
    // Otherwise we'll try to get it from the service management console and
    // fall back to a default file location. This is for production.
    StoreConf config;
    string config_file;

    if (configFilename.empty()) {
      config_file = DEFAULT_CONF_FILE_LOCATION;
    } else {
      config_file = configFilename;
    }
    config.parseConfig(config_file);

    // load the global config
    config.getUnsigned("max_msg_per_second", maxMsgPerSecond);
    config.getUnsigned("max_queue_size", maxQueueSize);
    config.getUnsigned("check_interval", checkPeriod);

    // If new_thread_per_category, then we will create a new thread/StoreQueue
    // for every unique message category seen.  Otherwise, we will just create
    // one thread for each top-level store defined in the config file.
    string temp;
    config.getString("new_thread_per_category", temp);
    if (0 == temp.compare("no")) {
      newThreadPerCategory = false;
    } else {
      newThreadPerCategory = true;
    }

    unsigned long int old_port = port;
    config.getUnsigned("port", port);
    if (old_port != 0 && port != old_port) {
      LOG_OPER("port %lu from conf file overriding old port %lu", port, old_port);
    }
    if (port <= 0) {
      throw runtime_error("No port number configured");
    }

    // check if config sets the size to use for the ThreadManager
    unsigned long int num_threads;
    if (config.getUnsigned("num_thrift_server_threads", num_threads)) {
      numThriftServerThreads = (size_t) num_threads;

      if (numThriftServerThreads <= 0) {
        LOG_OPER("invalid value for num_thrift_server_threads: %lu",
                 num_threads);
        throw runtime_error("invalid value for num_thrift_server_threads");
      }
    }

    // Build a new map of stores, and move stores from the old map as
    // we find them in the config file. Any stores left in the old map
    // at the end will be deleted.
    std::vector<pStoreConf> store_confs;
    config.getAllStores(store_confs);
    for (std::vector<pStoreConf>::iterator iter = store_confs.begin();
         iter != store_confs.end();
         ++iter) {
        pStoreConf store_conf = (*iter);

        bool success = configureStore(store_conf, &numstores);

        if (!success) {
          perfect_config = false;
        }
    }
  } catch(std::exception const& e) {
    string errormsg("Bad config - exception: ");
    errormsg += e.what();
    setStatusDetails(errormsg);
    perfect_config = false;
    enough_config_to_run = false;
  }

  if (numstores) {
    LOG_OPER("configured <%d> stores", numstores);
  } else {
    setStatusDetails("No stores configured successfully");
    perfect_config = false;
    enough_config_to_run = false;
  }

  // clean up existing stores
  deleteCategoryMap(pcategories);
  pcategories = NULL;
  if (pcategory_prefixes) {
    delete pcategory_prefixes;
    pcategory_prefixes = NULL;
  }
  defaultStore.reset();

  if (enough_config_to_run) {
    pcategories = pnew_categories;
    pcategory_prefixes = pnew_category_prefixes;
    defaultStore = tmpDefault;
  } else {
    // If the new configuration failed we'll run with
    // nothing configured and status set to WARNING
    deleteCategoryMap(pnew_categories);
    if (pnew_category_prefixes) {
      delete pnew_category_prefixes;
    }
  }

  pnew_categories = NULL;
  pnew_category_prefixes = NULL;
  tmpDefault.reset();

  if (!perfect_config || !enough_config_to_run) { // perfect should be a subset of enough, but just in case
    setStatus(WARNING); // status details should have been set above
  } else {
    setStatusDetails("");
    setStatus(ALIVE);
  }
}


// Configures the store specified by the store configuration. Returns false if failed.
bool scribeHandler::configureStore(pStoreConf store_conf, int *numstores) {
  string category;
  shared_ptr<StoreQueue> pstore;
  vector<string> category_list;
  shared_ptr<StoreQueue> model;
  bool single_category = true;


  // Check if a single category is specified
  if (store_conf->getString("category", category)) {
    category_list.push_back(category);
  }

  // Check if multiple categories are specified
  string categories;
  if (store_conf->getString("categories", categories)) {
    // We want to set up to configure multiple categories, even if there is
    // only one category specified here so that configuration is consistent
    // for the 'categories' keyword.
    single_category = false;

    // Parse category names, separated by whitespace
    stringstream ss(categories);

    while (ss >> category) {
      category_list.push_back(category);
    }
  }

  if (category_list.size() == 0) {
    setStatusDetails("Bad config - store with no category");
    return false;
  }
  else if (single_category) {
    // configure single store
    shared_ptr<StoreQueue> result =
      configureStoreCategory(store_conf, category_list[0], model);

    if (result == NULL) {
      return false;
    }

    (*numstores)++;
  } else {
    // configure multiple stores
    string type;

    if (!store_conf->getString("type", type) ||
        type.empty()) {
      string errormsg("Bad config - no type for store with category: ");
      errormsg += categories;
      setStatusDetails(errormsg);
      return false;
    }

    // create model so that we can create stores as copies of this model
    model = configureStoreCategory(store_conf, categories, model, true);

    if (model == NULL) {
      string errormsg("Bad config - could not create store for category: ");
      errormsg += categories;
      setStatusDetails(errormsg);
      return false;
    }

    // create a store for each category
    vector<string>::iterator iter;
    for (iter = category_list.begin(); iter < category_list.end(); iter++) {
       shared_ptr<StoreQueue> result =
         configureStoreCategory(store_conf, *iter, model);

      if (!result) {
        return false;
      }

      (*numstores)++;
    }
  }

  return true;
}


// Configures the store specified by the store configuration and category.
shared_ptr<StoreQueue> scribeHandler::configureStoreCategory(
  pStoreConf store_conf,                       //configuration for store
  const string &category,                      //category name
  const boost::shared_ptr<StoreQueue> &model,  //model to use (optional)
  bool category_list) {                        //is a list of stores?

  bool is_default = false;
  bool already_created = false;

  if (category.empty()) {
    setStatusDetails("Bad config - store with blank category");
    return shared_ptr<StoreQueue>();
  }

  LOG_OPER("CATEGORY : %s", category.c_str());
  if (0 == category.compare("default")) {
    if (tmpDefault != NULL) {
      setStatusDetails("Bad config - multiple default stores specified");
      return shared_ptr<StoreQueue>();
    }
    is_default = true;
  }

  bool is_prefix_category = (!category.empty() &&
                             category[category.size() - 1] == '*' &&
                             !category_list);

  std::string type;
  if (!store_conf->getString("type", type) ||
      type.empty()) {
    string errormsg("Bad config - no type for store with category: ");
    errormsg += category;
    setStatusDetails(errormsg);
    return shared_ptr<StoreQueue>();
  }

  // look for the store in the current list
  shared_ptr<StoreQueue> pstore;
  if (!is_prefix_category && pcategories) {
    category_map_t::iterator category_iter = pcategories->find(category);
    if (category_iter != pcategories->end()) {
      shared_ptr<store_list_t> pstores = category_iter->second;

      for ( store_list_t::iterator it = pstores->begin(); it != pstores->end(); ++it ) {
        if ( (*it)->getBaseType() == type &&
             pstores->size() <=  1) { // no good way to match them up if there's more than one
          pstore = (*it);
          pstores->erase(it);
        }
      }
    }
  }

  try {
    // create a new store if it doesn't already exist
    if (!pstore) {
      if (model != NULL) {
        // Create a copy of the model if we want a new thread per category
        if (newThreadPerCategory && !is_default && !is_prefix_category) {
          pstore = shared_ptr<StoreQueue>(new StoreQueue(model, category));
        } else {
          pstore = model;
          already_created = true;
        }
      } else {
        string store_name;
        bool is_model, multi_category, categories;

        /* remove any *'s from category name */
        if (is_prefix_category)
          store_name = category.substr(0, category.size() - 1);
        else
          store_name = category;

        // Does this store define multiple categories
        categories = (is_default || is_prefix_category || category_list);

        // Determine if this store will actually handle multiple categories
        multi_category = !newThreadPerCategory && categories;

        // Determine if this store is just a model for later stores
        is_model = newThreadPerCategory && categories;

        pstore =
          shared_ptr<StoreQueue>(new StoreQueue(type, store_name, checkPeriod,
                                                is_model, multi_category));
      }
    }
  } catch (...) {
    pstore.reset();
  }

  if (!pstore) {
    string errormsg("Bad config - can't create a store of type: ");
    errormsg += type;
    setStatusDetails(errormsg);
    return shared_ptr<StoreQueue>();
  }

  // open store. and configure it if not copied from a model
  if (model == NULL) {
    pstore->configureAndOpen(store_conf);
  } else if (!already_created) {
    pstore->open();
  }

  if (is_default) {
    LOG_OPER("Creating default store");
    tmpDefault = pstore;
  }
  else if (is_prefix_category) {
    category_prefix_map_t::iterator category_iter =
      pnew_category_prefixes->find(category);

    if (category_iter == pnew_category_prefixes->end()) {
      (*pnew_category_prefixes)[category] = pstore;
    } else {
      string errormsg =
        "Bad config - multiple prefix stores specified for category: ";

      errormsg += category;
      setStatusDetails(errormsg);
      return shared_ptr<StoreQueue>();
    }
  }

  // push the new store onto the new map if it's not just a model
  if (!pstore->isModelStore() && !category_list) {
    shared_ptr<store_list_t> pstores;
    category_map_t::iterator category_iter = pnew_categories->find(category);
    if (category_iter != pnew_categories->end()) {
      pstores = category_iter->second;
    } else {
      pstores = shared_ptr<store_list_t>(new store_list_t);
      (*pnew_categories)[category] = pstores;
    }
        pstores->push_back(pstore);
  }

  return pstore;
}


// delete pcats and everything it contains
void scribeHandler::deleteCategoryMap(category_map_t *pcats) {
  if (!pcats) {
    return;
  }
  for (category_map_t::iterator cat_iter = pcats->begin();
       cat_iter != pcats->end();
       ++cat_iter) {
    shared_ptr<store_list_t> pstores = cat_iter->second;
    if (!pstores) {
      throw std::logic_error("deleteCategoryMap: iterator in category map holds null pointer");
    }
    for (store_list_t::iterator store_iter = pstores->begin();
         store_iter != pstores->end();
         ++store_iter) {
      if (!*store_iter) {
        throw std::logic_error("deleteCategoryMap: iterator in store map holds null pointer");
      }

      (*store_iter)->stop();
    } // for each store
    pstores->clear();
  } // for each category
  pcats->clear();
  delete pcats;
}
