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
// @author Jason Sobel
// @author John Song

#include <boost/algorithm/string.hpp>
#include "common.h"
#include "conf.h"
#include "scribe_server.h"

using namespace boost;
using namespace std;

extern shared_ptr<scribeHandler> g_Handler;

StoreConf::StoreConf() {
}

StoreConf::~StoreConf() {
}

bool StoreConf::getStore(const string& storeName, pStoreConf& _return) {
  store_conf_map_t::iterator iter = stores.find(storeName);
  if (iter != stores.end()) {
    _return = iter->second;
    return true;
  } else {
    return false;
  }
}

void StoreConf::setParent(pStoreConf pParent) {
  parent = pParent;
}

void StoreConf::getAllStores(vector<pStoreConf>& _return) {
  for (store_conf_map_t::iterator iter = stores.begin(); iter != stores.end(); ++iter) {
    _return.push_back(iter->second);
  }
}

bool StoreConf::getInt(const string& intName, long int& _return) const {
  string str;
  if (getString(intName, str)) {
    _return = strtol(str.c_str(), NULL, 0);
    return true;
  } else {
    return false;
  }
}

bool StoreConf::getFloat(const std::string& floatName, float & _return) const {
  string str;
  if (getString(floatName, str)) {
    _return = strtof(str.c_str(), NULL);
    return true;
  } else {
    return false;
  }
}

bool StoreConf::getUnsigned(const string& intName,
                            unsigned long int& _return) const {
  string str;
  if (getString(intName, str)) {
    _return = strtoul(str.c_str(), NULL, 0);
    return true;
  } else {
    return false;
  }
}

bool StoreConf::getUnsignedLongLong(const string& llName,
                                    unsigned long long& _return) const {
  string str;
  if (getString(llName, str)) {
    _return = strtoull(str.c_str(), NULL, 10);
    return true;
  } else {
    return false;
  }
}

bool StoreConf::getString(const string& stringName,
                          string& _return) const {
  // allow parameter inheritance, i.e. if a named value is not found in the
  // current store's configuration, we keep looking up the current StoreConf's
  // ancestors until it is either found or we hit the root store.
  // To avoid ambiguity, when searching for named parameter in the ancestor store
  // we are looking for $type::$stringName where $type is the store type.
  // check the current store conf

  // first check the current store
  string_map_t::const_iterator iter = values.find(stringName);
  if (iter != values.end()) {
    _return = iter->second;
    return true;
  }

  // "category", "categories", "type" parameters can't be inherited
  string_map_t::const_iterator typeIter = values.find("type");
  string storeType = typeIter == values.end() ? "" : typeIter->second;
  if (storeType.empty()
      || stringName == "type"
      || stringName == "category"
      || stringName == "categories") {
    return false;
  }

  // not found
  bool found = false;
  string inheritedName = storeType + "::" + stringName;
  // searching for type::stringName start with the current configuration
  // this allows a parameter to be used by the current configuration
  // and descendant stores. E.g.
  // file::fs_type = std
  // can be used by this file store and all descendant file stores.
  for (const StoreConf* pconf = this; pconf;
        pconf = const_cast<StoreConf*>(pconf->parent.get())) {
    string_map_t::const_iterator iter = pconf->values.find(inheritedName);
    if (iter != pconf->values.end()) {
      _return = iter->second;
      found = true;
      break;
    }
  }
  // if we didn't find any.  then try g_Handler's config
  if (!found) {
    const StoreConf& gconf = g_Handler->getConfig();
    string_map_t::const_iterator iter = gconf.values.find(inheritedName);
    if (iter != gconf.values.end()) {
      _return = iter->second;
      found = true;
    }
  }
  return found;
}

void StoreConf::setString(const string& stringName, const string& value) {
  values[stringName] = value;
}

void StoreConf::setUnsigned(const string& stringName, unsigned long value) {
  ostringstream oss;
  oss << value;
  setString(stringName, oss.str());
}

void StoreConf::setUnsignedLongLong(const string& stringName, unsigned long long value) {
  ostringstream oss;
  oss << value;
  setString(stringName, oss.str());
}

// reads and parses the config data
void StoreConf::parseConfig(const string& filename) {

  queue<string> config_strings;

  if (readConfFile(filename, config_strings)) {
    LOG_OPER("got configuration data from file <%s>", filename.c_str());
  } else {
    ostringstream msg;
    msg << "Failed to open config file <" << filename << ">";
    throw runtime_error(msg.str());
  }

  parseStore(config_strings, this);
}

// Side-effects:  - removes items from raw_config and adds items to parsed_config
//
// Returns true if a valid entry was found
bool StoreConf::parseStore(queue<string>& raw_config, /*out*/ StoreConf* parsed_config) {

  int store_index = 0; // used to give things named "store" different names

  string line;
  while (!raw_config.empty()) {

    line = raw_config.front();
    raw_config.pop();

    // remove leading and trailing whitespace
    line = trimString(line);

    // remove comment
    size_t comment = line.find_first_of('#');
    if (comment != string::npos) {
      line.erase(comment);
    }

    int length = line.size();
    if (0 >= length) {
      continue;
    }
    if (line[0] == '<') {

      if (length > 1 && line[1] == '/') {
        // This is the end of the current store
        return true;
      }

      // This is the start of a new store
      string::size_type pos = line.find('>');
      if (pos == string::npos) {
        LOG_OPER("Bad config - line %s has a < but not a >", line.c_str());
        continue;
      }
      string store_name = line.substr(1, pos - 1);

      pStoreConf new_store(new StoreConf);
      if (parseStore(raw_config, new_store.get())) {
        if (0 == store_name.compare("store")) {
          // This is a special case for the top-level stores. They share
          // the same name, so we append an index to put them in the map
          ostringstream oss;
          oss << store_index;
          store_name += oss.str();
          ++store_index;
        }
        if (parsed_config->stores.find(store_name) != parsed_config->stores.end()) {
          LOG_OPER("Bad config - duplicate store name %s", store_name.c_str());
        }
        parsed_config->stores[store_name] = new_store;
      }
    } else {
      string::size_type eq = line.find('=');
      if (eq == string::npos) {
        LOG_OPER("Bad config - line %s is missing an =", line.c_str());
      } else {
        string arg = line.substr(0, eq);
        string val = line.substr(eq + 1, string::npos);

        // remove leading and trailing whitespace
        arg = trimString(arg);
        val = trimString(val);

        if (parsed_config->values.find(arg) != parsed_config->values.end()) {
          LOG_OPER("Bad config - duplicate key %s", arg.c_str());
        }
        parsed_config->values[arg] = val;
      }
    }
  }
  return true;
}

// trims leading and trailing whitespace from a string
string StoreConf::trimString(const string& str) {
  string whitespace = " \t";
  size_t start      = str.find_first_not_of(whitespace);
  size_t end        = str.find_last_not_of(whitespace);

  if (start != string::npos) {
    return str.substr(start, end - start + 1);
  } else {
    return "";
  }
}

// reads every line from the file and pushes then onto _return
// returns false on error
bool StoreConf::readConfFile(const string& filename, queue<string>& _return) {
  string line;
  ifstream config_file;

  config_file.open(filename.c_str());
  if (!config_file.good()) {
    return false;
  }

  while (getline(config_file, line)) {
    _return.push(line);
  }

  config_file.close();
  return true;
}

// serialize StoreConf
ostream& operator<<(ostream& os, const StoreConf& sconf) {
  return sconf.print(os, 0);
}

static string indent(uint32_t depth, bool useSpace, uint32_t tabw) {
  int len = useSpace ? depth * tabw : depth;
  return string(len, useSpace ? ' ' : '\t');
}

ostream& StoreConf::print(ostream& os, uint32_t depth,
                          bool useSpace, uint32_t tabw) const {
  // we only need to iterator through keys. as map guaranteed keys
  // are weakly ordered, so we will get consistent output.
  for (string_map_t::const_iterator iter = values.begin();
        iter != values.end(); iter++) {
    int len = useSpace ? depth * tabw : depth;
    os << indent(depth, useSpace, tabw) << iter->first
       << "=" << iter->second << endl;
  }
  // print out sub stores
  for (store_conf_map_t::const_iterator iter = stores.begin();
        iter != stores.end(); iter++) {
    os << indent(depth, useSpace, tabw) << "<" << iter->first << ">"
       << endl;
    iter->second->print(os, depth + 1, useSpace, tabw);
    os << indent(depth, useSpace, tabw) << "</" << iter->first << ">"
    << endl;
  }

  return os;
}
