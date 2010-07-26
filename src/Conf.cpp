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
#include "Common.h"
#include "Conf.h"
#include "ScribeServer.h"

using namespace boost;
using namespace std;

namespace scribe {

StoreConf::StoreConf() {
}

StoreConf::~StoreConf() {
}

bool StoreConf::getStore(const string& storeName, StoreConfPtr* value) {
  StoreConfMap::iterator iter = stores_.find(storeName);
  if (iter != stores_.end()) {
    *value = iter->second;
    return true;
  } else {
    return false;
  }
}

void StoreConf::setParent(StoreConfPtr parent) {
  parent_ = parent;
}

void StoreConf::getAllStores(vector<StoreConfPtr>* value) {
  for (StoreConfMap::iterator iter = stores_.begin();
       iter != stores_.end();
       ++iter) {
    value->push_back(iter->second);
  }
}

bool StoreConf::getInt(const string& intName, long* value) const {
  string str;
  if (getString(intName, &str)) {
    *value = lexical_cast<long>(str);
    return true;
  } else {
    return false;
  }
}

bool StoreConf::getFloat(const string& floatName, float* value) const {
  string str;
  if (getString(floatName, &str)) {
    *value = lexical_cast<float>(str);
    return true;
  } else {
    return false;
  }
}

bool StoreConf::getUnsigned(const string& intName, unsigned long* value) const {
  string str;
  if (getString(intName, &str)) {
    *value = lexical_cast<unsigned long>(str);
    return true;
  } else {
    return false;
  }
}

bool StoreConf::getUint64(const string& intName, uint64_t* value) const {
  string str;
  if (getString(intName, &str)) {
    *value = lexical_cast<uint64_t>(str);
    return true;
  } else {
    return false;
  }
}

bool StoreConf::getString(const string& stringName, string* value) const {
  // allow parameter inheritance, i.e. if a named value is not found in the
  // current store's configuration, we keep looking up the current StoreConf's
  // ancestors until it is either found or we hit the root store.
  // To avoid ambiguity, when searching for named parameter in the ancestor
  // store we are looking for $type::$stringName where $type is the store type.
  // check the current store conf

  // first check the current store
  StringMap::const_iterator iter = values_.find(stringName);
  if (iter != values_.end()) {
    *value = iter->second;
    return true;
  }

  // "category", "categories", "type" parameters can't be inherited
  StringMap::const_iterator typeIt = values_.find("type");
  string storeType = (typeIt == values_.end() ? "" : typeIt->second);
  if (storeType.empty() ||
      stringName == "type" ||
      stringName == "category" ||
      stringName == "categories") {
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
        pconf = const_cast<StoreConf*>(pconf->parent_.get())) {
    StringMap::const_iterator iter = pconf->values_.find(inheritedName);
    if (iter != pconf->values_.end()) {
      *value = iter->second;
      found = true;
      break;
    }
  }
  // if we didn't find any.  then try g_handler's config
  if (!found) {
    const StoreConf& gconf = g_handler->getConfig();
    StringMap::const_iterator iter = gconf.values_.find(inheritedName);
    if (iter != gconf.values_.end()) {
      *value = iter->second;
      found = true;
    }
  }
  return found;
}

void StoreConf::setString(const string& stringName, const string& value) {
  values_[stringName] = value;
}

void StoreConf::setUnsigned(const string& stringName, unsigned long value) {
  ostringstream oss;
  oss << value;
  setString(stringName, oss.str());
}

void StoreConf::setUint64(const string& stringName, uint64_t value) {
  ostringstream oss;
  oss << value;
  setString(stringName, oss.str());
}

// reads and parses the config data
void StoreConf::parseConfig(const string& filename) {

  queue<string> configStrings;

  if (readConfFile(filename, &configStrings)) {
    LOG_OPER("got configuration data from file <%s>", filename.c_str());
  } else {
    ostringstream msg;
    msg << "Failed to open config file <" << filename << ">";
    throw std::runtime_error(msg.str());
  }

  parseStore(configStrings, this);
}

// Side-effects:  - removes items from rawConfig and adds items to parsedConfig
//
// Returns true if a valid entry was found
bool StoreConf::parseStore(queue<string>& rawConfig,
                           /*out*/ StoreConf* parsedConfig) {

  int storeIndex = 0; // used to give things named "store" different names

  string line;
  while (!rawConfig.empty()) {

    line = rawConfig.front();
    rawConfig.pop();

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
      string storeName = line.substr(1, pos - 1);

      StoreConfPtr newStore(new StoreConf);
      if (parseStore(rawConfig, newStore.get())) {
        if (0 == storeName.compare("store")) {
          // This is a special case for the top-level stores. They share
          // the same name, so we append an index to put them in the map
          ostringstream oss;
          oss << storeIndex;
          storeName += oss.str();
          ++storeIndex;
        }
        StoreConfMap& stores = parsedConfig->stores_;
        if (stores.find(storeName) != stores.end()) {
          LOG_OPER("Bad config - duplicate store name %s", storeName.c_str());
        }
        stores[storeName] = newStore;
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

        if (parsedConfig->values_.find(arg) != parsedConfig->values_.end()) {
          LOG_OPER("Bad config - duplicate key %s", arg.c_str());
        }
        parsedConfig->values_[arg] = val;
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
bool StoreConf::readConfFile(const string& filename, queue<string>* contents) {
  string line;
  ifstream configFile;

  configFile.open(filename.c_str());
  if (!configFile.good()) {
    return false;
  }

  while (getline(configFile, line)) {
    contents->push(line);
  }

  configFile.close();
  return true;
}

// serialize StoreConf
ostream& operator<<(ostream& os, const StoreConf& storeConf) {
  return storeConf.print(os, 0);
}

static string indent(uint32_t depth, bool useSpace, uint32_t tabWidth) {
  int len = useSpace ? depth * tabWidth : depth;
  return string(len, useSpace ? ' ' : '\t');
}

ostream& StoreConf::print(ostream& os, uint32_t depth,
                          bool useSpace, uint32_t tabWidth) const {
  // we only need to iterator through keys. as map guaranteed keys
  // are weakly ordered, so we will get consistent output.
  for (StringMap::const_iterator iter = values_.begin();
        iter != values_.end(); iter++) {
    os << indent(depth, useSpace, tabWidth) << iter->first
       << "=" << iter->second << endl;
  }
  // print out sub stores
  for (StoreConfMap::const_iterator iter = stores_.begin();
        iter != stores_.end(); iter++) {
    os << indent(depth, useSpace, tabWidth) << "<" << iter->first << ">"
       << endl;
    iter->second->print(os, depth + 1, useSpace, tabWidth);
    os << indent(depth, useSpace, tabWidth) << "</" << iter->first << ">"
       << endl;
  }

  return os;
}

} //! namespace scribe
