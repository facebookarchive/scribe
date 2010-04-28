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

#ifndef SCRIBE_CONF_H
#define SCRIBE_CONF_H

#include <string>
#include <vector>
#include <queue>
#include <iostream>
#include <fstream>

#include "src/gen-cpp/scribe.h"

/*
 * This class reads and parses a configuration
 * describing a hierarchy of store objects.
 *
 * It reads a conf file with a proprietary format, although it could
 * be changed to xml (or anything else that supports hierarchy) by only
 * changing the code in this class.
 */
class StoreConf;
typedef boost::shared_ptr<StoreConf> pStoreConf;
typedef std::map<std::string, std::string> string_map_t;
typedef std::map<std::string, pStoreConf> store_conf_map_t;

class StoreConf {

 public:
  StoreConf();
  virtual ~StoreConf();

  // Return value is true if the key exists, and false if it doesn't.
  // This doesn't check for garbage ints or empty strings.
  // The return parameter is untouched if the key isn't found.
  void getAllStores(std::vector<pStoreConf>& _return);
  bool getStore(const std::string& storeName, pStoreConf& _return);
  bool getInt(const std::string& intName, long int& _return);
  bool getUnsigned(const std::string& intName, unsigned long int& _return);
  bool getUnsignedLongLong(const std::string& intName, unsigned long long& _return);
  bool getString(const std::string& stringName, std::string& _return);

  void setString(const std::string& stringName, const std::string& value);
  void setUnsigned(const std::string& intName, unsigned long value);
  void setUnsignedLongLong(const std::string& intName, unsigned long long value);

  // Reads configuration from a file and throws an exception if it fails.
  void parseConfig(const std::string& filename);

 private:
  string_map_t values;
  store_conf_map_t stores;

  static bool parseStore(/*in,out*/ std::queue<std::string>& raw_config,
                         /*out*/ StoreConf* parsed_config);
  static std::string trimString(const std::string& str);
  bool readConfFile(const std::string& filename, std::queue<std::string>& _return);
};

#endif //!defined SCRIBE_CONF_H
