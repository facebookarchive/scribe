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

#include "Common.h"

namespace scribe {

/*
 * This class reads and parses a configuration
 * describing a hierarchy of store objects.
 *
 * It reads a conf file with a proprietary format, although it could
 * be changed to xml (or anything else that supports hierarchy) by only
 * changing the code in this class.
 */
class StoreConf;
typedef shared_ptr<StoreConf>      StoreConfPtr;
typedef map<string, string>        StringMap;
typedef map<string, StoreConfPtr>  StoreConfMap;

std::ostream& operator<<(std::ostream& os, const StoreConf& storeConf);

class StoreConf {
 friend std::ostream& operator<<(std::ostream& os, const StoreConf& storeConf);
 public:
  StoreConf();
  virtual ~StoreConf();

  // Return value is true if the key exists, and false if it doesn't.
  // This doesn't check for garbage ints or empty strings.
  // The return parameter is untouched if the key isn't found.
  void getAllStores(vector<StoreConfPtr>* value);
  bool getStore(const string& storeName, StoreConfPtr* value);
  bool getInt(const string& intName, long* value) const;
  bool getUnsigned(const string& intName, unsigned long* value) const;
  bool getUint64(const string& intName, uint64_t* value) const;
  bool getFloat(const string& floatName, float* value) const;
  bool getString(const string& stringName, string* value) const;

  void setString(const string& stringName, const string& value);
  void setUnsigned(const string& intName, unsigned long value);
  void setUint64(const string& intName, uint64_t value);

  // Reads configuration from a file and throws an exception if it fails.
  void parseConfig(const string& filename);
  void setParent(StoreConfPtr parent);
 private:
  StringMap values_;
  StoreConfMap stores_;
  StoreConfPtr parent_;
  static bool parseStore(/*in,out*/ queue<string>& rawConfig,
                         /*out*/ StoreConf* parsedConfig);
  static string trimString(const string& str);
  bool readConfFile(const string& filename, queue<string>* contents);
  std::ostream& print(std::ostream& os, uint32_t depth, bool useSpace = true,
                      uint32_t tabWidth = 2) const;
};

} //! namespace scribe

#endif //! SCRIBE_CONF_H
