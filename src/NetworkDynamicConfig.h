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
// @author John Song

#ifndef SCRIBE_NETWORK_DYNAMIC_CONFIG_H
#define SCRIBE_NETWORK_DYNAMIC_CONFIG_H

#include "Conf.h"

namespace scribe {

// functional types for network dynamic updater validation and getHost calls
typedef bool (*NetworkIsConfigValidFunc)(const string& category,
                                         const StoreConf* pconf);
typedef bool (*NetworkGetHost)(const string& category,
                               const StoreConf* pconf,
                               string& host,
                               uint32_t& port);

struct NetworkDynamicConfigMod {
  const char* name;
  NetworkIsConfigValidFunc isConfigValidFunc;
  NetworkGetHost getHostFunc;
};

NetworkDynamicConfigMod* getNetworkDynamicConfigMod(const char* name);

} //! namespace scribe

#endif //! SCRIBE_NETWORK_DYNAMIC_CONFIG_H
