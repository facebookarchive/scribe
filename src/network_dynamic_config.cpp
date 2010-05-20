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

#include "scribe/src/network_dynamic_config.h"
#include "scribe/src/dynamic_bucket_updater.h"

static NetworkDynamicConfigMod netConfigMods[] = {
  {
    "thrift_bucket",
    DynamicBucketUpdater::isConfigValid,
    DynamicBucketUpdater::getHost,
  },
  {
    "",
    NULL,
    NULL,
  },
};

NetworkDynamicConfigMod* getNetworkDynamicConfigMod(const char* name) {
  for (NetworkDynamicConfigMod *pconf = netConfigMods;
      pconf->isConfigValidFunc && pconf->getHostFunc;
      ++pconf) {
    if (strcmp(name, pconf->name) == 0) {
      return pconf;
    }
  }

  return NULL;
}

