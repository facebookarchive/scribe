#!/usr/local/bin/thrift --gen cpp:pure_enums --gen php

##  Copyright (c) 2009- Facebook
##
##  Licensed under the Apache License, Version 2.0 (the "License");
##  you may not use this file except in compliance with the License.
##  You may obtain a copy of the License at
##
##      http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##
## See accompanying file LICENSE or visit the Scribe site at:
## http://developers.facebook.com/scribe/

namespace cpp scribe.thrift
namespace java com.facebook.infrastructure.service

// BucketStoreMapping service exception
exception BucketStoreMappingException {
  1: string message;
  2: i32 code;
}

struct HostPort {
  2: string host,
  3: i32 port
}
 
service BucketStoreMapping {
  // given a category, return a list of HashCodeToNetworkStore mappings
  map<i32, HostPort> getMapping(1: string category) throws (1: BucketStoreMappingException e);
}
