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

#ifndef SCRIBE_ENV
#define SCRIBE_ENV

namespace scribe {

const char* const kScribeVersion           = "2.2";
const char* const kDefaultConfFileLocation = "/usr/local/scribe/scribe.conf";

/*
 * This file contains methods for handling tasks that depend
 * on the environment in which this process is running.
 * i.e. using scribe will not force the adoption of a particular
 * system for monitoring or configuring services over the network.
 */

/*
 * Debug logging
 */
#ifndef LOG_OPER
#define LOG_OPER(format_string,...)                                     \
  {                                                                     \
    time_t now;                                                         \
    char dbgtime[26] ;                                                  \
    time(&now);                                                         \
    ctime_r(&now, dbgtime);                                             \
    dbgtime[24] = '\0';                                                 \
    fprintf(stderr,"[%s] " #format_string " \n", dbgtime,##__VA_ARGS__); \
  }
#endif


/*
 * Network based configuration and directory service
 */

namespace network_config {
  // gets a vector of machine/port pairs for a named service
  // returns true on success
  bool getService(const string& serviceName,
                         const string& options,
                         ServerVector* servers);

} // !namespace scribe::network_config

/*
 * Concurrency mechanisms
 */

namespace concurrency {
  // returns a new instance of read/write mutex.
  // you can choose different implementations based on your needs.
  shared_ptr<ReadWriteMutex> createReadWriteMutex();

} // !namespace scribe::concurrency

/*
 * Time functions
 */

namespace clock {
  unsigned long nowInMsec();

} // !namespace scribe::clock

/*
 * Hash functions
 */

// You can probably find better hash functions than these
class integerhash {
 public:
  size_t operator()(const uint32_t x) const;

  static uint32_t hash32(uint32_t key);
};

class strhash {
 public:
  // hash operator
  size_t operator()(const string &x) const;

  size_t operator()(const char *x) const;

  static uint32_t hash32(const char *s);
};

/*
 * Starting a scribe server.
 */
void startServer();

/*
 * Stopping a scribe server.
 */
void stopServer();

} // !namespace scribe

#endif //! SCRIBE_ENV
