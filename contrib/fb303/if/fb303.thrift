/**
 * fb303.thrift
 *
 * Copyright (c) 2006- Facebook
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *
 * Definition of common Facebook data types and status reporting mechanisms
 * common to all Facebook services. In some cases, these methods are
 * provided in the base implementation, and in other cases they simply define
 * methods that inheriting applications should implement (i.e. status report)
 *
 * @author Mark Slee <mcslee@facebook.com>
 */

namespace java com.facebook.fb303
namespace cpp facebook.fb303

/**
 * Common status reporting mechanism across all services
 */
enum fb_status {
  DEAD = 0,
  STARTING = 1,
  ALIVE = 2,
  STOPPING = 3,
  STOPPED = 4,
  WARNING = 5,
}

/**
 * Standard base service
 */
service FacebookService {

  /**
   * Returns a descriptive name of the service
   */
  string getName(),

  /**
   * Returns the version of the service
   */
  string getVersion(),

  /**
   * Gets the status of this service
   */
  fb_status getStatus(),

  /**
   * User friendly description of status, such as why the service is in
   * the dead or warning state, or what is being started or stopped.
   */
  string getStatusDetails(),

  /**
   * Gets the counters for this service
   */
  map<string, i64> getCounters(),

  /**
   * Get counter values for a specific list of keys.  Returns a map from
   * key to counter value; if a requested counter doesn't exist, it won't
   * be in the returned map.
   */
  map<string, i64> getSelectedCounters(1: list<string> keys),

  /**
   * Gets the value of a single counter
   */
  i64 getCounter(1: string key),

  /**
   * Gets the exported string values for this service
   */
  map<string, string> getExportedValues(),

  /**
   * Get exported strings for a specific list of keys.  Returns a map from
   * key to string value; if a requested key doesn't exist, it won't
   * be in the returned map.
   */
  map<string, string> getSelectedExportedValues(1: list<string> keys),

  /**
   * Gets the value of a single exported string
   */
  string getExportedValue(1: string key),

  /**
   * Sets an option
   */
  void setOption(1: string key, 2: string value),

  /**
   * Gets an option
   */
  string getOption(1: string key),

  /**
   * Gets all options
   */
  map<string, string> getOptions(),

  /**
   * Returns a CPU profile over the given time interval (client and server
   * must agree on the profile format).
   */
//  string getCpuProfile(1: i32 profileDurationInSec),

  /**
   * Returns a heap profile over the given time interval (client and server
   * must agree on the profile format).
   */
//  string getHeapProfile(1: i32 profileDurationInSec),

  /**
   * Returns a synchronization primitive contention profile over the given time
   * interval (client and server must agree on the profile format).
   */
//  string getContentionProfile(1: i32 profileDurationInSec),

 /**
   * Returns a WallTime Profiler data over the given time
   * interval (client and server must agree on the profile format).
   */
//  string getWallTimeProfile(1: i32 profileDurationInSec),

  /**
   * Returns the current memory usage (RSS) of this process in bytes.
   */
//  i64 getMemoryUsage(),

  /**
   * Returns the 1-minute load average on the system (i.e. the load may not
   * all be coming from the current process).
   */
//  double getLoad(),

  /**
   * Returns the command line used to execute this process.
   */
//  string getCommandLine(),

  /**
   * Returns the unix time that the server has been running since
   */
  i64 aliveSince(),

  /**
   * Tell the server to reload its configuration, reopen log files, etc
   */
  oneway void reinitialize(),

  /**
   * Suggest a shutdown to the server
   */
  oneway void shutdown(),

  /**
   * Translate frame pointers to file name and line pairs.
   */
//  list<string> translateFrames(1: list<i64> pointers),
}
