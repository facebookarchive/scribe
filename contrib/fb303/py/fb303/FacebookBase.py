#!/usr/bin/env python
#
# Copyright (c) 2006- Facebook
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import sys
import time
import FacebookService
from ttypes import fb_status

class FacebookBase(FacebookService.Iface):

  def __init__(self, name):
    self.name = name
    self.alive = int(time.time())
    self.counters = {}
    self.exportedValues = {}

  def getName(self, ):
    return self.name

  def getVersion(self, ):
    return ''

  def getStatus(self, ):
    return fb_status.ALIVE

  def getStatusDetails(self):
    return ''

  def getCounters(self):
    return self.counters

  def resetCounter(self, key):
    self.counters[key] = 0

  def getCounter(self, key):
    if key in self.counters:
      return self.counters[key]
    return 0

  def getSelectedCounters(self, keys):
    ret = {}
    for key in keys:
      if key in self.counters:
        ret[key] = self.counters[key]
    return ret

  def incrementCounter(self, key):
    self.counters[key] = self.getCounter(key) + 1

  def getExportedValues(self):
    return self.exportedValues

  def getExportedValue(self, key):
    if key in self.exportedValues:
      return self.exportedValues[key]
    return ""

  def getSelectedExportedValues(self, keys):
    ret = {}
    for key in keys:
      if key in self.exportedValues:
        ret[key] = self.exportedValues[key]
    return ret

  def setOption(self, key, value):
    pass

  def getOption(self, key):
    return ""

  def getOptions(self):
    return {}

  def aliveSince(self):
    return self.alive

  def reinitialize(self):
    pass

  def shutdown(self):
    pass
