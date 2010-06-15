<?php
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

require_once 'tests.php';
require_once 'testutil.php';

$success = true;

$pid_central = scribe_start('latency.central',
                            $GLOBALS['SCRIBE_BIN'],
                            $GLOBALS['SCRIBE_PORT'],
                            'scribe.conf.latency.central');

$pid_client = scribe_start('latency.client',
                            $GLOBALS['SCRIBE_BIN'],
                            $GLOBALS['SCRIBE_PORT'],
                            'scribe.conf.latency.client');

// write 10k messages to many different categories
$categories = array("hello", "foo", "food", "rock", "rockstar", "paper",
                    "scissors", "apple", "banana");
$client = "client1";

print("Testing writing 200k messages to multiple categories.\n");
super_stress_test($categories, $client, 10000, 200000, 20, 100, 1);

sleep(200);
// see counter values to inspect the result

if (!scribe_stop($GLOBALS['SCRIBE_CTRL'], $GLOBALS['SCRIBE_PORT'], $pid_central)
  | !scribe_stop($GLOBALS['SCRIBE_CTRL'], $GLOBALS['SCRIBE_PORT'], $pid_client))
{
  print("ERROR: could not stop scribe\n");
  return false;
}

return $success;
