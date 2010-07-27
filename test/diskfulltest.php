<?php
//  Copyright (c) 2007-2010 Facebook
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

// this test verifies the following process:
// 1) scribe is running in central-client style (two scribes)
// 2) the central scribe goes down
// 3) buffer files in client scribe pile up until diskfull
// 4) central scribe comes up
// 5) buffer files should be flushed to central scribe without jamming

require_once 'tests.php';
require_once 'testutil.php';

function set_up() {
  system('mkdir -p /tmp/scribetest_/central');
  system('mkdir -p /tmp/scribetest_/client');
  system('mount -t tmpfs -o size=16m tmpfs /tmp/scribetest_/client');
}

function tear_down() {
  system('umount /tmp/scribetest_/client');
  system('rm -rf /tmp/scribetest_/client');
  system('rm -rf /tmp/scribetest_/central');
}

set_up();

$success = true;

$pid1 = scribe_start('diskfulltest.client', $GLOBALS['SCRIBE_BIN'],
                     $GLOBALS['SCRIBE_PORT'], 'scribe.conf.diskfull.client');

print("running stress test\n");
$msg_sent = stress_test('test', 'client1', 1000, 200000, 20, 100, 1);

sleep(2); // it should begin to complain errors

$pid2 = scribe_start('diskfulltest.central', $GLOBALS['SCRIBE_BIN'],
                     $GLOBALS['SCRIBE_PORT2'], 'scribe.conf.diskfull.central');

// wait for messages to arrive
sleep(30);

// we would get duplicate messages
uniqueFiles('/tmp/scribetest_/central/test', 'test_');

// check results
$results = resultChecker('/tmp/scribetest_/central/test', 'test_', 'client1');

if ($results["count"] < $msg_sent) {
  $success = false;
}

$results = resultChecker('/tmp/scribetest_/client/test', 'test_', 'client1');

if ($results["count"] != 0 || $results["out_of_order"] != 0) {
  $success = false;
}

if (!scribe_stop($GLOBALS['SCRIBE_CTRL'], $GLOBALS['SCRIBE_PORT'], $pid1)) {
  print("ERROR: could not stop client scribe\n");
  $success = false;
}

if (!scribe_stop($GLOBALS['SCRIBE_CTRL'], $GLOBALS['SCRIBE_PORT2'], $pid2)) {
  print("ERROR: could not stop central scribe\n");
  $success = false;
}

tear_down();

return $success;
