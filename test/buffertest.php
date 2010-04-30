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

include_once 'tests.php';
include_once 'testutil.php';

$success = true;

$pid = scribe_start('buffertest', $GLOBALS['SCRIBE_BIN'],
                    $GLOBALS['SCRIBE_PORT'], 'scribe.conf.buffertest');

// delete scribetest_ directory, and try to log messages there.
// Then recreate directory and verify that buffers get written
system("rm -rf /tmp/scribetest_", $error);
if ($error) {
  print("ERROR: unable to delete /tmp/scribetest_\n");
}


// write 10k messages to category test (handled by default store)
print("test writing 10k messages to category test\n");
stress_test('test', 'client1', 1000, 10000, 20, 100, 1);

// write another 10k messages to category test (should see 1 out of order)
sleep(2);
print("test writing another 10k messages (will see 1 out of order)\n");
stress_test('test', 'client1', 1000, 10000, 20, 100, 1);

// write 200k messages to category test using different client name
print("test writing 200k more messages to category test\n");
stress_test('test', 'client2', 10000, 200000, 50, 100, 1);

// write 10k messages to category foodoo (handled by prefix store)
print("test writing 10k messages to category foodoo\n");
stress_test('foodoo', 'client1', 10000, 10000, 20, 100, 1);


// write 10k messages to category rock (handled by categories prefix store)
print("test writing 100k messages to category rock\n");
stress_test('rock', 'client1', 100, 10000, 20, 100, 1);

// re-create primary store path
system("mkdir /tmp/scribetest_", $error);
if ($error) {
  print("ERROR: unable to recreate /tmp/scribetest_\n");
}


// sleep for a while to wait for buffers to flush
print("Waiting for buffers to flush...\n");
sleep(120);

// verify all messages got logged
$results = resultChecker('/tmp/scribetest_/test', 'test-', 'client1');

if ($results["count"] != 20000 || $results["out_of_order"] != 1) {
  $success = false;
}

$results = resultChecker('/tmp/scribetest_/test', 'test-', 'client2');

if ($results["count"] != 200000 || $results["out_of_order"] != 0) {
  $success = false;
}

$results = resultChecker('/tmp/scribetest_/foodoo', 'foodoo-', 'client1');

if ($results["count"] != 10000 || $results["out_of_order"] != 0) {
  $success = false;
}

$results = resultChecker('/tmp/scribetest_/rock', 'rock-', 'client1');

if ($results["count"] != 10000 || $results["out_of_order"] != 0) {
  $success = false;
}


if (!scribe_stop($GLOBALS['SCRIBE_CTRL'], $GLOBALS['SCRIBE_PORT'], $pid)) {
  print("ERROR: could not stop scribe\n");
  return false;
}

return $success;
