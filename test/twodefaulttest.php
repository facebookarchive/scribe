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


// very similar to basictestst. Infact it does every test that basictest does
// but does them twice - once for each place where default category gets logged.
// The scribe.conf.twodefaulttest will show that the default category is being
// logged into two filestores simultaneously

require_once 'tests.php';
require_once 'testutil.php';

$success = true;

$pid = scribe_start('twodefaulttest', $GLOBALS['SCRIBE_BIN'],
                    $GLOBALS['SCRIBE_PORT'],
                    'scribe.conf.twodefaulttest');

// write 10k messages to category test (handled by default store)
print("test writing 10k messages to category test\n");
stress_test('test', 'client1', 1000, 10000, 20, 100, 1);
sleep(1);
$results = resultChecker('/tmp/scribetest_/test', 'test-', 'client1');

if ($results["count"] != 10000 || $results["out_of_order"] != 0) {
  $success = false;
  print_r($results);
}
$results = resultChecker('/tmp/scribe_test_/test', 'test-', 'client1');

if ($results["count"] != 10000 || $results["out_of_order"] != 0) {
  $success = false;
  print_r($results);
}



// write another 10k messages to category test (should see 1 out of order)
print("test writing another 10k messages (will see 1 out of order)\n");
stress_test('test', 'client1', 1000, 10000, 20, 100, 1);
sleep(1);
$results = resultChecker('/tmp/scribetest_/test', 'test-', 'client1');

if ($results["count"] != 20000 || $results["out_of_order"] != 1) {
  $success = false;
}
$results = resultChecker('/tmp/scribe_test_/test', 'test-', 'client1');

if ($results["count"] != 20000 || $results["out_of_order"] != 1) {
  $success = false;
}

// write 200k messages to category test using different client name
print("test writing 200k more messages to category test\n");
stress_test('test', 'client2', 10000, 200000, 50, 100, 1);
sleep(1);
$results = resultChecker('/tmp/scribetest_/test', 'test-', 'client2');

if ($results["count"] != 200000 || $results["out_of_order"] != 0) {
  $success = false;
}
$results = resultChecker('/tmp/scribe_test_/test', 'test-', 'client2');

if ($results["count"] != 200000 || $results["out_of_order"] != 0) {
  $success = false;
}

// write 10k messages to category tps (handled by named store)
print("test writing 10k messages to category tps\n");
stress_test('tps', 'client1', 1000, 10000, 200, 100, 1);
sleep(1);
$results = resultChecker('/tmp/scribetest_/tps', 'tps-', 'client1');

if ($results["count"] != 10000 || $results["out_of_order"] != 0) {
  $success = false;
}
$results = resultChecker('/tmp/scribe_test_/tps', 'tps-', 'client1');

if ($results["count"] != 10000 || $results["out_of_order"] != 0) {
  $success = false;
}

// write 10k messages to category foodoo (handled by prefix store)
print("test writing 10k messages to category foodoo\n");
stress_test('foodoo', 'client1', 10000, 10000, 20, 100, 1);
sleep(1);
$results = resultChecker('/tmp/scribetest_/foodoo', 'foodoo-', 'client1');

if ($results["count"] != 10000 || $results["out_of_order"] != 0) {
  $success = false;
}
$results = resultChecker('/tmp/scribe_test_/foodoo', 'foodoo-', 'client1');

if ($results["count"] != 10000 || $results["out_of_order"] != 0) {
  $success = false;
}

// write 10k messages to category rock (handled by categories prefix store)
print("test writing 100k messages to category rock\n");
stress_test('rock', 'client1', 100, 10000, 20, 100, 1);
sleep(1);
$results = resultChecker('/tmp/scribetest_/rock', 'rock-', 'client1');

if ($results["count"] != 10000 || $results["out_of_order"] != 0) {
  $success = false;
}
$results = resultChecker('/tmp/scribe_test_/rock', 'rock-', 'client1');

if ($results["count"] != 10000 || $results["out_of_order"] != 0) {
  $success = false;
}

// write 10k messages to category paper (handled by categories store)
print("test writing 10k messages to category paper\n");
stress_test('paper', 'client1', 1000, 10000, 20, 500, 1);
sleep(1);
$results = resultChecker('/tmp/scribetest_/paper', 'paper-', 'client1');

if ($results["count"] != 10000 || $results["out_of_order"] != 0) {
  $success = false;
}
$results = resultChecker('/tmp/scribe_test_/paper', 'paper-', 'client1');

if ($results["count"] != 10000 || $results["out_of_order"] != 0) {
  $success = false;
}

if (!scribe_stop($GLOBALS['SCRIBE_CTRL'], $GLOBALS['SCRIBE_PORT'], $pid)) {
  print("ERROR: could not stop scribe\n");
  $success = false;
}

return $success;
