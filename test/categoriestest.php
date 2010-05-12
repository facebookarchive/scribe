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

$pid = scribe_start('categoriestest', $GLOBALS['SCRIBE_BIN'],
                    $GLOBALS['SCRIBE_PORT'], 'scribe.conf.categoriestest');

// write 10k messages to many different categories
$categories = array("hello", "foo", "food", "rock", "rockstar", "paper",
                    "scissors", "apple", "banana");
$client = "client1";

print("Testing writing 100k messages to multiple categories.\n");
super_stress_test($categories, $client, 1000, 100000, 20, 100, 1);

sleep(10);

// verify all messages got written
foreach ($categories as $category) {
  $path = "/tmp/scribetest_/$category";
  $prefix = $category . '-';

  print("Verifying messages for category $category.\n");
  $results = resultChecker($path, $prefix, $client);

  if ($results["count"] != 100000 || $results["out_of_order"] != 0) {
    print("Error:Bad results for category $category\n");
    $success = false;
  }
}


if (!scribe_stop($GLOBALS['SCRIBE_CTRL'], $GLOBALS['SCRIBE_PORT'], $pid)) {
  print("ERROR: could not stop scribe\n");
  $success = false;
}

return $success;
