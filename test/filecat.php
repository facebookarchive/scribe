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

/* File cat test
 * Read file, $file line by line send to scribe at localhost:1463
 * with a category name = $category each line becomes a message.
 * messages are sent at a rate of $rate messages per second
 */

require_once 'tests.php';

function printUsage() {
  echo "Usage $arg[0] pathtofile [categoryname]";
}

if ($argc > 2) {
  $file = $argv[1];
  $category = $argv[2];
} else if ($argc > 1) {
  $file = $argv[1];
  $category = "filecattest";
} else {
  printUsage();
}

print 'starting test...';
$rate = 10000;
$msg_per_call = 20;
file_cat($file, $category, $rate, $msg_per_call);
print 'done';

?>
