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

if ($argc > 1) {
  $client = $argv[1];
} else {
  $client = 'client1';
}

if ($argc > 2) {

  for($i = 0; $i < $argc - 2; ++$i) {
    $categories[$i] = $argv[$i+2];
  }

} else {
  $categories =  array("rock", "paper", "scissors");
}

print "starting test...\n";

super_stress_test($categories, $client, 10000, 200000, 20, 100, 1);

print "done\n";

?>
