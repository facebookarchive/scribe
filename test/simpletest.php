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

// run some simpletests
// -verify that we can send a message
// -send a message with non-printable characters
// -send some strange input messages

function simple_test() {
  $messages = array();
  $msg = new LogEntry;
  $msg->category = 'scribe_test';
  $msg->message = "this is a message\n";
  $messages []= $msg;
  $msg2 = new LogEntry;
  $msg2->category = 'scribe_test';
  $msg2->message = "and a binary" . chr(0) . chr(1) . " message\n";
  $messages []= $msg2;
  $msg3 = new LogEntry;
  $msg3->category = 'buckettest';
  $msg3->message = '99' . chr(1) . 'a key-value message with a non-printable delimiter\n';
  $messages []= $msg3;
  $msg4 = new LogEntry;
  $msg4->category = 'buckettest';
  $msg4->message = '99' . chr(1) . 'a different message in the same bucket\n';
  $messages []= $msg4;
  $msg5 = new LogEntry;
  $msg5->category = 'buckettest';
  $msg5->message = '98' . chr(1) . 'a different bucket\n';
  $messages []= $msg5;

  $scribe_client = create_scribe_client();
  $ret = scribe_Log_test($messages, $scribe_client);

  print "Log returned: " . $ret . "\n";
}


$success = true;

$pid = scribe_start('simpletest', $GLOBALS['SCRIBE_BIN'],
                    $GLOBALS['SCRIBE_PORT'], 'scribe.conf.simpletest');

print("running strange input test\n");
strange_input_test();
sleep(2);

print("running some simple tests\n");
simple_test();

// wait for messages to arrive
sleep(5);

// check results
$file = fopen("/tmp/scribetest_/scribe_test/scribe_test_current", 'r');

if ($file) {

  // strange_input_test should end up writing '\n'
  $line = fgets($file, 3);

  if ($line != '\n') {
    print("ERROR: Did not find first message\n");
    $success = false;
  }

  // simple_test should have written 2 more lines
  $line = fgets($file);

  if ($line != "this is a message\n") {
    print("ERROR: Did not find first simple_test line\n");
    $success = false;
  }

  $line = fgets($file);

  if ($line != ("and a binary" . chr(0) . chr(1) . " message\n")) {
    print("ERROR: Did not find second simpe_test line\n");
    $success = false;
  }

} else {
  print("ERROR: could not open result file: $file\n");
  $success = false;
}


if (!scribe_stop($GLOBALS['SCRIBE_CTRL'], $GLOBALS['SCRIBE_PORT'], $pid)) {
  print("ERROR: could not stop scribe\n");
  return false;
}

return $success;
