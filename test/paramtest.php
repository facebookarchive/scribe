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

// param test.  send messages to category paramtest
// and verify that message is found in the file

$success = true;
$pid = scribe_start('paramtest', $GLOBALS['SCRIBE_BIN'],
                    $GLOBALS['SCRIBE_PORT'], 'scribe.conf.paramtest');

print("running param inheritance test\n");
param_test();
sleep(2);

// check results
$file = "/tmp/scribetest_/paramtest/primary_current";
$cmd = "grep \"paramtest\" $file > /dev/null 2>&1; echo $?";
echo "checking message: $cmd\n";
system($cmd, $ret);
if ($ret != 0) {
  print("ERROR: didn't find message\"paramtest\" in file $file\n");
  $success = false;
}

if (!scribe_stop($GLOBALS['SCRIBE_CTRL'], $GLOBALS['SCRIBE_PORT'], $pid)) {
  print("ERROR: could not stop scribe\n");
  return false;
}

return $success;
