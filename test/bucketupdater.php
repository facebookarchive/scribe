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

// testing bucket store updater.
// Setup:
// 1. 3 scribe servers.  one running on port 1465 and using conf file
//    scribe.conf.bucketupdater.server1, one running on port 1466
//    and using conf file scribe.conf.bucketupdater.server2, and the
//    last one running on port 1477 and using scribe.conf.bucketupdater.server.3.
//    Scribe server that configured by scribe.bucketupdater.server1
//    writes scribe messages to /tmp/scribetest_/bucketupdater/server1
//    Scribe server that configured by scribe.bucketupdater.server2
//    writes scribe messages to /tmp/scribetest_/bucketupdater/server2
//    Scribe server that configured by scribe.bucketupdater.server.3
//    writes scribe messages to /tmp/scribetest_/bucketupdater/server3
// 2. bidupdater server which implements bucket updater interface.
//    It reads mappings from a local file bidmap which is symbolic
//    linked to bidmap.1 or bidmap.2.  bidmap.1 maps bucket 1 to
//    scribe server that writes to /tmp/scribetest_/bucketupdater/server1,
//    bucket 2 to scribe server that writes to /tmp/scribetest_/bucketupdater/server2,
//    and bucket 3 to scribe server that writes to
//    /tmp/scribetest_bucketupdater/server3.
// 3. bidmap.2 maps bucket1 to server2 and bucket 2 and 3 to server2.
// 4. central server use connection pool.
//
// Test setup:
// 1. launch scribe -p 1465 scribe.conf.bucketupdater.srever1
// 2. launch scribe -p 1466 scribe.conf.bucketupdater.server2
// 3. launch scribe -p 1467 scribe.conf.bucketupdater.server3
// 3. ln -sf bidmap.1 bidmap.
// 4. launch bidupdater: bidupdater -p 9999 -f bidmap
// 5. scribe -p 1463 scribe.conf.bucketupdater which use bidupdater
//    to dynamically configure bucket store: bucketupdater.
// 6. send two message, one with bucket id 1 and the other with bucket id 2
//    to scribe running on 1463.  Check that the messages are
//    in /tmp/scribetest_/bucketupdater/server1/bucketupdater/bucket1
//    and /tmp/scribetest_/bucketupdater/server2/bucketupdater/bucket2
//    respectively.
// 7. ln -sf bidmap.2 bid
// 8. wait for 5 seconds
// 9. send another two messages with bucket id 1 and 2.
//    Check that message with bucket id 1 goes to server2, and bucket
//    2 and 3 ends in server1.
// 10. check conn pool works correctly by verifying that there is no
//    network connection from 1463 to 1467, i.e. server3 using
//    netstat -n | grep ESTABLISH | grep -q 9999
//    and check the return code is 1.

echo "Starting scribe server1: 1465, scribe.conf.bucketupdater.server1\n";
// start scribe server1
$pidScribe1 = scribe_start("bucketupdater.server1",
     $GLOBALS['SCRIBE_BIN'], 1465,
     "scribe.conf.bucketupdater.server1");

echo "Starting scribe server2: 1466, scribe.conf.bucketupdater.server2\n";
// start scribe server2
$pidScribe2 = scribe_start("bucketupdater.server2",
     $GLOBALS['SCRIBE_BIN'], 1466,
     "scribe.conf.bucketupdater.server2");

echo "Starting scribe server2: 1467, scribe.conf.bucketupdater.server2\n";
// start scribe server3
$pidScribe3 = scribe_start("bucketupdater.server3",
     $GLOBALS['SCRIBE_BIN'], 1467,
     "scribe.conf.bucketupdater.server3");

$cmd = "ln -sf bidmap.1 bidmap";
echo "$cmd\n";
// symlink
system($cmd);

// start bidupdaer
$cmd = "../../_bin/scribe/test/bucketupdater/bidupdater -p 9999 -f ./bidmap > bidupdater.out 2>&1 & echo $!";
echo "$cmd\n";
$pidUpdater = system($cmd);

echo "Starting scribe central server: 1463,scribe.conf.bucketupdater.central.\n";
// start scribe central server
$pidScribeCentral = scribe_start("bucketupdater.central",
      $GLOBALS['SCRIBE_BIN'], 1463,
      "scribe.conf.bucketupdater.central");

// test
$success1 = bucketupdater_test("server1/bucket001/content_current",
                              "server2/bucket002/content_current",
                              "server3/bucket002/content_current");

// change symlink
$cmd = "ln -sf bidmap.2 bidmap";
echo "$cmd\n";
system($cmd);

echo "sleep(15) to wait for bucket updater to take effect.\n";
sleep(15);

// test again
$success2 = bucketupdater_test("server2/bucket001/content_current",
                               "server1/bucket002/content_current",
                               "server1/bucket002/content_current");

// stop scribe server
scribe_stop($GLOBALS['SCRIBE_CTRL'], 1465, $pidScribe1);
scribe_stop($GLOBALS['SCRIBE_CTRL'], 1466, $pidScribe2);
scribe_stop($GLOBALS['SCRIBE_CTRL'], 1467, $pidScribe3);
scribe_stop($GLOBALS['SCRIBE_CTRL'], 1463, $pidScribeCentral);

// kill -9
$cmd = "kill -9 $pidUpdater\n";
echo "$cmd\n";
system($cmd);

return ($success1 && $success2);
