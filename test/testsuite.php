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

// This script writes to /tmp/scribetest_/ and /tmp/scribe_test_/
//TODO: warning!

///////////////////////////////////////////////////////////////////////////////
// Change these paths to the location of your scribed binary and
// scribe_ctrl script
$GLOBALS['SCRIBE_BIN']  = '../src';
$GLOBALS['SCRIBE_CTRL'] = '../examples';
//
///////////////////////////////////////////////////////////////////////////////


// In order to change these port number, you would also need to edit
// the scribe.conf.* configs
$GLOBALS['SCRIBE_PORT'] = 1463;
$GLOBALS['SCRIBE_PORT2'] = 1466;

$ALL_TESTS = array(
  'simpletest',
  'basictest',
  'basictest2',
  'buffertest',
  'buffertest2',
//  'categoriestest',
  'paramtest',
  'twodefaulttest',
  //'reloadtest',
  //'latencytest',
);

$output_file = null;

function usage() {
  print("Usage (must be run as root):

  DO NOT RUN ON ANY MACHINE THAT IS ALREADY RUNNING SCRIBE!

    Run all tests:
      php testsuite.php
    Run specific test:
      php testsuite.php [test name]*
  ");
}

function write_callback($buffer) {
  global $output_file;

  $success = fwrite($output_file, $buffer);

  if (!$success) {
    return "ERROR: could not write test output\n";
  }
}

function clean_test_dirs() {
  system("rm -rf /tmp/scribetest_ /tmp/scribe_test_");
  system("mkdir /tmp/scribetest_ /tmp/scribe_test_", $error);

  if ($error) {
    print("ERROR:  Could not mkdir /tmp/scribetest_ /tmp/scribe_test_\n");
  }
}

function run_test($testname) {
  global $output_file;

  print("Running test $testname ... ");

  try {
    clean_test_dirs();

    // redirect output for each test to its own file
    $filename = "scribe.test." . $testname;
    $output_file = fopen($filename, 'w');

    if (!$output_file) {
      print("ERROR: could not open test output file: $output_file \n");
      return false;
    }

    ob_start("write_callback");

    //run test
    $success = include($testname . ".php");

    ob_end_flush();
    fclose($output_file);
    $output_file = null;

    if ($success) {
      print("succeeded.\n");
    } else {
      print("FAILED.  see test output.\n");
    }

  } catch (Exception $e) {
    print("EXCEPTION: " . $e->getMessage() . "\n");
    $success = false;
  }

  return $success;
}

$succeeded = 0;
$failed = 0;

if ($argc > 1) {
  if ($argv[1] == "-h" || $argv[1] == "help") {
    usage();
    return false;
  }

  // run only tests specified
  $tests = array_splice($argv, 1);
  foreach ($tests as $testname) {
    if (run_test($testname)) {
      $succeeded++;
    } else {
      $failed++;
    }
  }
} else {
  foreach ($ALL_TESTS as $testname) {
    if (run_test($testname)) {
      $succeeded++;
    } else {
      $failed++;
    }
  }
}

print("$succeeded tests passed, $failed tests failed.\n");

if ($succeeded && !$failed) {
  print("SUCCESS\n");
  return true;
} else {
  print("FAILED\n");
  return false;
}
