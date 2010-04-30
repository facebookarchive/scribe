<?php
//  Copyright (c) 2009- Facebook
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


/**
 *  Starts scribe in a new process using the config file for the given test.
 *  stdout/err is redirected to scribed.out.test<test_num>
 *
 *  @param  string  $test_name               name of test being run
 *  @param  string  $scribed_path            scribed binary full path
 *  @param  int     $port                    port
 *  @param  string  $config                  name of config file to use
 *
 *  @return int  returns process id on success, 0 on failure
 *  @author agiardullo
 */
function scribe_start($test_name, $scribed_path, $port, $config) {
  $scribed = "$scribed_path/scribed";
  $command = "$scribed -p $port $config > scribed.out.$test_name 2>&1 & echo $!";

  print("Starting Scribed...\n");

  $pid = system($command, $error);

  if ($error) {
    print("ERROR: Could not start scribed.\n");
    return 0;
  }

  // give scribe a chance to startup
  sleep(2);
  return $pid;
}


/**
 *  Stops scribe.  Will check for termination if $pid is provided.
 *
 *  @param  string  $scribe_ctrl_path         scribe_ctrl path
 *  @param  int     $port                     port
 *  @param  int     $pid                      scribed process (optional)
 *
 *  @return bool returns true on success
 *  @author agiardullo
 */
function scribe_stop($scribe_ctrl_path, $port, $pid = 0) {
  $wait = 5; //seconds
  $scribe_ctrl = "$scribe_ctrl_path/scribe_ctrl";
  $command = "$scribe_ctrl stop $port ";

  print("Stopping Scribed: $command\n");

  // send stop command to scribe
  system($command, $error);
  echo "$command => $error\n";

  if ($error != 0) {
    return true;
  }

  sleep($wait);

  if ($pid) {
    // kill if scribe process has not yet terminated
    system('kill -9 ' . $pid, $success);
    echo "kill -9 $pid => $success\n";

    if ($success) {
      print("ERROR: scribed did not stop after $wait seconds.\n");
      return false;
    }
  }

  return true;
}

function check_alive($scribe_ctrl_path, $port) {
  $scribe_ctrl = "$scribe_ctrl_path/scribe_ctrl";
  $command = "$scribe_ctrl status $port ";

  // send fb303 status command
  print("Checking fb303 status.\n");
  system($command, $return);

  if ($return == 2) {  /* TODO: probably shouldn't hard code this */
    return true;
  } else {
    return false;
  }
}

function get_counters($scribe_ctrl_path, $port) {
  $counters = array();
  $scribe_ctrl = "$scribe_ctrl_path/scribe_ctrl";
  $command = "$scribe_ctrl counters $port ";

  $output = array();
  exec($command, $output);

  foreach ($output as $line) {
    $parsed = explode(": ", $line);
    if (count($parsed == 2)) {
      $counters[$parsed[0]] = $parsed[1];
    }
  }

  return $counters;
}

/**
 *  Checks for all messages that contain $clientname in all files that match
 *  the given file prefix
 *
 *  @param  string  $path                     directory to scan
 *  @param  string  $fileprefix               which files to read
 *  @param  string  $client                   which messages to check
 *
 *  @return array["count"          => total number of messages found
 *                "out_of_order"]  => number of messages not in order
 *  @author agiardullo
 */
function resultChecker($path, $fileprefix, $clientname) {
  $results = array();
  $results["count"] = 0;
  $results["out_of_order"] = 0;
  $last_entry = -1;

  try {

    // For each file that matches the prefix, call resultFileChecker
    // to count the matching lines
    $files = array();
    if ($dir = opendir($path)) {
      while (false !== ($file = readdir($dir))) {
        if (0 === strncmp($file, $fileprefix, strlen($fileprefix))) {
          $files[] = $file;
        }
      }
      sort($files);
      foreach ($files as $file) {
          $filename = "$path/$file";
          $tmp_results = resultFileChecker($filename, $clientname, $last_entry);
          $results["count"] += $tmp_results["count"];
          $results["out_of_order"] += $tmp_results["out_of_order"];
      }
    } else {
      print("ERROR: could not open directory: $path \n");
    }

  } catch(Exception $e) {
    print("EXCEPTION: " . $e->getMessage() . "\n");
  }

  if ($dir) {
    closedir($dir);
  }

  $count = $results['count'];
  $out_of_order = $results['out_of_order'];
  print("Total: <$count> matching entries, <$out_of_order> out of order\n");

  return $results;
}

function resultFileChecker($filename, $clientname, &$last_entry) {
  $results = array();
  $results["count"] = 0;
  $results["out_of_order"] = 0;

  $file = fopen($filename, 'r');

  if (!$file) {
    print("ERROR: could not open result file: $file\n");
    return $results;
  }

  while ($line = fgets($file)) {
    if (!empty($line)) {
      $client = "";

      $fields = explode("-", $line);
      if (is_array($fields) && count($fields) >= 2) {
        list($client, $entry) = $fields;
      }

      // only check non-empty lines that countain the given client name
      if ($client == $clientname) {
        $results["count"]++;

        if ($entry != $last_entry + 1) {
          $results["out_of_order"]++;
          print("Out of order entry: <$entry> follows <$last_entry>\n");
        }
        $last_entry = $entry;
      }
    }
  }

  $count = $results['count'];
  $out_of_order = $results['out_of_order'];
  print("$filename: <$count> total, <$out_of_order> out of order\n");

  fclose($file);
  return $results;
}
