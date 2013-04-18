Copyright (c) 2007-2008 Facebook

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

See accompanying file LICENSE or visit the [Scribe site](http://developers.facebook.com/scribe/)

- - -

This file contains a couple of simple examples of how to configure and use
Scribe.
	
Example code in this directory:

 - scribe_cat: a simple example of a client that can send messages to Scribe
 - scribe_ctrl: a script that manages a running Scribe instance (requires root)
 - example1.conf: sample configuration file for running Example 1
 - example2.conf: sample configuration file for running Example 2

Example 1
===

This is a simple example that shows how to configure and send messages to
Scribe.

Create a directory to log messages:

	mkdir /tmp/scribetest

Start scribe using the configuration in example1.conf:

	src/scribed examples/example1.conf

From another terminal, use scribe_cat to send a message to scribe:

	echo "hello world" | ./scribe_cat test

If the previous command failed, make sure you did a 'make install' from the
root scribe directory and that `$PYTHONPATH` is set correctly (see README.BUILD)

Verify that the message got logged:

	cat /tmp/scribetest/test/test_current

Check the status of scribe (requires root):

	./scribe_ctrl status

Check scribe's counters (you should see 1 message 'received good'):

	./scribe_ctrl counters

Shutdown scribe:

	./scribe_ctrl stop


Example 2
===

This example shows you how to log messages between multiple Scribe instances.
In this example, we will run each Scribe server on a different port to simulate
running Scribe on multiple machines.

	          'client'                    'central'
	----------------------------     --------------------
	| Port 1464                 |    | Port 1463         |
	|        ----------------   |    | ----------------  |
	|     -> | scribe server |--|--->| | scribe server | |
	|        ----------------   |    | ----------------  |
	|                |          |    |    |         |    |
	|            temp file      |    |    |    temp file |
	|---------------------------     |-------------------
	                                      |
	                                 -------------------
	                                 | /tmp/scribetest/ |
	                                 -------------------


Create a directory for the second scribe instance:

	mkdir /tmp/scribetest2

Start up the 'central' instance of Scribe on port 1463 to write messages to disk
(See example2central.conf):

	src/scribed examples/example2central.conf

Start up the 'client' instance of Scribe on port 1464 to forward messages to
the 'central' Scribe server (See example2client.conf):

	src/scribed examples/example2client.conf

Use scribe_cat to send some messages to the 'client' Scribe instance:

	echo "test message" | ./scribe_cat -h localhost:1464 test2

	echo "this message will be ignored" | ./scribe_cat -h localhost:1464 ignore_me

	echo "123:this message will be bucketed" | ./scribe_cat -h localhost:1464 bucket_me

The first message will be logged similar to example 1.
The second message will not get logged.
The third message will be bucketized into 1 of 5 buckets
(See example2central.conf)

Verify that the first message got logged:

	cat /tmp/scribetest/test2/test2_current

Verify that the third message got logged into a subdirectory:

	cat /tmp/scribetest/bucket*/bucket_me_current

Check the status and counters of both instances:

	./scribe_ctrl status 1463
	./scribe_ctrl status 1464
	./scribe_ctrl counters 1463
	./scribe_ctrl counters 1464

Shutdown both servers:

	./scribe_ctrl stop 1463
	./scribe_ctrl stop 1464


Example 3
===

_Test Scribe buffering_
---

Startup the two Scribe instances used in Example 2.
Start the 'central' server first:

	src/scribed examples/example2central.conf

Then start the 'client':

	src/scribed examples/example2client.conf

Log a message to the 'client' Scribe instance:

	echo "test message 1" | ./scribe_cat -h localhost:1464 test3

Verify that the message got logged:

	cat /tmp/scribetest/test3/test3_current

Stop the 'central' Scribe instance:

	./scribe_ctrl stop 1463

Attempting to check the status of this server will return failure since it not running:

	./scribe_ctrl status 1463

Try to Log another message:

	echo "test message 2" | ./scribe_cat -h localhost:1464 test3

This message will be buffered by the 'client' since it cannot be forwarded to
the 'central' server.  Scribe will keep retrying until it is able to send.

After a couple seconds, the status of the 'client' will be set to a warning message:

	./scribe_ctrl status 1464

Try to Log yet another message(which will also get buffered):

	echo "test message 3" | ./scribe_cat -h localhost:1464 test3

Restart the 'central' instance:

	src/scribed examples/example2central.conf

Wait for both Scribe instance's statuses to change to ALIVE:

	./scribe_ctrl status 1463
	./scribe_ctrl status 1464

Verify that all 3 messages have now been received by the 'central' server:

	cat /tmp/scribetest/test3/test3_current

Shutdown:

	./scribe_ctrl stop 1463
	./scribe_ctrl stop 1464
