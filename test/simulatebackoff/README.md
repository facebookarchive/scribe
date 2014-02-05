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

One of the common problems we face when running scribe is that when multiple
scribe servers(senders) are forwarding messages to a single scribe server(receiver)
some times the receiver cannot keep up and asks the remote servers to backoff
by sending a TRY_LATER.
This test is used to simulate that behavior so that we can study it and
improve the way scribe senders backoff. In this test we basically try to
"hammer" a scribe receiver.

You will need 3 servers: 2 will act as senders and 1 as receiver

	IMPORTANT: substitute "put_your_receiving_server_here" in hammersource.conf
	appropriately with you receiving server

Follow the steps in order

1. On the 1st sender do

	    $ cd scribe/test/simulatebackoff
	    $ scribed -c hammersource.conf -p 1463     # start the scribe server 1
	
   On another terminal window do

	    $ sudo php new.superstress.php  # create scribe clients and send to server 2

2. On the 2nd sender do

	    $ cd scribe/test/simulatebackoff
	    $ scribed -c hammersource.conf -p 1463    # start the scribe server 2
		
   On another terminal window do

		$ sudo php new.superstress.php  # create scribe clients and send to server 2

3. Wait a few minutes and let the two servers buffer messages to local disk
for a while. For consistency you may wait for new.superstress.php to finish

4. On the scribe receiver

		$ sudo rm -rf /tmp/defaultprimary /tmp/defaultsecondary /tmp/hammerprimary
		/tmp/hammersecondary
		$ cd scribe/test/simulatebackoff
		$ scribed hammersink.conf     # start the scribe receiver

5. Use `$sudo scribe_ctrl counters 1463` on the machines to get basic counters.

6. Use some monitoring system like ganglia to study the network sent/received
bytes, memory usages and disk

7. Verify that all messages sent were received. Note that you cannot use
resultchecker for this because two senders are used.

