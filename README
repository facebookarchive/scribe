Introduction
============

Scribe is a server for aggregating log data that's streamed in real
time from clients. It is designed to be scalable and reliable.

See the Scribe Wiki for documentation:
http://wiki.github.com/facebook/scribe

Keep up to date on Scribe development by joining the Scribe Discussion Group:
http://groups.google.com/group/scribe-server/


License (See LICENSE file for full license)
===========================================
Copyright 2007-2008 Facebook

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Hierarchy
=========

scribe/

  aclocal/
    Contains scripts for building/linking with Boost

  examples/
    Contains simple examples of using Scribe

  if/
    Contains Thrift interface for Scribe

  lib/
    Contains Python package for Scribe

  src/
    Contains Scribe source

  test/
    Contain php scripts for testing scribe


Requirements
============

[libevent] Event Notification library
[boost] Boost C++ library (version 1.36 or later)
[thrift] Thrift framework (version 0.5.0 or later)
[fb303] Facebook Bassline (included in thrift/contrib/fb303/)
   fb303 r697294 or later is required.
[hadoop] optional. version 0.19.1 or higher (http://hadoop.apache.org)

These libraries are open source and may be freely obtained, but they are not
provided as a part of this distribution.


Helpful tips:
-Thrift, fb303, and scribe installation expects python to be installed
 under /usr.  See PY_PREFIX option in 'configure --help' to change this path.
-Some python installs do not include python site-packages in the default
 python include path.  If python cannot find the installed packages for
 scribe or fb303, try setting the environment variable PYTHONPATH to the
 location of the installed packages.  This path gets output during
 'make install'. (Eg: PYTHONPATH='/usr/lib/python2.5/site-packages').


To build
========

./bootstrap.sh <configure options>
make

(If you have multiple versions of Boost installed, see Boost configure options below.)

Subsequent builds
=================

./bootstrap <configure options>
make

OR

./configure <configure options>
make

NOTE: After the first run with bootstrap.sh you can use "[ ./bootstrap | ./configure ] <options>" followed by "make"
to create builds with different configurations. "bootstrap" can be passed the same arguments as "configure".

Make sure that if you change configure.ac and|or add macros run "bootstrap.sh".
to regenerate configure. In short whenever in doubt run "bootstrap.sh".


Configure options
=================

To find all available configure options run
./configure --help

Use *only* the listed options.

Examples:
# To disable optimized builds and turn on debug. [ default has been set to optimized]
./configure --disable-opt

# To disable static libraries and enable shared libraries. [ default has been set to static]
./configure --disable-static

# To build scribe with Hadoop support
./configure --enable-hdfs

# If the build process cannot find your Hadoop/Jvm installs, you may need to specify them manually:
./configure --with-hadooppath=/usr/local/hadoop --enable-hdfs CPPFLAGS="-I/usr/local/java/include -I/usr/local/java/include/linux" LDFLAGS="-ljvm -lhdfs"

# To set thrift home to a non-default location
./configure --with-thriftpath=/myhome/local/thrift

# If Boost is installed in a non-default location or there are multiple Boost versions
# installed, you will need to specify the Boost path and library names
./configure --with-boost=/usr/local --with-boost-system=boost_system-gcc40-mt-1_36 --with-boost-filesystem=boost_filesystem-gcc40-mt-1_36


Install
=======

as root:
make install


Run
===

See the examples directory to learn how to use Scribe.


Acknowledgements
================
The build process for Scribe uses autoconf macros to compile/link with Boost.
These macros were written by Thomas Porschberg, Michael Tindal, and
Daniel Casimiro.  See the m4 files in the aclocal subdirectory for more
information.
