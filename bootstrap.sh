#!/bin/sh

# To be safe include -I flag
autoreconf --force --verbose --install
./configure --with-boost-system=boost_system --with-boost-filesystem=boost_filesystem --config-cache $*
