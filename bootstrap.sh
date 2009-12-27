#!/bin/sh

# To be safe include -I flag
autoreconf --force --verbose --install
./configure --config-cache $*
