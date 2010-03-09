#!/bin/bash

uptime=7200
dowtime=3600

while [ 1 ]; do
  sleep $dowtime
  echo [`date`] starting scribe sinkder... 
  ./scribed -p 9999 -c scribe.sinker.conf &
  pid1=$!
  ./scribed -p 9998 -c scribe.sinker.conf &
  pid2=$!
  echo pid1 is $pid1, pid2 is $pid2
  sleep $uptime
  echo [`date`] killing scribe sinkder
  kill -9 $pid1
  kill -9 $pid2
done
