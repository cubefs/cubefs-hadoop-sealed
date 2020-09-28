#!/bin/env sh
export HADOOP_ROOT_LOGGER=info,console
test_jar=/root/hadoop/hadoop-3.3.0/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.3.0-tests.jar

hadoop jar $test_jar mrbench \
  -numRuns 20 \
  -maps 6 \
  -reduces 3 \
  -inputLines 10 \
  -inputType descending \
> mrbench.log 2>&1

