#!/bin/env sh

test_jar=/root/hadoop/hadoop-3.3.0/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.3.0-tests.jar
hadoop jar $test_jar TestDFSIO  -write -nrFiles 10 -size 100MB > dfsio.log 2>&1
hadoop jar $test_jar TestDFSIO  -read -nrFiles 10 -size 100MB > dfsio_read.log 2>&1
hadoop jar $test_jar TestDFSIO  -clean

