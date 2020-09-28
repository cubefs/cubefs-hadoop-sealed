#!/bin/env sh
export HADOOP_ROOT_LOGGER=info,console

test_jar=/root/hadoop/hadoop-3.3.0/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.3.0-tests.jar

hadoop jar $test_jar nnbench \
  -operation create_write \
  -maps 6 \
  -reduces 3 \
  -blockSize 8 \
  -bytesToWrite 1 \
  -numberOfFiles 100 \
  -replicationFactorPerFile 3 \
  -readFileAfterOpen true \
  -baseDir /benchmarks/NNBench-`hostname -s` \
> nnbench.log 2>&1

hadoop jar $test_jar nnbench \
  -operation open_read \
  -maps 6 \
  -reduces 3 \
  -blockSize 8 \
  -bytesToWrite 1 \
  -numberOfFiles 100 \
  -replicationFactorPerFile 3 \
  -readFileAfterOpen true \
  -baseDir /benchmarks/NNBench-`hostname -s` \
>> nnbench.log 2>&1 

hadoop jar $test_jar nnbench \
  -operation rename \
  -maps 6 \
  -reduces 3 \
  -blockSize 8 \
  -bytesToWrite 1 \
  -numberOfFiles 100 \
  -replicationFactorPerFile 3 \
  -readFileAfterOpen true \
  -baseDir /benchmarks/NNBench-`hostname -s` \
>> nnbench.log 2>&1 

hadoop jar $test_jar nnbench \
  -operation delete \
  -maps 6 \
  -reduces 3 \
  -blockSize 8 \
  -bytesToWrite 1 \
  -numberOfFiles 100 \
  -replicationFactorPerFile 3 \
  -readFileAfterOpen true \
  -baseDir /benchmarks/NNBench-`hostname -s` \
>> nnbench.log 2>&1 
