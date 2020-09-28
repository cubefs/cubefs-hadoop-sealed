#!/bin/env sh
export HADOOP_ROOT_LOGGER=info,console

test_jar=/root/hadoop/hadoop-3.3.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0.jar
hadoop jar $test_jar terasort  /tera/input /tera/output > tera_sort.log 2>&1


