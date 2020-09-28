#!/bin/env sh
hadoop_classpath=`hadoop classpath`
export CLASSPATH=$hadoop_classpath:$CLASSPATH:$HADOOP_HOME/share/hadoop/tools/lib/*
package="org.apache.hadoop.fs."
names="TestChubaoFileSystemContract ChubaoFSMainOperationsTest"

for name in $names
do
  cases=$cases" "$package$name
done
echo "TestCases:"$cases
java org.junit.runner.JUnitCore $cases