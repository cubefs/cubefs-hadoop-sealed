#!/bin/env sh

export HADOOP_HOME=/root/hadoop/hadoop-3.3.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_YARN_HOME=$HADOOP_HOME
export PATH=$HADOOP_HOME/bin/:$PATH
export HADOOP_ROOT_LOGGER=info,console
