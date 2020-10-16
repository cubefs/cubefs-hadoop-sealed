# Compile
`mvn package`

Note:
The following two jars  may not be found in the maven repository. You need to modify pom.xml to solve the dependency problem during compilation, you may  refer to the comment in pom.xml.

1. libchubaofs-xx-SNAPSHOT.jar, building from chubaofs/java
2. hadoop-common-3.0.0-tests.jar, from hadoop bin package(hadoop-3.3.0/share/hadoop/common/hadoop-common-3.3.0-tests.jar)

# Deploy

When using the SDK, you need to use one so and two jars and two profiles.

**One so:**
1. libsdk.so, building from chubaofs/libsdk, used in cfs-site.xml.

**Two jars:**
1. chubaofs-hadoop-xx-SNAPSHOT.jar
2. libchubaofs-xx-SNAPSHOT.jar

**Two profiles:**

1. core-site.xml
2. cfs-site.xml

## HDFS Shell on ChubaoFS

1. Put the two jars to $HADOOP_HOME/share/hadoop/common
2. Put the two profiles to $HADOOP_HOME/etc/hadoop

## YARN on ChubaoFS

1. Put the two jars to $HADOOP_HOME/share/hadoop/common
2. Put the two profiles to $HADOOP_HOME/etc/hadoop
3. Start the yarn servers.

## HBase on ChubaoFS

1.  Put the two jars to  $HBASE_HOME/lib.
2.  Put the two profiles to $HBASE_HOME/conf.
3.  Modify conf/hbase-site.xml
    ``<property>`
    `<name>hbase.rootdir</name>`
    `<value>cfs://172.16.1.1:8080/hbase</value>`
    </property>`
 4. Start hbase。

# Usage

Just replace hdfs:// with cfs://

# About tests.

1.  The unit tests.

     The scirpt is sbin/unit_test

2.  Mapreduce Benchmark

   The following cases are passed:
   dfsio/mrbench/nnbench/terasort
   You may view  detailed test parameters in the sbin/mr_bench

3.  HBase Benchmark

   The following cases are passed:
   randomWrite/sequentialWrite/randomRead/sequentialRead/checkAndDelete
   You may view  detailed test parameters in the sbin/hbase_bench

 Note: The above tests are done in a cluster of 4 dockers, each uses 4 cpus and 8GB memory.