#!/bin/env sh
hbase pe --nomapred --oneCon=true --table=rw_test_1 --rows=1000 --valueSize=100 --presplit=4 --autoFlush=true randomWrite 2
hbase pe --nomapred --oneCon=true --table=rw_test_2 --size=1.1 --valueSize=5000 --presplit=4 --autoFlush=true sequentialWrite 2
hbase pe --nomapred --oneCon=true --table=rw_test_2 --size=1.1 --valueSize=5000 randomRead 2
hbase pe --nomapred --oneCon=true --table=rw_test_2 --size=1.1 --valueSize=5000 sequentialRead 2
hbase pe --nomapred --oneCon=true --table=rw_test_2 --size=1.1 --valueSize=5000 checkAndDelete 2