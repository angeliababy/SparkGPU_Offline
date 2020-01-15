#!/bin/bash

# 当前目录路径
CDIR=`pwd`

ST=`date`
/usr/bin/spark2-submit  --master yarn-client ${CDIR}/gpusparksqltest.py  --driver-memory 20g --num-executors 4 --executor-memory 20g --executor-cores 4 --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps " --driver-java-options="-XX:MaxPermSize=10g -Xmx10g" --conf "spark.eventLog.enabled=false"

ET=`date`

echo $ST
echo $ET
