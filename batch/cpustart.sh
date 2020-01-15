#!/bin/bash


CDIR=`pwd`
START_TIME=`date`
/opt/cloudera/parcels/SPARK2-2.2.0.cloudera1-1.cdh5.12.0.p0.142354/lib/spark2/bin/spark-submit --master yarn-client ${CDIR}/cpusparksqltest.py --driver-memory 20g --num-executors 4 --executor-memory 20g --executor-cores 4 --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps " --driver-java-options="-XX:MaxPermSize=10g -Xmx10g" --conf "spark.eventLog.enabled=false"

END_TIME=`date`

echo $START_TIME
echo $END_TIME

#/opt/cloudera/parcels/SPARK2-2.2.0.cloudera1-1.cdh5.12.0.p0.142354/lib/spark2/bin/spark-submit --master yarn-client sparksqltestcpu.py --driver-memory 20g --num-executors 4 --executor-memory 20g --executor-cores 4 --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps " --driver-java-options="-XX:MaxPermSize=10g -Xmx10g" --conf "spark.eventLog.enabled=false"
