#!/usr/bin/env python
# encoding: utf-8

import time

import os
os.environ['PYSPARK_PYTHON']='/usr/bin/python'
os.environ['HADOOP_CONF_DIR']='/etc/hadoop/conf'
os.environ['YARN_CONF_DIR']='/etc/spark2/conf.cloudera.spark2_on_yarn'
os.environ['PYSPARK_PYTHON']='/usr/bin/python'


from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession,Row
from pyspark.sql.types import *

conf = SparkConf().setMaster("yarn-client").setAppName("SparkSQLXdrExample").set("spark.yarn.executor.memoryOverhead","10g")
sc = SparkContext(conf=conf)
#sc = SparkContext(conf)

spark = SparkSession\
    .builder\
    .appName("SparkSQLXdrExample")\
    .getOrCreate()
sc = spark.sparkContext

# empty value processing
def conver_p(x):
    if x == '':
        return 0
    else:
        return int(x)


#hdfs://cdsw1.richstone.com/flume_xdr
#flume_data = sc.textFile("hdfs://cdsw1.richstone.com:8020/flume_xdr/fs_test.2018-12-18.1545116753258.xdr")
flume_data = sc.textFile("/test1/datas/xdr0.csv.COMPLETED")
a = time.time()
lines = flume_data.map(lambda line: line.split(','))
xdr_data = lines.map(lambda p: \
        Row(Procedure_End_Time=conver_p(p[0]),\
        UL_Data=conver_p(p[2]),\
        DL_Data=conver_p(p[3]),\
        Rcode=conver_p(p[4]),\
        DNSReq_Num=conver_p(p[5]),\
        Response_Time=conver_p(p[6])))

# Apply the schema to the RDD and Create DataFrame
swimmers = spark.createDataFrame(xdr_data)
print("count:++++++++++++++++++++++++++++++",swimmers.count())
# Creates a temporary view using the DataFrame
swimmers.createOrReplaceTempView("xdrdata")


result=spark.sql("SELECT UL_Data AS JIAKUAN_DNS_001,DL_Data AS JIAKUAN_DNS_002,"+
                        "(CASE WHEN DNSReq_Num > 0 THEN 1 ELSE 0 END  ) AS JIAKUAN_DNS_003," +
                        "(case when DNSReq_Num > 0 and ISNULL(Rcode)=false and Rcode<>'' then 1 else 0 end) as JIAKUAN_DNS_004," +
                        "(case when Response_Time>0 and Rcode=0 then 1 else 0 end) as JIAKUAN_DNS_005," +
                        "(case when Response_Time>0 then Response_Time else 0 end) as JIAKUAN_DNS_006," +
                        "(case when Response_Time>0 and Rcode=0 then Response_Time else 0 end) as JIAKUAN_DNS_007 ," +
                        "(select sum(1) from xdrdata ) as sumVal FROM xdrdata limit 10")

print(result.show())
print("run time: ",time.time()-a)
#result.rdd.repartition(1).saveAsTextFile("hdfs:///test1/xdr/xdrcpu")

#spark.stop()
