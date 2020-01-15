#!/usr/bin/env python
# encoding: utf-8


from pyspark import SparkConf, SparkContext
import numpy as np
from numba import cuda
import time
import os
from pyspark.sql import SparkSession,Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from timeit import repeat
from timeit import timeit

#os.environ['PYSPARK_PYTHON']='/usr/bin/python2.7'
os.environ['HADOOP_CONF_DIR']='/etc/hadoop/conf'
os.environ['YARN_CONF_DIR']='/etc/spark2/conf.cloudera.spark2_on_yarn'

#conf = SparkConf().setMaster("local").setAppName("SparkXdrExampleGPU")
#sc = SparkContext(conf=conf)

def gpu_work3(xs):
    #print(type(xs))
    inp = np.asarray(list(xs),dtype=np.int64)
    #print("inp: ",len(inp))
    inp=cuda.to_device(inp)
    out = np.zeros((len(inp),7),dtype=np.int64)
    out=cuda.to_device(out)
    block_size = 32*4*2   
    grid_size = (len(inp)+block_size-1)//block_size
    #print("grid block: ",grid_size,block_size)
    foo3[grid_size,block_size](inp,out)
    outc=out.copy_to_host()
    return outc

@cuda.jit
def foo3(inp,out):
    i= cuda.grid(1)
    #cuda.syncthreads()
    if i < len(out):
        out[i][0]=inp[i][5]
        out[i][1]=inp[i][0]
        out[i][2] = 1 if(inp[i][1] > 0) else 0
        out[i][3]=1 if(inp[i][1]>0 and inp[i][3] is not None) else 0
        out[i][4]=1 if(inp[i][4]>0 and inp[i][3] == 0) else 0
        out[i][5] = inp[i][4] if(inp[i][4]>0) else 0
        out[i][6] = inp[i][4] if(inp[i][4]>0 and inp[i][3]==0) else 0
        #out[i][7] = len(out)
        #out[i][7] = sumD
    #cuda.syncthreads()
   


#flume_data = sc.textFile("file:///opt/data.csv")

conf = SparkConf().setMaster("yarn-client").setAppName("SparkXdrExampleGPU").set("spark.yarn.executor.memoryOverhead","10g")
sc = SparkContext(conf=conf)
#/home/users/chenzhuo/datas/xdr0.csv.COMPLETED
def MPData():
    a = time.time()
    #flume_data = sc.textFile("hdfs://cdsw1.richstone.com:8020/flume_xdr/fs_test.2018-12-18.154511675325[2-9].xdr")
    #flume_data = sc.textFile("/test1/datas/xdr[0-9].csv.COMPLETED")
    flume_data = sc.textFile("/test1/datas/xdr0.csv.COMPLETED")
    #flume_data = sc.textFile("hdfs://cdsw1.richstone.com:8020/flume_xdr/fs_test.2018-12-18.1545116753258.xdr")
    lines = flume_data.map(lambda line: line.split(','))
    #print(xdr_data.first())
    #xdr_data = xdr_data.map(lambda p: Row(Response_Time=int(p[0]), Rcode=int(p[1]))
    xdr_data = lines.map(lambda p: \
            Row(Procedure_End_Time=p[0],\
            UL_Data=p[2],\
            DL_Data=p[3],\
            Rcode=p[4],\
            DNSReq_Num=p[5],\
            Response_Time=p[6]))
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .getOrCreate()
    gpu_start = time.time()
    res_data = xdr_data.mapPartitions(gpu_work3)
    gpu_end = time.time()
    #countnum = res.data.count()
    #res_data = res_data.mapPartitions(lambda a : a[7]=countnum)
    print("count: +++++++++++++++++++++",res_data.count())
    data=res_data.take(10)
    #data=res_data.collect()
    #res_data.saveAsTextFile("/tmp/gpu2")
    #print(data)
    #print(time.time()-a)    #GPU example do not print
    #------------------------------------------------------------------------
    # The schema is encoded in a string.
    schemaString = "JIAKUAN_DNS_001|JIAKUAN_DNS_002|JIAKUAN_DNS_003|JIAKUAN_DNS_004|JIAKUAN_DNS_005|JIAKUAN_DNS_006|JIAKUAN_DNS_007"
    
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split("|")]
    schema = StructType(fields)
    
    dataX=[map(str,list(data[i])) for i in range(len(data))]
    #print(dataX)
    df = spark.createDataFrame(sc.parallelize(dataX),schema)
    #df = spark.createDataFrame(sc.parallelize(data),schema)
    df.createOrReplaceTempView("xdr_table")
    
    # SQL can be run over DataFrames that have been registered as a table.
    results = spark.sql("SELECT * FROM xdr_table")
    results.show()
    print("gpu executor time:",(gpu_end - gpu_start))
    print("Job executor time:",(time.time()-a))
    #results.rdd.repartition(1).saveAsTextFile("/tmp/gpu45")

t = repeat('MPData()', 'from __main__ import MPData,foo3,gpu_work3', number=1,repeat=1)
print(t)
sc.stop()
