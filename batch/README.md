## Spark on GPU 批处理模式

数据样本于data目录中

### GPU模式操作指引

1. 该计算模式为yarn计算模式，因此数据必须先上传HDFS

2. 读取数据的路径需要修改`gpusparksqltest.py`文件中的代码，

   如:`flume_data = sc.textFile("/test1/datas/xdr0.csv.COMPLETED")`其中`/test1/datas/xdr0.csv.COMPLETED`为HDFS上测试数据路劲

3. 执行`gpustart.sh` 启动测试

### CPU模式操作指引

1. 需要用spark2.x版本（如果没有设置全局环境变量需要修改启动脚本`cpustart.sh`）

2. 该计算模式为yarn计算模式，因此数据必须先上传HDFS

3. 读取数据的路径需要修改`cpusparksqltest.py`文件中的代码，

   如:`flume_data = sc.textFile("/test1/datas/xdr0.csv.COMPLETED")`中`/test1/datas/xdr0.csv.COMPLETED`为HDFS上测试数据路劲

4. 执行`cpustart.sh` 启动测试
