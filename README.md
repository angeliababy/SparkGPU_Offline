# Spark on GPU 计算

本项目完整源码地址：https://github.com/angeliababy/SparkGPU_Offline

项目博客地址: https://blog.csdn.net/qq_29153321/article/details/103988522

本项目旨在研究GPU混合计算框架对Spark分布式计算进行加速,以下为研究测试代码
```
代码目录结构：
-|-batch    //numba方式的批处理代码
 |-cuda     //pycuda方式的批处理代码
 |-data     //测试数据
```

## spark cpu主要逻辑

SELECT UL_Data AS JIAKUAN_DNS_001,DL_Data AS JIAKUAN_DNS_002,
                        (CASE WHEN DNSReq_Num > 0 THEN 1 ELSE 0 END  ) AS JIAKUAN_DNS_003,
                        (case when DNSReq_Num > 0 and ISNULL(Rcode)=false and Rcode<>'' then 1 else 0 end) as JIAKUAN_DNS_004, 
                        (case when Response_Time>0 and Rcode=0 then 1 else 0 end) as JIAKUAN_DNS_005, 
                        (case when Response_Time>0 then Response_Time else 0 end) as JIAKUAN_DNS_006, 
                        (case when Response_Time>0 and Rcode=0 then Response_Time else 0 end) as JIAKUAN_DNS_007 , 
                        (select sum(1) from xdrdata ) as sumVal FROM xdrdata
                        
## spark gpu实现过程

**GPU编程一般步骤：**

在CUDA中，host和device是两个重要的概念，我们用host指代CPU及其内存，而用device指代GPU及其内存。CUDA程序中既包含host程序，又包含device程序，它们分别在CPU和GPU上运行。同时，host与device之间可以进行通信，这样它们之间可以进行数据拷贝。


1.分配host内存，并进行数据初始化；

2.分配device内存，并从host将数据拷贝到device上；

3.调用CUDA的核函数在device上完成指定的运算；

4.将device上的运算结果拷贝到host上；

5.释放device和host上分配的内存。


kernel在device上执行时实际上是启动很多线程，一个kernel所启动的所有线程称为一个网格（grid），同一个网格上的线程共享相同的全局内存空间，grid是线程结构的第一层次，而网格又可以分为很多线程块（block），一个线程块里面包含很多线程，这是第二个层次。线程两层组织结构如下图所示，grid和block可以灵活地定义为1-dim，2-dim以及3-dim结构

![在这里插入图片描述](https://img-blog.csdnimg.cn/20200115141552574.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzI5MTUzMzIx,size_16,color_FFFFFF,t_70)
由于SM的基本执行单元是包含32个线程的线程束，所以block大小一般要设置为32的倍数。

### numba gpu加速,只有map形式
```
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
```

### pycuda有两种形式，其核函数是C++函数形式，建议用map形式
map形式：
```
def gpufunc(xdr_data):
    a=time.time()
    xdr_data = iter(xdr_data)
    inp = np.asarray(list(xdr_data),dtype=np.float32)
    N = len(inp)
    # print("len:",N)
    out = np.zeros((len(inp),7),dtype=np.float32)
    # out = np.empty(N, gpuarray.vec.float1)

    N = np.int32(N)
    #print(inp,out)
    # GPU run
    nTheads = 256*4
    nBlocks = int( ( N + nTheads - 1 ) / nTheads )
    drv.init()
    dev = drv.Device(0)
    contx = dev.make_context()
    mod = SourceModule("""
__global__ void func(float *out, float *inp, size_t N)
{
  //const int i = blockIdx.x * blockDim.x + threadIdx.x;
  unsigned idxInLeftBlocks = blockIdx.x * (blockDim.x * blockDim.y);
  unsigned idxInCurrBlock  = threadIdx.y * blockDim.x + threadIdx.x;
  unsigned i = idxInLeftBlocks + idxInCurrBlock;
  if (i >= N-1)
  {
    return;
  }
  out[i*7+0] = inp[i*6+5];
  out[i*7+1]=inp[i*6+0];
  if(inp[i*6+1] > 0) 
    out[i*7+2] = 1;
  else 
    out[i*7+2] = 0;
  if(inp[i*6+1]>0 and inp[i*6+3]!=NULL) 
    out[i*7+3] = 1;
  else 
    out[i*7+3] = 0;
  if(inp[i*6+4]>0 and inp[i*6+3] == 0) 
     out[i*7+4] = 1;
  else 
     out[i*7+4] = 0;
  if(inp[i*6+4]>0) 
     out[i*7+5] = inp[i*6+4];
  else 
    out[i*7+5] = 0;
  if(inp[i*6+4]>0 and inp[i*6+3]==0) 
     out[i*7+6] = inp[i*6+4];
  else 
    out[i*7+6] = 0;
}
""")

    func = mod.get_function("func")

    start = timer()
    func(
            drv.Out(out), drv.In(inp), N,
            block=( nTheads, 1, 1 ), grid=( nBlocks, 1 ) )
    out1 = [np.asarray(x) for x in out]
    print("len",len(out1))
    contx.pop()
    del contx
    del inp
    del out
    run_time = timer() - start
    print("gpu run time %f seconds " % run_time)
    return iter(out1)
```

collect形式，核函数同上
```
    inp = np.asarray(xdr_data.collect(),dtype=np.float32)
    N = len(inp)
    # print("len:",N)
    
    out = np.zeros((len(inp),7),dtype=np.float32)
    # out = np.empty(N, gpuarray.vec.float1)

    N = np.int32(N)
    #print(inp,out)
    # GPU run
    nTheads = 256*4
    nBlocks = int( ( N + nTheads - 1 ) / nTheads )
    func(
            drv.Out(out), drv.In(inp), N,
            block=( nTheads, 1, 1 ), grid=( nBlocks, 1 ) )
```

参考博客[http://numba.pydata.org/numba-doc/latest/cuda/intrinsics.html#example](http://numba.pydata.org/numba-doc/latest/cuda/intrinsics.html#example)

[https://blog.csdn.net/xiaohu2022/article/details/79599947](https://blog.csdn.net/xiaohu2022/article/details/79599947)




