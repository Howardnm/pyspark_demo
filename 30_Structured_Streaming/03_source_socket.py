# coding:utf8
"""
演示Spark的Socket数据源
Socket数据源就是让Spark作为Socket客户端，去连接Socket服务器，从Socket服务器哪里获取数据
准备工作：
安装nc工具：(作用：创建一个socket服务端，发送数据流)
  1. Ubuntu系统：sudo apt-get install netcat
  2. CentOS系统：sudo yum install -y nc
启动nc服务端：nc -lk 9999
启动本程序
"""
# 1. 导包
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

# 2. 构建执行入口，SparkSession对象
spark: SparkSession = SparkSession.builder.\
    appName("test").\
    master("local[*]").\
    config("spark.sql.shuffle.partitions", 1).\
    getOrCreate()

# TODO: 读取Socket数据源
# 3. 构建Source：Socket
df: DataFrame = spark.readStream.\
    format("socket").\
    option("host", "ct104").\
    option("port", "9999").\
    load()

# 4. 数据处理 ： 略

# 5. 数据输出
df.writeStream.\
    format("console").\
    outputMode("append").\
    start().\
    awaitTermination()
