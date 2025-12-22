# coding:utf8
"""
Spark 结构化流的单词计数小程序
准备工作：
安装nc工具：(作用：创建一个socket服务端，发送数据流)
  1. Ubuntu系统：sudo apt-get install netcat
  2. CentOS系统：sudo yum install -y nc
启动nc服务端：nc -lk 9999
启动本程序
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# 构建Spark执行环境入口对象
spark = SparkSession.builder.\
    master("local[*]").\
    appName("test").\
    config("spark.sql.shuffle.partitions", 1).\
    getOrCreate()

# TODO 1: 读取socket数据流
# 假设输入的数据是： 单词 单词 单词...
df = spark.readStream.\
    format("socket").\
    option("host", "ct104").\
    option("port", 9999).\
    load()


# TODO 2: 数据处理
# 通过split 切分空格，通过爆炸函数将数据都划到一个列中，起个别名叫word列
word_df = df.select(
    F.explode(F.split("value", " ")).alias("word")
)

# 统计单词出现的次数
result_df = word_df.groupby("word").count()


# TODO 3: 输出数据
result_df.writeStream.\
    outputMode("complete").\
    format("console").\
    start().\
    awaitTermination()

# socket服务端
# [root@CT104 ~]# nc -lk 9999
# spark flink spark
# spark a

# 输出结果：
# -------------------------------------------
# Batch: 0
# -------------------------------------------
# +----+-----+
# |word|count|
# +----+-----+
# +----+-----+
#
# -------------------------------------------
# Batch: 1
# -------------------------------------------
# +-----+-----+
# | word|count|
# +-----+-----+
# |spark|    2|
# |flink|    1|
# +-----+-----+
#
# -------------------------------------------
# Batch: 2
# -------------------------------------------
# +-----+-----+
# | word|count|
# +-----+-----+
# |spark|    3|
# |    a|    1|
# |flink|    1|
# +-----+-----+