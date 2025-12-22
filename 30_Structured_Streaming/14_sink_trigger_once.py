# coding:utf8
"""
演示：设置触发器Trigger
Spark支持如下几种模式：
    - 默认模式，不指定，Spark`尽可能的快`完成一个批次的计算
    - 固定间隔模式，可以设置固定的批次等待时间 ，比如5秒。 达到5秒这个批次就计算。
    - 一次性模式，启动后执行一次批次然后程序结束。
    - 固定间隔连续处理模式，和固定间隔一样，是设置固定的批次等待时间，只不过比上述的固定间隔模式，延迟更低些。
"""
import time

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame, Row

# 构建Spark执行环境入口对象
spark = SparkSession.builder.\
    master("local[*]").\
    appName("test").\
    config("spark.sql.shuffle.partitions", 1).\
    config("spark.default.parallelism", 1).\
    getOrCreate()


# 构建Source：Socket
df: DataFrame = spark.readStream.\
    format("socket").\
    option("host", "ct104").\
    option("port", "9999").\
    load()

# TODO 1: 一次性模式，启动后执行一次批次然后程序结束。
df.writeStream.\
    format("console").\
    trigger(once=True).\
    outputMode("append").\
    start().\
    awaitTermination()
