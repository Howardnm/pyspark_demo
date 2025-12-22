# coding:utf8
"""
CheckPoint检查点在流计算中：
- 是自动化的，间隔是和批次一一对应的。
  - 也就是批次计算结束，检查点记录一次
- 检查点记录什么
  - offsets：记录当前接收到的数据（等待计算）
  - commits：记录已经处理到的数据（完成计算）
  - metatdata：元数据信息
  - source、sink：数据源和数据输出的一些元数据
  - state：记录的状态信息（比如，在complete模式下，无界DF中的数据），记录在硬盘中做持久化保存。
    一旦出问题，可以续上。
"""
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame, Row


# 构建Spark执行环境入口对象
spark = SparkSession.builder.\
    master("local[*]").\
    appName("test").\
    config("spark.sql.shuffle.partitions", 1).\
    config("spark.default.parallelism", 1). \
    getOrCreate()

# TODO 方式1: 在上面builder对象，补上下面这个参数
# config("spark.sql.streaming.checkpointLocation", "../data/output/ck2")

# 构建Source：Socket
df: DataFrame = spark.readStream.\
    format("socket").\
    option("host", "ct104").\
    option("port", "9999").\
    load()

# TODO 方式2: 设置checkpoint检查点的目的，就是确保一旦出现问题可以自动恢复并续上前面的状态。
df.writeStream.\
    format("parquet").\
    outputMode("append").\
    option("path", "../data/output/parquet_output").\
    option("checkpointLocation", "../data/output/parquet_checkpoint").\
    start().\
    awaitTermination()
# 在输出的时候，调用option，设置checkpointLocation属性即可

# TODO : 方式1、方式2，只能二选一，最好把目录存到HDFS
