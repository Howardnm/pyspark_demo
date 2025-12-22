# coding:utf8
"""
演示Spark的Rate数据源
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

# TODO: 读取Rate数据源，Rate数据源是Spark内置的一个用于压力测试的流数据源
df: DataFrame = spark.readStream.\
    format("rate").\
    option("rowsPerSecond", 100).\
    option("rampUpTime", 10).\
    option("numPartitions", 1).\
    load()

# rowsPerSecond: 每秒生成多少行数据
# rampUpTime: 多少秒内达到rowsPerSecond的速率，即逐渐增加生成速率，直到达到rowsPerSecond
# numPartitions: 生成数据的分区数

# 4. 数据处理 ： 略

# 5. 数据输出
df.writeStream.\
    format("console").\
    outputMode("append").\
    option("truncate", False).\
    start().\
    awaitTermination()

# truncate: 控制台输出时，是否截断显示内容，False表示不截断，True表示截断