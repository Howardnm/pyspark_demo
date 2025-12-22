# coding:utf8
"""
输出位置：文件
"""
# 1. 导包
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# 2. 构建执行入口，SparkSession对象
spark: SparkSession = SparkSession.builder.\
    appName("test").\
    master("local[*]").\
    config("spark.sql.shuffle.partitions", 1).\
    getOrCreate()

df: DataFrame = spark.readStream.\
    format("rate").\
    option("rowsPerSecond", 100).\
    option("rampUpTime", 10).\
    option("numPartitions", 1).\
    load()

# TODO 1: sink format 输出格式
# file: 小缺点，file数据会一个批次，产生一份文件，一般我们会手动写程序完成小文件合并，或者可以借助第三方工具。
# 使用file输出，需要设置 checkpointLocation
# 5. 数据输出
df.writeStream.\
    format("parquet").\
    outputMode("append").\
    option("path", "../data/output/parquet_output").\
    option("checkpointLocation", "../data/output/parquet_checkpoint").\
    start().\
    awaitTermination()

# Spark内置支持多种位置输出
#
# - console，输出到控制台
# - foreach，单条输出
# - foreachBatch，单批次输出
# - file(parquet|csv|json|text......)，文件输出
# - kafka，输出数据到kafka

