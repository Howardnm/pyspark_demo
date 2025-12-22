# coding:utf8
"""
演示：Spark结构化流的输出位置：memory方式
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


query = df.writeStream.format("memory").\
    outputMode("append").\
    queryName("test").\
    start()

while True:
    spark.sql("SELECT COUNT(*) FROM test").show()
    # spark.sql("SELECT COUNT(*) FROM test").write.format("jdbc")
    time.sleep(1)
# query.awaitTermination()
