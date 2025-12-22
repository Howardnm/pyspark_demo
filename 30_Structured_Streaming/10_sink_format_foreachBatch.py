# coding:utf8
"""
演示：Spark结构化流的输出位置：foreachBatch方式
"""
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


def func(df: DataFrame, epoch_id: int):
    """
    将一个微批批次的数据，打包提供传入（DataFrame），同时提供当前批次ID
    处理自行解决
    :param df: 传入的当前批次的数据（DataFrame对象），是批处理的DataFrame
    :param epoch_id: 批次ID
    :return: None没有
    """
    print(f"当前批次{epoch_id}，数据：")
    df.show()




df.writeStream.foreachBatch(func).\
    outputMode("append").\
    start().\
    awaitTermination()
