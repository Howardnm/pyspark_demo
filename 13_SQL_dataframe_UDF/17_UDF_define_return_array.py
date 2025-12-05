# coding:utf8
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
import pandas as pd
from pyspark.sql import functions as F


if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions", 2).\
        getOrCreate()
    sc = spark.sparkContext

    # TODO 0: 构建一个DataFrame
    rdd = sc.parallelize([["hadoop spark flink"], ["hadoop flink java"]])
    df = rdd.toDF(["line"])
    df.cache()
    df.show()
    # +------------------+
    # |              line|
    # +------------------+
    # |hadoop spark flink|
    # | hadoop flink java|
    # +------------------+

    # TODO 0.1: 定义UDF的处理逻辑
    def split_line(data):
        return data.split(" ")  # 返回值是一个Array对象


    # TODO 1: 方式1 sparksession.udf.register(), DSL和SQL风格均可以使用
    udf2 = spark.udf.register("udf1", split_line, ArrayType(StringType()))

    # TODO 1.1: DSL风格中使用
    df.select(udf2(df['line'])).show()
    # +--------------------+
    # |          udf1(line)|
    # +--------------------+
    # |[hadoop, spark, f...|
    # |[hadoop, flink, j...|
    # +--------------------+
    # TODO 1.2: SQL风格中使用
    df.createTempView("lines")
    spark.sql("SELECT udf1(line) FROM lines").show(truncate=False)  # truncate=False 表示不截断显示
    # +----------------------+
    # |udf1(line)            |
    # +----------------------+
    # |[hadoop, spark, flink]|
    # |[hadoop, flink, java] |
    # +----------------------+

    # TODO 2 方式2的形式构建UDF
    udf3 = F.udf(split_line, ArrayType(StringType()))
    df.select(udf3(df['line'])).show(truncate=False)
    # +----------------------+
    # |split_line(line)      |
    # +----------------------+
    # |[hadoop, spark, flink]|
    # |[hadoop, flink, java] |
    # +----------------------+

