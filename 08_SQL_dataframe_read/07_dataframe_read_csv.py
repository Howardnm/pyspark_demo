# coding:utf8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd


if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    # 读取CSV文件
    df = spark.read.format("csv")\
        .option("sep", ";")\
        .option("header", True)\
        .option("encoding", "utf-8")\
        .schema("name STRING, age INT, job STRING")\
        .load("../data/input/sql/people.csv")

    # option-sep: 设置分隔符
    # option-header: csv是否包含表头
    # option-encoding: 设置文件编码格式

    df.printSchema()
    df.show(5)


# 运行结果
# root
#  |-- name: string (nullable = true)
#  |-- age: integer (nullable = true)
#  |-- job: string (nullable = true)
#
# +-----+---+---------+
# | name|age|      job|
# +-----+---+---------+
# |Jorge| 30|Developer|
# |  Bob| 32|Developer|
# |  Ani| 11|Developer|
# | Lily| 11|  Manager|
# |  Put| 11|Developer|
# +-----+---+---------+
# only showing top 5 rows