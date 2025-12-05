# coding:utf8
import string
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

    # TODO 使用RDD的mapPartitions算子来实现UDAF聚合功能
    # TODO 需求: 对DataFrame中的num列进行求和聚合操作

    # TODO 0: 创建DataFrame
    rdd = sc.parallelize([1, 2, 3, 4, 5], 3).map(lambda x: [x])
    print(rdd.collect())
    # [[1], [2], [3], [4], [5]]
    df = rdd.toDF(['num'])
    df.show(truncate=False)
    # +---+
    # |num|
    # +---+
    # |1  |
    # |2  |
    # |3  |
    # |4  |
    # |5  |
    # +---+

    # TODO 0.1: 定义聚合函数
    def process(iter):
        sum = 0
        for row in iter:
            sum += row['num']  # row是Row对象, 在一行row数据中，取出num列的值进行累加

        return [sum]    # 一定要嵌套list, 因为mapPartitions方法要求的返回值是list对象

    # TODO 1: 将DataFrame转换为单分区的RDD
    # 折中的方式 就是使用RDD的mapPartitions 算子来完成聚合操作
    # 如果用mapPartitions API 完成 UDAF 聚合, 一定要单分区
    single_partition_rdd = df.rdd.repartition(1)
    print(single_partition_rdd.collect())
    # [Row(num=1), Row(num=2), Row(num=3), Row(num=4), Row(num=5)]

    # TODO 2: 使用mapPartitions算子完成UDAF聚合功能
    results = single_partition_rdd.mapPartitions(process).collect()
    print(results)
    # [15]