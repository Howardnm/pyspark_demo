# coding:utf8
import string
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
import pandas as pd
from pyspark.sql import functions as F


if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = (SparkSession.builder
             .appName("test")
             .master("local[*]") # 本地运行
             .config("spark.sql.shuffle.partitions", 2)

             .config("spark.sql.warehouse.dir", "hdfs://ct104:8020/user/hive/warehouse")
             .config("hive.metastore.uris", "thrift://ct104:9083")
             .enableHiveSupport()
             .getOrCreate()
             )
    # spark.sql.warehouse.dir  配置hive warehouse的地址, 这是hive表数据存储的默认位置
    # hive.metastore.uris      配置hive metastore的地址
    # .enableHiveSupport()     启用hive支持

    sc = spark.sparkContext

    spark.sql("SELECT * FROM test").show()
    spark.sql("show databases").show()
    # +---------+
    # |namespace|
    # +---------+
    # |   db_msg|
    # |  default|
    # |  itheima|
    # |   myhive|
    # +---------+


