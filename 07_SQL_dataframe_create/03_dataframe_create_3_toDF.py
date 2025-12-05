# coding:utf8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType


if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    # 基于RDD转换成DataFrame
    rdd = sc.textFile("../data/input/sql/people.txt").\
        map(lambda x: x.split(",")).\
        map(lambda x: (x[0], int(x[1])))

    # =================================================================
    # toDF的方式构建DataFrame
    df1 = rdd.toDF(["name", "age"])

    # ------------------------------------------------------------------
    df1.printSchema()
    df1.show()

    # =================================================================
    # toDF的方式2 通过StructType来构建
    schema = StructType()\
        .add("name", StringType(), nullable=True)\
        .add("age", IntegerType(), nullable=False)

    df2 = rdd.toDF(schema=schema)
    # ------------------------------------------------------------------
    df2.printSchema()
    df2.show()


# 输出结果:
# 1、toDF方式1 构建的表结构:
# root
#  |-- name: string (nullable = true)
#  |-- age: long (nullable = true)
#
# 2、toDF方式1 构建的数据内容:
# +-------+---+
# |   name|age|
# +-------+---+
# |Michael| 29|
# |   Andy| 30|
# | Justin| 19|
# +-------+---+
# -----------------------------------------
# 3、toDF方式2 构建的表结构:
# root
#  |-- name: string (nullable = true)
#  |-- age: integer (nullable = false)
#
# 4、toDF方式2 构建的数据内容:
# +-------+---+
# |   name|age|
# +-------+---+
# |Michael| 29|
# |   Andy| 30|
# | Justin| 19|
# +-------+---+