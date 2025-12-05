# coding:utf8
import string

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType

if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        config("spark.sql.shuffle.partitions", 2). \
        getOrCreate()
    sc = spark.sparkContext

    # TODO 0: 构建一个DataFrame
    rdd = sc.parallelize([[1], [2], [3]])
    df = rdd.toDF(["num"])
    df.show()
    # +---+
    # |num|
    # +---+
    # |  1|
    # |  2|
    # |  3|
    # +---+

    # TODO 0.1: 定义UDF的处理逻辑
    # UDF的返回值是字典, 用ascii_letters字符串来模拟字母表
    def process(data):
        return {"num": data, "letters": string.ascii_letters[data]}


    # TODO 1: 方式1 sparksession.udf.register(), DSL和SQL风格均可以使用
    # UDF的返回值是字典的话, 需要用StructType来接收
    udf1 = spark.udf.register("udf1", process, StructType()
                              .add("num", IntegerType(), nullable=True)
                              .add("letters", StringType(), nullable=True)
                              )

    # TODO 1.1: SQL风格中使用
    df.selectExpr("udf1(num)").show(truncate=False)
    # +---------+
    # |udf1(num)|
    # +---------+
    # |{1, b}   |
    # |{2, c}   |
    # |{3, d}   |
    # +---------+

    # TODO 1.2: DSL风格中使用
    df.select(udf1(df['num'])).show(truncate=False)
    # +---------+
    # |udf1(num)|
    # +---------+
    # |{1, b}   |
    # |{2, c}   |
    # |{3, d}   |
    # +---------+