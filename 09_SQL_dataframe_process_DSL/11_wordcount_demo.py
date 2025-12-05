# coding:utf8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
from pyspark.sql import functions as F


if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    # TODO 1: SQL 风格进行处理
    rdd = sc.textFile("../data/input/words.txt").\
        flatMap(lambda x: x.split(" ")).\
        map(lambda x: [x])

    df = rdd.toDF(["word"])
    df.show()
    # +--------+
    # | word   |
    # +--------+
    # | hello  |
    # | spark  |
    # | hello  |
    # | hadoop |
    # | hello  |
    # | flink  |
    # +--------+
    # 注册DF为表格
    df.createTempView("words")

    spark.sql("SELECT word, COUNT(*) AS cnt FROM words GROUP BY word ORDER BY cnt DESC").show()

    # +--------+-----+
    # | word   | cnt |
    # +--------+-----+
    # | hello  |  3  |
    # | spark  |  1  |
    # | hadoop |  1  |
    # | flink  |  1  |
    # +--------+-----+

    # ------------------------------------------------------------------
    # TODO 2: DSL 风格处理
    df = spark.read.format("text").load("../data/input/words.txt")
    df.show()
    # +--------------+
    # | value        |
    # +--------------+
    # | hello spark  |
    # | hello hadoop |
    # | hello flink  |
    # +--------------+
    # withColumn方法
    # 方法功能: 对已存在的列进行操作, 返回一个新的列。如果新列名字和老列相同, 则替换, 否则作为新列存在
    df2 = df.withColumn("value", F.explode(F.split(df['value'], " ")))
    # explode: 将数组或map类型的列进行拆分, 每个元素作为一行输出
    # split: 按照指定的分隔符, 将字符串拆分成数组

    df2.show()
    # +------+
    # | value|
    # +------+
    # | hello|
    # | spark|
    # | hello|
    # |hadoop|
    # | hello|
    # | flink|
    # +------+
    df2.groupBy("value").\
        count().\
        withColumnRenamed("value", "word").\
        withColumnRenamed("count", "cnt").\
        orderBy("cnt", ascending=False).\
        show()

    # withColumnRenamed: 重命名列
    # 方法功能: 对DataFrame中的列进行重命名, 一次只能重命名一个列

    # +------+---+
    # |  word|cnt|
    # +------+---+
    # | hello|  3|
    # | spark|  1|
    # | flink|  1|
    # |hadoop|  1|
    # +------+---+