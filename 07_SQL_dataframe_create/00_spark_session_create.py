# coding:utf8

# SparkSession对象的导包, 对象是来自于 pyspark.sql包中
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = (SparkSession.builder
             .appName("test")
             .master("local[*]")
             .getOrCreate()
             )

    # 可以通过SparkSession对象 获取 SparkContext对象
    sc = spark.sparkContext

    # SparkSQL的HelloWorld
    df = spark.read.csv("../data/input/stu_score.txt", sep=',', header=False)
    df2 = df.toDF("id", "name", "score")
    df2.printSchema()
    df2.show(5)

    # 创建临时视图
    df2.createTempView("score")

    # SQL 风格
    spark.sql("""
        SELECT * FROM score WHERE name='语文' LIMIT 5
    """).show()

    # DSL 风格
    df2.where("name='语文'").limit(5).show()

# 结果:
# root
#  |-- id: string (nullable = true)
#  |-- name: string (nullable = true)
#  |-- score: string (nullable = true)
#
# +---+----+-----+
# | id|name|score|
# +---+----+-----+
# |  1|语文|   99|
# |  2|语文|   99|
# |  3|语文|   99|
# |  4|语文|   99|
# |  5|语文|   99|
# +---+----+-----+
# only showing top 5 rows
#
# +---+----+-----+
# | id|name|score|
# +---+----+-----+
# |  1|语文|   99|
# |  2|语文|   99|
# |  3|语文|   99|
# |  4|语文|   99|
# |  5|语文|   99|
# +---+----+-----+
#
# +---+----+-----+
# | id|name|score|
# +---+----+-----+
# |  1|语文|   99|
# |  2|语文|   99|
# |  3|语文|   99|
# |  4|语文|   99|
# |  5|语文|   99|
# +---+----+-----+
