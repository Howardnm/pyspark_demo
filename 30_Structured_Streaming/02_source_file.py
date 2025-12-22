# coding:utf8
"""
演示Spark结构化流 读取（监听）一个文件夹中 的文件
只监听新增的文件，读取新文件中的内容
不监听文件的修改和删除
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

# 构建SparkSession对象
spark = SparkSession.builder.\
    master("local[*]").\
    config("spark.sql.shuffle.partitions", 1).\
    appName("test").getOrCreate()

schema = StructType().\
    add("name", StringType()).\
    add("age", IntegerType()).\
    add("hobby", StringType())

# TODO: 读取文件数据源
# 通过标准格式读取
df = spark.readStream.\
    format("csv").\
    option("sep", ";").\
    schema(schema).\
    load("../data/input/csv")

# 将内容输出到控制台
df.writeStream.\
    outputMode("append").\
    format("console").\
    start().\
    awaitTermination()