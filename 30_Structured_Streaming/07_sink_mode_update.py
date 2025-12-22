# coding:utf8
"""
演示Spark 输出模式：update
"""
# 1. 导包
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# 2. 构建执行入口，SparkSession对象
spark: SparkSession = SparkSession.builder.\
    appName("test").\
    master("local[*]").\
    config("spark.sql.shuffle.partitions", 1).\
    getOrCreate()

# 3. 构建Source：Socket
df: DataFrame = spark.readStream.\
    format("socket").\
    option("host", "ct104").\
    option("port", "9999").\
    load()

# 4. 数据处理 ： 略
word_df = df.select(F.explode(F.split("value", " ")).alias("word"))
# TODO 0： Complete 模式必须使用聚合函数！！！
result_df = word_df.groupby("word").count()

# TODO 1: sink 输出模式 Update
# Update 模式说明：
# 更新模式（Update）：每次只输出自上次触发以来发生变化的数据行，适用于需要关注变化的场景。
# 适用场景：
# 1. 实时监控：关注关键指标的变化，如异常检测。
# 2. 增量更新：只处理和存储发生变化的数据，节省资源。
# 3. 实时推荐系统：根据用户行为的变化动态调整推荐结果。
# 5. 数据输出
result_df.writeStream.\
    format("console").\
    outputMode("update").\
    start().\
    awaitTermination()

# 输出模式有三种：Append、Complete、Update
# Append：追加模式：只输出新增的数据行。Append 模式不可以使用聚合函数！！！
# Complete：完整模式：输出所有的数据行。Complete 模式必须使用聚合函数！！！
# Update：更新模式：只输出更新的数据行。不支持排序。
# Update：没有聚合函数时，等同于 Append 模式；有聚合函数时，等同于 Complete 模式，但不输出未变化的数据行。