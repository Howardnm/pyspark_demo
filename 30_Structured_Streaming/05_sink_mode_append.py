# coding:utf8
"""
演示Spark 输出模式：Append
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
result_df = df.select(F.explode(F.split("value", " ")))

# TODO: sink 输出模式 Append
# Append 模式说明：
# 追加模式（Append）：只输出新增加的数据行，适用于只关心新增数据的场景。
# 适用场景：
# 1. 日志处理：实时处理和存储新增的日志数据。
# 2. 事件流处理：处理新增的事件数据，如用户点击、传感器数据等。
# 3. 实时监控：监控系统状态的变化，只关注新增的告警或状态变化。

# 数据输出
result_df.writeStream.\
    format("console").\
    outputMode("append").\
    start().\
    awaitTermination()

# 输出模式有三种：Append、Complete、Update
# Append：追加模式：只输出新增的数据行。Append 模式不可以使用聚合函数！！！
# Complete：完整模式：输出所有的数据行
# Update：更新模式：只输出更新的数据行