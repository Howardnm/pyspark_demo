# coding:utf8
"""
演示Spark 输出模式：Complete
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

# TODO 1: sink 输出模式 Complete
# Complete 模式说明：
# 完整模式（Complete）：每次输出所有的聚合结果，适用于需要完整视图的场景。
# 适用场景：
# 1. 实时仪表盘：显示所有关键指标的最新状态。
# 2. 数据汇总：需要定期查看所有数据的汇总结果。
# 3. 统计分析：需要对所有数据进行统计分析和报告生成。
# 5. 数据输出
result_df.writeStream.\
    format("console").\
    outputMode("complete").\
    start().\
    awaitTermination()

# 输出模式有三种：Append、Complete、Update
# Append：追加模式：只输出新增的数据行。Append 模式不可以使用聚合函数！！！
# Complete：完整模式：输出所有的数据行。Complete 模式必须使用聚合函数！！！
# Update：更新模式：只输出更新的数据行。