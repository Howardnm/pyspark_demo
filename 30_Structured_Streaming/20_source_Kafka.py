# conding:utf8
"""
演示从Kafka中读取数据
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

# 1. 设置环境变量，让 Spark 自动下载匹配的包
import pyspark
print(pyspark.__version__)
# 注意：把 3.2.0 换成你实际的 Spark 版本号 (print(spark.version))
# 格式为: org.apache.spark:spark-sql-kafka-0-10_2.12:YOUR_SPARK_VERSION
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
# 他会自动把jar依赖包下载到：~/.ivy2/jars 目录下
# 想要永久化jar包，可以把包复制到 cd /export/server/anaconda3/envs/pyspark/lib/python3.8/site-packages/pyspark/jars
# 复制的时候，jar要改名，例如：【org.apache.kafka_kafka-clients-2.8.0.jar】 改成 【kafka-clients-2.8.0.jar】

# 2. 构建执行入口，SparkSession对象
spark: SparkSession = SparkSession.builder.\
    appName("test").\
    master("local[*]").\
    config("spark.sql.shuffle.partitions", 1).\
    getOrCreate()

# 读取kafka数据
kafka_df = spark.readStream.format("kafka").\
    option("kafka.bootstrap.servers", "ct104:9092, ct105:9092, ct106:9092").\
    option("subscribePattern", "spark*").\
    load()

# TODO 3.0: 注意
# - subscribe
# - subscribePattern
# - assigin
# 三个参数三选一，它们之间是互斥的。
# TODO 3.1: subscribe 可以消费多个主题：
    # .option("subscribe", "topic1,topic2")
# TODO 3.2: subscribePattern 通过正则订阅符合名字的主题
    # .option("subscribePattern", "spark*").\
# TODO 3.3: assign 指定主题的指定分区的指定offset开始进行消费
    # option("assign", '{"spark1": [0], "spark2": [0, 2]}').\
    # 例如："spark3": [0,2]
    # 表示从spark3主题中的【0号、2号分区】消费。
# TODO 3.4: 订阅多个Topic, 明确指定偏移量
    # 1、不写, 默认从最早到最晚的偏移量范围
    # 2、明确指定偏移量
    # .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""") \
    # .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")
    # 3、指定从最早到最晚的偏移量范围
    # .option("startingOffsets", "earliest") \
    # .option("endingOffsets", "latest") \

# 将kafka中的value字节数组转换回字符串
kafka_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset")

kafka_df.writeStream.format("console").\
    outputMode("append").\
    option("truncate", False).\
    start().\
    awaitTermination()

# -------------------------------------------
# Batch: 1
# -------------------------------------------
# +----+-------------------+------+---------+------+-----------------------+-------------+
# |key |value              |topic |partition|offset|timestamp              |timestampType|
# +----+-------------------+------+---------+------+-----------------------+-------------+
# |NULL|[31 32 33 34 35 36]|spark1|0        |2     |2025-12-19 11:48:36.459|0            |
# +----+-------------------+------+---------+------+-----------------------+-------------+

# 转成字符串：kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")
# -------------------------------------------
# Batch: 1
# -------------------------------------------
# +------+
# |value |
# +------+
# |123456|
# +------+