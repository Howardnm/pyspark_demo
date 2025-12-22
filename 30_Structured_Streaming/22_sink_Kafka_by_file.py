# conding:utf8
"""
演示向Kafka中写入数据
socket -> spark -> kafka -> 模拟消费者取出
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

# 1. 设置环境变量，让 Spark 自动下载匹配的包
import pyspark
print(pyspark.__version__)
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# 2. 构建执行入口，SparkSession对象
spark: SparkSession = SparkSession.builder.\
    appName("test").\
    master("local[*]").\
    config("spark.sql.shuffle.partitions", 1).\
    getOrCreate()

# 3. 通过csv文件获取数据,可以指定topic、分区
df = spark.readStream.format("csv").\
    option("sep", ";").\
    schema("value STRING,topic STRING,partition INT").\
    load("../data/input/csv1")
# hello1;spark2;0
# hello2;spark3;0
# hello3;spark4;0

# 4. 输出到kafka
df.writeStream.format("kafka").\
    option("kafka.bootstrap.servers", "ct104:9092, ct105:9092, ct106:9092").\
    option("checkpointLocation", "../data/output/parquet_checkpoint1").\
    outputMode("append").\
    start().\
    awaitTermination()

# 5. 启动3个kafka消费者读取数据
# cd /export/server/kafka
# bin/kafka-console-consumer.sh --bootstrap-server ct104:9092,ct105:9092,ct106:9092 --topic spark2
# bin/kafka-console-consumer.sh --bootstrap-server ct104:9092,ct105:9092,ct106:9092 --topic spark3
# bin/kafka-console-consumer.sh --bootstrap-server ct104:9092,ct105:9092,ct106:9092 --topic spark4

# 写入Kafka要求：
# 1. 被写入的DataFrame中，`必须`包含一个列，叫做`value`，value列就是写入kafka的数据
# 2. 被写入的DataFrame中，`可选`包含如下列：
#    1. key，指定数据key
#    2. topic，指定数据写向哪个主题
#    3. partition，指定数据应该去往哪个分区
#    4. header，给数据加上标头