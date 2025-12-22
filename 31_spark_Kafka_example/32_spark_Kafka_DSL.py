# coding:utf8
"""
从kafka中取到物联网数据，进行数据分析
kafka的主题设置为：iot
1. 从kafka中取到对应的数据，kafka source
2. 将数据进行处理完成分析的需求
    2.1 过滤信号大于30的数据
    2.2 按照类型分组
    2.3 求分组的count聚合 和 信号强度的avg聚合
3. 将结果输出到控制台中
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# 执行环境入口对象
spark = SparkSession.builder.\
    master("local[*]").\
    appName("iot_analysis").\
    config("spark.sql.shuffle.partitions", 1).\
    getOrCreate()

# 1. 获取kafka的数据
kafka_df = spark.readStream.format("kafka").\
    option("kafka.bootstrap.servers", "ct104:9092, ct105:9092, ct106:9092").\
    option("subscribe", "iot").\
    load()

# 2. 对数据进行处理
# 2.1 数据是字节数组，需要转换成明文字符串
# df(
#   value
#   {'deviceID': 'device_5_14', 'deviceType': '窗户', 'deviceSignal': 16, 'time': '20220702'}
#   {'deviceID': 'device_1_17', 'deviceType': '油烟机', 'deviceSignal': 93, 'time': '20220702'}
# )
# { "a": {"b": 1}  }
# $.a.b  # 可以取到1
json_df = kafka_df.selectExpr("CAST(value AS STRING)")

# 2.2 对数据（JSON）进行处理，将一条JSON数据，变成4个列
df = json_df.select(
    F.get_json_object("value", '$.deviceID').alias("deviceID"),
    F.get_json_object("value", '$.deviceType').alias("deviceType"),
    F.get_json_object("value", '$.deviceSignal').alias("deviceSignal"),
    F.get_json_object("value", '$.time').alias("time")
)

# 2.3 通过SQL语法来做，注册临时视图 createTempView
df.createTempView("iot")
# 2.4 通过SQL语句分析
result_df = spark.sql("""
    SELECT deviceType, COUNT(deviceType) AS cnt, AVG(deviceSignal) AS avg_signal 
    FROM iot WHERE deviceSignal > 30 GROUP BY deviceType ORDER BY avg_signal DESC
""")

# 3. 打印输出
result_df.writeStream.format("console").\
    outputMode("complete").\
    trigger(processingTime='5 seconds').\
    start().\
    awaitTermination()
