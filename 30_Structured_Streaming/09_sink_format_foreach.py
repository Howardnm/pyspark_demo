# coding:utf8
"""
演示：Spark结构化流的输出位置：foreach方式
"""

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame, Row

# 构建Spark执行环境入口对象
spark = (SparkSession.builder
         .master("local[*]")
         .appName("test")
         .config("spark.sql.shuffle.partitions", 1)
         .config("spark.default.parallelism", 1) # 设置并行度
         .getOrCreate()
         )

# 构建Source：Socket
df: DataFrame = spark.readStream. \
    format("socket"). \
    option("host", "ct104"). \
    option("port", "9999"). \
    load()


# 定义处理函数
# 方式1：
def func(row: Row):
    """
    func这个函数，会被Spark自动调用，将要输出的数据```一条条```的传入进来
    至于如何处理，随意，自己爱怎么写怎么写。
    :param row: Row对象，一行数据
    :return: None，不需要返回值
    """
    print(f"我得到一行数据：{row}")

# 方式2：
class MyClass:
    def open(self, partition_id, epoch_id):
        """
        在调用process前，先调用open
        一般用来打开数据库链接
        :param partition_id: 当前被处理的数据的所在分区，分区数量由并行度控制
        :param epoch_id: 当前被处理数据的所在 批次
        :return: True or False， True表示一切正常可以执行process
        """
        print(f"我open了：当前数据分区：{partition_id}，当前数据批次：{epoch_id}")
        return True

    def process(self, row: Row):
        """
        会被Spark自动调用，将要输出的数据```一条条```的传入进来
        至于如何处理，随意，自己爱怎么写怎么写。
        :param row: Row对象，传给你的数据自行处理
        :return: None，不需要
        """
        f = open("/tmp/process.txt", "a", encoding="UTF-8")
        f.write(str(row))
        f.write("\n")
        f.close()

    def close(self, error):
        """
        当出现错误的时候调用close，process处理完的时候调用close
        :param error:
        :return:
        """
        print(f"我close了, {error}")


(df.writeStream
 # .foreach(func)
 .foreach(MyClass())
 .outputMode("append")
 .start()
 .awaitTermination()
 )
