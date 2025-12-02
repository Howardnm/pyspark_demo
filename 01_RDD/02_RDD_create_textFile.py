# coding:utf-8
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("CreateRDD_textFile")
    sc = SparkContext(conf=conf)

    # 使用textFile API方法创建RDD，即从外部存储系统（如HDFS、本地文件系统等）读取数据，创建RDD
    rdd = sc.textFile("../data/input/words.txt")  # 读取本地文件系统中的文本文件，创建RDD
    # textFile方法默认分区数根据文件大小和Hadoop配置参数确定，可以通过第二个参数指定分区数
    print(rdd.getNumPartitions())  # 打印RDD的分区数
    rdd = sc.textFile("../data/input/words.txt", minPartitions=3)  # 指定最小分区数为3
    print(rdd.getNumPartitions())  # 打印RDD的分区数
    # collect方法将RDD中的数据收集到Driver端，返回一个python list对象
    print(rdd.collect())  # 收集并打印RDD中的数据
    sc.stop()  # 停止SparkContext

# Output:
# 2
# 4
# ['hello spark', 'hello hadoop', 'hello flink']