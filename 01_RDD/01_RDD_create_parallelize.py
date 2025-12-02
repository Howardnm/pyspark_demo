# coding:utf-8
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("CreateRDD_Parallelize")
    sc = SparkContext(conf=conf)

    # 使用parallelize方法创建RDD，即本地集合数据 --> 分布式数据集（RDD）
    data = [10, 20, 30, 40, 50]
    rdd = sc.parallelize(data)
    # rdd.parallelize默认分区数为机器的CPU核数，可以通过第二个参数指定分区数
    print(rdd.getNumPartitions())
    rdd = sc.parallelize(data, numSlices=3) # 指定分区数为3
    print(rdd.getNumPartitions())
    # collect方法将RDD中的数据收集到Driver端，返回一个python list对象，即分布式数据集 --> 本地数据集
    print(rdd.collect())  # 收集并打印RDD中的数据

    sc.stop()  # 停止SparkContext

# Output:
# 4
# 3
# [10, 20, 30, 40, 50]