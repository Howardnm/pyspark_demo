# coding:utf8
import time

from pyspark import SparkConf, SparkContext
import os

os.environ['HADOOP_USER_NAME'] = 'hadoop'

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # CheckPoint与Cache/Persist的区别：
    # 1. Cache/Persist是将数据临时缓存在每个Executor的内存或磁盘中，程序执行结束后，数据会丢失
    #    CheckPoint是将数据可靠地保存到HDFS中，程序执行结束后，数据依然存在
    # 2. Cache/Persist的容错机制是通过数据的血缘关系来实现的
    #    CheckPoint的容错机制是通过将数据保存到HDFS中来实现的，他并没有保存数据的血缘关系

    # 1. 告知spark, 开启CheckPoint功能, 设置缓存目录
    # 注意：该目录必须是HDFS上的路径
    # 因为CheckPoint的容错机制是依赖于HDFS的高可靠性的
    sc.setCheckpointDir("hdfs://ct104:8020/output/ckp")

    rdd1 = sc.textFile("../data/input/words.txt")
    rdd2 = rdd1.flatMap(lambda x: x.split(" "))
    rdd3 = rdd2.map(lambda x: (x, 1))

    # 调用checkpoint API 保存数据即可
    rdd3.checkpoint()

    rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
    print(rdd4.collect())

    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x: sum(x))
    print(rdd6.collect())

    # 释放rdd3的缓存，但CheckPoint数据依然保存在HDFS中
    rdd3.unpersist()

    # 删除在HDFS上保存的CheckPoint数据
    # import shutil
    # shutil.rmtree("hdfs://ct104:8020/output/ckp")

    # 保持程序运行状态，方便在Spark UI中查看CheckPoint信息，http://ct104:4040
    # time.sleep(100000)

    sc.stop()

# 结果：
# [('hadoop', 1), ('hello', 3), ('spark', 1), ('flink', 1)]
# [('hadoop', 1), ('hello', 3), ('spark', 1), ('flink', 1)]