# coding:utf8
import time

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.textFile("../data/input/words.txt")
    rdd2 = rdd1.flatMap(lambda x: x.split(" "))
    rdd3 = rdd2.map(lambda x: (x, 1))

    # 把 RDD3 加入缓存进行优化，提高后续计算的效率，避免rdd3被重复计算
    # Spark提供了多种缓存级别，常用的有以下几种：
    # rdd3.cache()  # 缓存到内存中
    # rdd3.persist(StorageLevel.MEMORY_ONLY)  # 仅内存缓存，和rdd3.cache()一样
    # rdd3.persist(StorageLevel.MEMORY_ONLY_2)  # 仅内存缓存，2个副本
    # rdd3.persist(StorageLevel.DISK_ONLY)  # 仅缓存硬盘上
    # rdd3.persist(StorageLevel.DISK_ONLY_2)  # 仅缓存硬盘上，2个副本
    # rdd3.persist(StorageLevel.DISK_ONLY_3)  # 仅缓存硬盘上，3个副本
    # rdd3.persist(StorageLevel.MEMORY_AND_DISK)  # 先放内存，不够放硬盘
    # rdd3.persist(StorageLevel.MEMORY_AND_DISK_2)  # 先放内存，不够放硬盘，2个副本
    # rdd3.persist(StorageLevel.OFF_HEAP)  # 堆外内存（系统内存）

    # 如上API，自行选择使用即可
    # 一般建议使用rdd3.persist(StorageLevel.MEMORY_AND_DISK)
    # 如果内存比较小的集群，建议使用rdd3.persist(StorageLevel.DISK_ONLY) 或者就别用缓存了 用CheckPoint

    rdd3.persist(StorageLevel.MEMORY_AND_DISK_2)

    rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
    print(rdd4.collect())

    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x: sum(x))
    print(rdd6.collect())

    # 释放缓存
    rdd3.unpersist()

    # 保持程序运行状态，方便在Spark UI中查看缓存信息，http://ct104:4040
    # time.sleep(100000)
    sc.stop()

# 结果：
# [('hadoop', 1), ('hello', 3), ('spark', 1), ('flink', 1)]
# [('hadoop', 1), ('hello', 3), ('spark', 1), ('flink', 1)]