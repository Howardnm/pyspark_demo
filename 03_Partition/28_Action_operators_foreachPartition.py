# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], numSlices=3)

    # foreachPartition算子不会有返回值
    # foreachPartition和foreach类似，都是对RDD中的每个元素进行操作，但有以下区别：
        # 1. foreach是对RDD中的每个元素进行操作
        # 2. foreachPartition是对RDD中的每个分区进行操作
    # 作用是减少driver和executor之间的数据传输次数，从而提高性能。
    # 场景：在分布式计算中，可以用于在每个分区上执行sql语句，更新数据库等操作

    def process(iter):
        result = list()
        for it in iter:
            result.append(it * 10)

        print(result)

    print(rdd.foreachPartition(process))

    sc.stop()
# 结果打印在executor的控制台上
# [20, 40]
# [70, 90, 60]
# [10, 30]
# None