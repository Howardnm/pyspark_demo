# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5], numSlices=3)

    # repartition和coalesce算子用于修改RDD的分区数
    # 不同点在于repartition会进行shuffle操作，而coalesce默认不进行shuffle操作
    # 实际上，repartition是调用coalesce并设置shuffle=True实现的

    # repartition 修改分区，会进行shuffle，只能增加或减少分区
    print(rdd.repartition(1).getNumPartitions())
    print(rdd.repartition(5).getNumPartitions())

    # coalesce 修改分区，默认不进行shuffle，只能减少分区
    print(rdd.coalesce(1).getNumPartitions())
    print(rdd.coalesce(5).getNumPartitions())

    print(rdd.coalesce(1).getNumPartitions())
    print(rdd.coalesce(5, shuffle=True).getNumPartitions())

    sc.stop()

# 运行结果
# 1
# 5
# 1
# 3
# 1
# 5