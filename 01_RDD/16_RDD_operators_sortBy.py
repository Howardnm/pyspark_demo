# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('c', 3), ('f', 1), ('b', 11), ('c', 3), ('a', 1), ('c', 5), ('e', 1), ('n', 9), ('a', 1)], numSlices=3)

    # 使用sortBy对rdd执行排序

    # 按照value 数字进行排序
    # 参数1函数, 表示的是 ,  告知Spark 按照数据的哪个列进行排序
    # 参数2: True表示升序 False表示降序
    # 参数3: 排序的分区数 如果分区≥2 则每个分区内有序, 但是分区间无序
    """注意: 如果要全局有序, 排序分区数请设置为1"""
    print(rdd.sortBy(lambda x: x[1], ascending=True, numPartitions=1).collect())

    # 按照key来进行排序
    print(rdd.sortBy(lambda x: x[0], ascending=False, numPartitions=1).collect())

    sc.stop()

# 运行结果
# [('f', 1), ('a', 1), ('e', 1), ('a', 1), ('c', 3), ('c', 3), ('c', 5), ('n', 9), ('b', 11)]
# [('n', 9), ('f', 1), ('e', 1), ('c', 3), ('c', 3), ('c', 5), ('b', 11), ('a', 1), ('a', 1)]