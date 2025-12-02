# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], numSlices=1)
    # 获取RDD中前n个元素，默认升序排列
    print(rdd.takeOrdered(3))
    # 获取RDD中后n个元素，通过传入一个函数，实现降序排列
    print(rdd.takeOrdered(3, lambda x: -x))

    sc.stop()

# 结果
# [1, 2, 3]
# [9, 7, 6]