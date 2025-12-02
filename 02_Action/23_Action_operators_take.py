# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 5, 3, 1, 3, 2, 6, 7, 8, 6])
    # take(n)返回RDD中的前n个元素，返回值是一个list
    print(rdd.take(5))

    sc.stop()

# 输出结果: [1, 3, 5, 3, 1]