# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 2), ('c', 3)])
    print(rdd.mapValues(lambda x: x * 10).collect())

    sc.stop()

# Output:
# [('a', 10), ('b', 20), ('c', 30)]