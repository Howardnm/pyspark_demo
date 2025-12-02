# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], numSlices=2)

    print(rdd.glom().collect())

    sc.stop()

# 输出结果：
# [[1, 2, 3, 4], [5, 6, 7, 8, 9]]