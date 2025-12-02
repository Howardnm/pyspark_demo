# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 1), ('b', 1)])
    rdd = rdd.groupByKey()
    print(rdd.collect())
    print(rdd.map(lambda x: (x[0], list(x[1]))).collect())

    sc.stop()

# Output:
# [('b', <pyspark.resultiterable.ResultIterable object at 0x7eeb75eddf70>), ('a', <pyspark.resultiterable.ResultIterable object at 0x7eeb74de40d0>)]
# [('b', [1, 1, 1]), ('a', [1, 1])]