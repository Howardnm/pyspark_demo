# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('E', 1), ('C', 1), ('D', 1), ('b', 1), ('g', 1), ('f', 1),
                          ('y', 1), ('u', 1), ('i', 1), ('o', 1), ('p', 1),
                          ('m', 1), ('n', 1), ('j', 1), ('k', 1), ('l', 1)], numSlices=3)

    # 按key进行排序，忽略大小写
    print(rdd.sortByKey(ascending=True, numPartitions=1, keyfunc=lambda key: str(key).lower()).collect())
    sc.stop()

# 输出结果：
# [('a', 1), ('b', 1), ('C', 1), ('D', 1), ('E', 1), ('f', 1), ('g', 1), ('i', 1), ('j', 1), ('k', 1), ('l', 1), ('m', 1), ('n', 1), ('o', 1), ('p', 1), ('u', 1), ('y', 1)]