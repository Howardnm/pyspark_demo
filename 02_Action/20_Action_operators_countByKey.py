# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("../data/input/words.txt")
    rdd2 = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1))
    print(rdd2.collect())
    # 通过countByKey来对key进行计数（每个相同的key分别有多少个）, 这是一个Action算子
    result = rdd2.countByKey()

    print(result)
    print(type(result))

    sc.stop()

# 运行结果如下：
# [('hello', 1), ('spark', 1), ('hello', 1), ('hadoop', 1), ('hello', 1), ('flink', 1)]
# defaultdict(<class 'int'>, {'hello': 3, 'spark': 1, 'hadoop': 1, 'flink': 1})
# <class 'collections.defaultdict'>