# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], numSlices=2)

    # 如果不用累加器，是这样的：
    # acmlt = 0
    # Spark提供的累加器变量, 参数是初始值
    acmlt = sc.accumulator(0)

    def map_func(data):
        global acmlt
        acmlt += 1
        print(acmlt)

    rdd2 = rdd.map(map_func)
    rdd2.cache()    # 这里特意使用了cache，来证明有了cache缓存，下面rdd3就不会重新走一遍rdd2的血缘关系了
                    # 换句话说，map_func不会再次执行，所以只会迭代到10，而不是20
    rdd2.collect()
    print(acmlt)

    rdd3 = rdd2.map(lambda x:x)
    rdd3.collect()
    print(acmlt)

    sc.stop()

# 输出结果
# 1
# 2
# 3
# 4
# 5
# 1
# 2
# 3
# 4
# 5
# 10 # 当没用累加器时: 0，原因是drive的变量没有被Executor节点的迭代器所影响
# 10 # 当没用累加器时: 0，原因是drive的变量没有被Executor节点的迭代器所影响
























