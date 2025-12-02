# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], numSlices=3)
    # foreach算子不会有返回值
    # 作用是对RDD中的每个元素执行指定的函数，但不会返回新的RDD，而是直接在每个元素上执行操作
    # 注意：foreach算子是在集群的每个Executor节点上执行的，不需要收集到Driver端
    # 场景1：通常用于执行副作用操作，如打印日志、更新外部存储等
    # 场景2：在分布式计算中，可以用于在每个节点上执行sql语句，更新数据库等操作
    result = rdd.foreach(lambda x: x * 10)
    print(result)  # None

    # 使用foreach算子在每个节点上打印每个元素乘以10的结果
    rdd.foreach(lambda x: print(x * 10))
    sc.stop()

# 输出结果：
# None
# 10
# 30
# 20
# 40
# 70
# 90
# 60
