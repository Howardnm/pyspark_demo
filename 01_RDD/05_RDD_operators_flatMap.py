# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 案例1，将每行的单词拆分开来
    rdd = sc.parallelize(["hadoop spark hadoop", "spark hadoop hadoop", "hadoop flink spark"])
    # 得到所有的单词, 组成RDD, flatMap的传入参数 和map一致, 就是给map逻辑用的, 解除嵌套无需逻辑(传参)
    rdd = rdd.flatMap(lambda line: line.split(" "))
    print(rdd.collect())

    # 案例2，将嵌套的列表解除嵌套
    rdd = sc.parallelize([[1, 2, 3], [4, 5], [6]])
    rdd = rdd.flatMap(lambda x: x)
    print(rdd.collect())
    sc.stop()

# 输出结果:
# ['hadoop', 'spark', 'hadoop', 'spark', 'hadoop', 'hadoop', 'hadoop', 'flink', 'spark']
# [1, 2, 3, 4, 5, 6]