# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 5, 3, 1, 3, 2, 6, 7, 8, 6])
    # 将RDD降序排列，取前5个元素，返回一个列表
    # (即：取出RDD中前N个最大的元素)
    print(rdd.top(5))

    sc.stop()

# 输出结果: [8, 7, 6, 6, 5]