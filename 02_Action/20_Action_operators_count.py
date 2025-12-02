# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 3, 3, 1])
    # 计算RDD中元素的个数，返回值是一个Long类型的值
    print(rdd.count())

    sc.stop()

# 运行结果如下：
# 5
