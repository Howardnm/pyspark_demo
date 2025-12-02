# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], numSlices=3)

    # mapPartitions和map的效果一样，但区别在于：
    # map是对RDD中的每个元素进行处理，而mapPartitions是对RDD中的每个分区进行处理。
    # 这样可以减少网络I0的次数，从而提高性能。
        # 因为用map，数据在driver端和executor端传输的次数是RDD中元素的个数，
        # 而用mapPartitions，数据在driver端和executor端传输的次数是RDD的分区数。

    # 构建一个函数，处理每个分区的数据
    def process(iter):
        result = list()
        for it in iter:
            result.append(it * 10)

        return result

    # 把函数传入mapPartitions中，对每个分区的数据进行处理
    print(rdd.mapPartitions(process).collect())

    sc.stop()

# 结果：
# [10, 30, 20, 40, 70, 90, 60]