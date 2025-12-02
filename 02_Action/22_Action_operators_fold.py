# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], numSlices=3)
    # fold算子：aggregate的简化版，初始值在每个分区都会加上
    # fold(v0, func)：v0表示初始值，func表示计算函数
    # 计算过程：
    # 分区内聚合+10：
    # 分区1：1,2,3  => ((10+1)+2+3)=16
    # 分区2：4,5,6  => ((10+4)+5+6)=25
    # 分区3：7,8,9  => ((10+7)+8+9)=34
    # 分区间聚合+10：
    # 最后再把各个分区的结果相加：10 + 16 + 25 + 34 = 85
    # 注意：初始值在每个分区都会加上一次，最后分区聚合再加一次
    print(rdd.fold(10, lambda a, b: a + b))


    sc.stop()

# 输出结果：
# 85