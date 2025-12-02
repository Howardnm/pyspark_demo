# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 5, 3, 1, 3, 2, 6, 7, 8, 6])
    # 采样数据，不放回抽样，抽取5个元素，随机种子为1
    # 结果每次运行都相同，因为设置了随机种子，保证了随机性的可重复性，随机种子相同，结果也相同
    # 原理： takeSample函数内部使用了随机数生成器，根据传入的随机种子初始化该生成器
    # 参数1、withReplacement参数表示是否放回抽样，False表示不放回抽样
        # False最多只能抽取不超过RDD中元素个数的样本数量
        # True表示放回抽样，可以抽取任意数量的样本
    # 参数2、num参数表示抽取的样本数量
    # 参数3、seed参数表示随机种子，不传入则使用系统时间作为随机种子，每次运行结果可能不同
    print(rdd.takeSample(withReplacement=False, num=5, seed=1))

    sc.stop()

# 运行结果如下：
# [2, 7, 6, 6, 3]