# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 2), ('b', 3)])

    # 通过groupBy对数据进行分组
    # groupBy传入的函数的 意思是: 通过这个函数, 确定按照谁来分组(返回谁即可)
    # 分组规则 和SQL是一致的, 也就是相同的在一个组(Hash分组)
    rdd = rdd.groupBy(lambda t: t[0]) # 按照每个元组的第一个元素进行分组
    print(rdd.collect())
    # 将分组后的结果进行格式的转换
    rdd = rdd.map(lambda t: (t[0], list(t[1])))
    print(rdd.collect())
    sc.stop()

# 输出结果
# [('b', < pyspark.resultiterable.ResultIterable object at 0x77fa470860a0 >), ('a', < pyspark.resultiterable.ResultIterable object at 0x77fa470861c0 >)]
# [('b', [('b', 1), ('b', 2), ('b', 3)]), ('a', [('a', 1), ('a', 1)])]