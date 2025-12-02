# coding:utf8
from pyspark import SparkConf, SparkContext # 导入Spark配置和上下文模块
'''
用于在Linux环境下运行的代码，而且是在local模式下运行
'''

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("HelloWorld") # 创建Spark配置对象
    sc = SparkContext(conf=conf)  # 创建Spark上下文对象

    # 需求：wordcount单词计数，读取HDFS上的文件，统计每个单词出现的次数
    file_rdd = sc.textFile("hdfs://ct104:8020/words.txt")  # 读取HDFS上的文本文件，创建RDD

    words_rdd = file_rdd.flatMap(lambda line: line.split(" "))  # 按空格拆分每一行，生成单词RDD
    word_pair_rdd = words_rdd.map(lambda x: (x, 1))  # 将每个单词映射为(单词, 1)的键值对RDD
    word_count_rdd = word_pair_rdd.reduceByKey(lambda a, b: a + b)  # 按单词进行聚合，计算每个单词的总次数

    print(word_count_rdd.collect()) # 收集结果并打印