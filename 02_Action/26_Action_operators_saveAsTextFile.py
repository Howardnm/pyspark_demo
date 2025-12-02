# coding:utf8

from pyspark import SparkConf, SparkContext
import os

# 设置HADOOP_USER_NAME环境变量，指定HDFS操作用户，否则可能会出现权限问题
os.environ['HADOOP_USER_NAME'] = 'hadoop'

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    # 创建一个包含7个元素的RDD，分为3个分区,并将其保存到HDFS文件系统中
    # 将数据拆分3个分区，每个分区保存到一个文件中，生成多个part-0000x文件
    # 注意：saveAsTextFile算子是在集群的每个Executor节点上执行的，不需要收集到Driver端
    # 场景：通常用于将处理后的数据保存到分布式文件系统，如HDFS、S3等
    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], numSlices=3)

    rdd.saveAsTextFile("hdfs://ct104:8020/output/out1")
    sc.stop()

# 保存结果到HDFS文件系统中后，可以通过HDFS命令查看生成的文件
# hdfs dfs -ls /output/out1
# hdfs dfs -cat /output/out1/part-00000
# hdfs dfs -cat /output/out1/part-00001
# hdfs dfs -cat /output/out1/part-00002
# hdfs dfs -rm -r /output/out1
# 注意：每次运行saveAsTextFile都会生成一个新的目录，不能覆盖已存在的目录

# 输出结果:
# [hadoop@CT104 input]$ hadoop fs -ls /output/out1
# Found 4 items
# -rw-r--r--   3 hadoop supergroup          0 2025-12-02 00:13 /output/out1/_SUCCESS
# -rw-r--r--   3 hadoop supergroup          4 2025-12-02 00:13 /output/out1/part-00000
# -rw-r--r--   3 hadoop supergroup          4 2025-12-02 00:13 /output/out1/part-00001
# -rw-r--r--   3 hadoop supergroup          6 2025-12-02 00:13 /output/out1/part-00002
