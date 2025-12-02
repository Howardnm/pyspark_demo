# coding:utf-8
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("CreateRDD_wholeTextFile")
    sc = SparkContext(conf=conf)

    # 使用wholeTextFiles方法创建RDD, 每个文件作为一个元素, (文件路径, 文件内容)
    rdd = sc.wholeTextFiles("../data/input/tiny_files")  # 读取本地文件系统中的文本文件，创建RDD
    # 以kv对的形式存储，key为文件路径，value为文件内容
    print(rdd.collect())  # 收集并打印RDD中的数据

    rdd = rdd.map(lambda x: (x[1]))
    rdd = rdd.flatMap(lambda x: x.split(" "))
    print(rdd.collect())
    sc.stop()  # 停止SparkContext

# Output:
# [('file:/tmp/pycharm_project_940/data/input/tiny_files/1.txt', 'hello spark\r\nhello hadoop\r\nhello flink'), ('file:/tmp/pycharm_project_940/data/input/tiny_files/3.txt', 'hello spark\r\nhello hadoop\r\nhello flink'), ('file:/tmp/pycharm_project_940/data/input/tiny_files/4.txt', 'hello spark\r\nhello hadoop\r\nhello flink'), ('file:/tmp/pycharm_project_940/data/input/tiny_files/5.txt', 'hello spark\r\nhello hadoop\r\nhello flink'), ('file:/tmp/pycharm_project_940/data/input/tiny_files/2.txt', 'hello spark\r\nhello hadoop\r\nhello flink')]
# ['hello', 'spark\r\nhello', 'hadoop\r\nhello', 'flink', 'hello', 'spark\r\nhello', 'hadoop\r\nhello', 'flink', 'hello', 'spark\r\nhello', 'hadoop\r\nhello', 'flink', 'hello', 'spark\r\nhello', 'hadoop\r\nhello', 'flink', 'hello', 'spark\r\nhello', 'hadoop\r\nhello', 'flink']
