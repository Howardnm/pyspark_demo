# coding:utf8
import time

from pyspark.sql import SparkSession


if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions", 2).\
        getOrCreate()
    sc = spark.sparkContext

    """读取数据"""
    df = spark.read.format("csv").\
        option("sep", ";").\
        option("header", True).\
        load("../data/input/sql/people.csv")

    df.cache()
    df.show()
    # +-----+----+---------+
    # | name| age|      job|
    # +-----+----+---------+
    # |Jorge|  30|Developer|
    # |  Bob|  32|Developer|
    # |  Ani|  11|Developer|
    # | Lily|  11|  Manager|
    # |  Put|  11|Developer|
    # |Alice|   9|  Manager|
    # |Alice|   9|  Manager|
    # |Alice|   9|  Manager|
    # |Alice|   9|  Manager|
    # |Alice|NULL|  Manager|
    # |Alice|   9|     NULL|
    # +-----+----+---------+

    # TODO 1: 数据清洗: 数据去重
    """ dropDuplicates 是DataFrame的API, 可以完成数据去重 
        1、无参数使用, 对全部的列 联合起来进行比较, 去除重复值, 只保留一条
    """
    df.dropDuplicates().show()
    # +-----+----+---------+
    # | name| age|      job|
    # +-----+----+---------+
    # |  Bob|  32|Developer|
    # | Lily|  11|  Manager|
    # |Jorge|  30|Developer|
    # |  Ani|  11|Developer|
    # |  Put|  11|Developer|
    # |Alice|   9|  Manager| ---> 重复值只保留一条
    # |Alice|   9|     NULL|
    # |Alice|NULL|  Manager|
    # +-----+----+---------+
    df.dropDuplicates(['age', 'job']).show()
    # +-----+----+---------+
    # | name| age|      job|
    # +-----+----+---------+
    # |Alice|NULL|  Manager|
    # |  Ani|  11|Developer| ---> 根据age, job 两列进行去重
    # | Lily|  11|  Manager|
    # |Jorge|  30|Developer|
    # |  Bob|  32|Developer|
    # |Alice|   9|     NULL|
    # |Alice|   9|  Manager|
    # +-----+----+---------+

    # TODO 2: 数据清洗: 缺失值处理
    # TODO 2.1: dropna API 删除缺失值
    """ dropna 是DataFrame的API, 可以对缺失值的数据进行删除 
        1、无参数使用, 只要列中有null 就删除这一行数据
        2、thresh参数可以设置最少满足有效列的数量
    """
    df.dropna().show()
    # +-----+---+---------+
    # | name|age|      job|
    # +-----+---+---------+
    # |Jorge| 30|Developer|
    # |  Bob| 32|Developer|
    # |  Ani| 11|Developer|
    # | Lily| 11|  Manager|
    # |  Put| 11|Developer|
    # |Alice|  9|  Manager|
    # |Alice|  9|  Manager|
    # |Alice|  9|  Manager|
    # |Alice|  9|  Manager|
    # +-----+---+---------+
    df.dropna(thresh=3).show()
    # thresh = 3表示, 最少满足3个有效列,  不满足 就删除当前行数据
    # +-----+---+---------+
    # | name|age|      job|
    # +-----+---+---------+
    # |Jorge| 30|Developer|
    # |  Bob| 32|Developer|
    # |  Ani| 11|Developer|
    # | Lily| 11|  Manager|
    # |  Put| 11|Developer|
    # |Alice|  9|  Manager|
    # |Alice|  9|  Manager|
    # |Alice|  9|  Manager|
    # |Alice|  9|  Manager|
    # +-----+---+---------+
    df.dropna(thresh=2, subset=['name', 'age']).show()
    # +-----+---+---------+
    # | name|age|      job|
    # +-----+---+---------+
    # |Jorge| 30|Developer|
    # |  Bob| 32|Developer|
    # |  Ani| 11|Developer|
    # | Lily| 11|  Manager|
    # |  Put| 11|Developer|
    # |Alice|  9|  Manager|
    # |Alice|  9|  Manager|
    # |Alice|  9|  Manager|
    # |Alice|  9|  Manager|
    # |Alice|  9|     NULL|
    # +-----+---+---------+

    # TODO 2.2: fillna API 填充缺失值
    """ fillna 是DataFrame的API, 可以对缺失值的数据进行填充
        1、无参数使用, 对所有的列 进行统一的填充值 填充
    """
    df.fillna("loss").show()
    # +-----+----+---------+
    # | name| age|      job|
    # +-----+----+---------+
    # |Jorge|  30|Developer|
    # |  Bob|  32|Developer|
    # |  Ani|  11|Developer|
    # | Lily|  11|  Manager|
    # |  Put|  11|Developer|
    # |Alice|   9|  Manager|
    # |Alice|   9|  Manager|
    # |Alice|   9|  Manager|
    # |Alice|   9|  Manager|
    # |Alice|loss|  Manager|
    # |Alice|   9|     loss|
    # +-----+----+---------+
    df.fillna("N/A", subset=['job']).show()
    # 只填充特定列的缺失值
    # +-----+----+---------+
    # | name| age|      job|
    # +-----+----+---------+
    # |Jorge|  30|Developer|
    # |  Bob|  32|Developer|
    # |  Ani|  11|Developer|
    # | Lily|  11|  Manager|
    # |  Put|  11|Developer|
    # |Alice|   9|  Manager|
    # |Alice|   9|  Manager|
    # |Alice|   9|  Manager|
    # |Alice|   9|  Manager|
    # |Alice|NULL|  Manager|
    # |Alice|   9|      N/A|
    # +-----+----+---------+
    df.fillna({"name": "未知姓名", "age": 9999, "job": "未知工作"}).show()
    # 可以针对不同的列 设置不同的填充值
    # +-----+----+---------+
    # | name| age|      job|
    # +-----+----+---------+
    # |Jorge|  30|Developer|
    # |  Bob|  32|Developer|
    # |  Ani|  11|Developer|
    # | Lily|  11|  Manager|
    # |  Put|  11|Developer|
    # |Alice|   9|  Manager|
    # |Alice|   9|  Manager|
    # |Alice|   9|  Manager|
    # |Alice|   9|  Manager|
    # |Alice|9999|  Manager|
    # |Alice|   9| 未知工作|
    # +-----+----+---------+

    """ 在这里我们将处理特定值为缺失值的情况
        1、如果特定值出现, 将其替换为null
    """
    df.replace("Alice", None).show()
    # +-----+----+---------+
    # | name| age|      job|
    # +-----+----+---------+
    # |Jorge|  30|Developer|
    # |  Bob|  32|Developer|
    # |  Ani|  11|Developer|
    # | Lily|  11|  Manager|
    # |  Put|  11|Developer|
    # | NULL|   9|  Manager|
    # | NULL|   9|  Manager|
    # | NULL|   9|  Manager|
    # | NULL|   9|  Manager|
    # | NULL|NULL|  Manager|
    # | NULL|   9|     NULL|
    # +-----+----+---------+
