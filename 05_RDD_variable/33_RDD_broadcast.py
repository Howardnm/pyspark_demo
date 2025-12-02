# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 为什么要使用广播变量?
    # 场景: 本地集合对象 和 分布式集合对象(RDD) 进行关联的时候
    # 如果不使用广播变量, 每个Task都会从Driver端获取一份本地集合对象
    # 1. 网络IO次数多
    # 2. Executor内存占用高
    # 解决方案: 使用广播变量
    # 广播变量: 将本地集合对象 进行广播, 每个Executor只会获取一份本地集合对象
    # 1. 网络IO次数少
    # 2. Executor内存占用低

    # 如果本地集合对象数据过大, 那么不适合使用广播变量, 而是应该将本地集合对象转换为RDD, 然后进行join操作

    stu_info_list = [(1, '张大仙', 11),
                     (2, '王晓晓', 13),
                     (3, '张甜甜', 11),
                     (4, '王大力', 11)]

    # 1. 将本地Python List对象标记为广播变量
    broadcast = sc.broadcast(stu_info_list)

    score_info_rdd = sc.parallelize([
        (1, '语文', 99),
        (2, '数学', 99),
        (3, '英语', 99),
        (4, '编程', 99),
        (1, '语文', 99),
        (2, '编程', 99),
        (3, '语文', 99),
        (4, '英语', 99),
        (1, '语文', 99),
        (3, '英语', 99),
        (2, '编程', 99)
    ])

    def map_func(data):
        id = data[0]
        name = ""
        # 匹配本地list和分布式rdd中的学生ID  匹配成功后 即可获得当前学生的姓名
        # 2. 在使用到本地集合对象的地方, 从广播变量中取出来用即可
        for stu_info in broadcast.value:
            stu_id = stu_info[0]
            if id == stu_id:
                name = stu_info[1]

        return (name, data[1], data[2])


    print(score_info_rdd.map(map_func).collect())

"""
场景: 本地集合对象 和 分布式集合对象(RDD) 进行关联的时候
需要将本地集合对象 封装为广播变量
可以节省:
1. 网络IO的次数
2. Executor的内存占用
"""

# 输出结果
# [('张大仙', '语文', 99), ('王晓晓', '数学', 99), ('张甜甜', '英语', 99), ('王大力', '编程', 99), ('张大仙', '语文', 99), ('王晓晓', '编程', 99), ('张甜甜', '语文', 99), ('王大力', '英语', 99), ('张大仙', '语文', 99), ('张甜甜', '英语', 99), ('王晓晓', '编程', 99)]