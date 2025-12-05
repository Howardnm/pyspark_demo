# coding:utf8
import string

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType

if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        config("spark.sql.shuffle.partitions", 2). \
        getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize([
        ("张三", "class1", 99),
        ("李四", "class2", 97),
        ("王五", "class3", 95),
        ("赵六", "class4", 95),
        ("田七", "class5", 95),
        ("周八", "class1", 55),
        ("吴九", "class2", 66),
        ("郑十", "class3", 11),
        ("钱十一", "class4", 97),
        ("孙十二", "class5", 87),
        ("李十三", "class1", 76),
        ("周十四", "class2", 66),
        ("吴十五", "class3", 3),
        ("郑十六", "class4", 11),
        ("钱十七", "class5", 32)
    ])

    schema = StructType() \
        .add("name", StringType(), True) \
        .add("class", StringType(), True) \
        .add("score", IntegerType(), True)

    df = rdd.toDF(schema)
    df.cache()

    # TODO 0: 注册临时视图
    df.createTempView("stu")

    # TODO 1: 使用窗口函数, 聚合窗口，计算学生成绩平均分
    # 聚合函数： avg()、sum()、max()、min()、count()
    spark.sql("""
        select
            *,
            avg(score) over() as avg_score
        from
            stu
    """).show(truncate=False)
    # +------+------+-----+-----------------+
    # |name  |class |score|avg_score        |
    # +------+------+-----+-----------------+
    # |张三  |class1|99   |65.66666666666667|
    # |李四  |class2|97   |65.66666666666667|
    # |王五  |class3|95   |65.66666666666667|


    # TODO 2: 使用窗口函数，排序窗口，计算学生成绩排名
    # 排序函数： rank()、dense_rank()、row_number()
        # rank(): 相同分数并列排名，后续排名会跳过
        # dense_rank(): 相同分数并列排名，后续排名不跳过
        # row_number(): 不考虑分数相同与否，直接排名
    spark.sql("""
        select
            *,
            row_number() over(order by score desc) as row_number_no, -- 全局排名
            dense_rank() over(partition by class order by score desc) as dense_rank_no, -- 按班级分区排名
            rank() over(order by score desc) as rank_no -- 全局排名
        from
            stu
    """).show(truncate=False)
    # +------+------+-----+-------------+-------------+-------+
    # |name  |class |score|row_number_no|dense_rank_no|rank_no|
    # +------+------+-----+-------------+-------------+-------+
    # |张三  |class1|99   |1            |1            |1      |
    # |李十三|class1|76   |8            |2            |8      |
    # |周八  |class1|55   |11           |3            |11     |
    # |李四  |class2|97   |2            |1            |2      | ---> 按班级分区排名
    # |吴九  |class2|66   |9            |2            |9      |
    # |周十四|class2|66   |10           |2            |9      |
    # |王五  |class3|95   |4            |1            |4      | ---> 按班级分区排名
    # |郑十  |class3|11   |13           |2            |13     |
    # |吴十五|class3|3    |15           |3            |15     |
    # |钱十一|class4|97   |3            |1            |2      | ---> 按班级分区排名
    # |赵六  |class4|95   |5            |2            |4      |
    # |郑十六|class4|11   |14           |3            |13     |
    # |田七  |class5|95   |6            |1            |4      | ---> 按班级分区排名
    # |孙十二|class5|87   |7            |2            |7      |
    # |钱十七|class5|32   |12           |3            |12     |
    # +------+------+-----+-------------+-------------+-------+

    # NTILE
    # TODO 3: 使用窗口函数，分组窗口，计算学生成绩分布到5个等级
    # 分桶函数： ntile(n)
    spark.sql("""
        select
            *,
            ntile(5) over(order by score desc) as score_level
        from
            stu
    """).show(truncate=False)
    # +------+------+-----+-----------+
    # |name  |class |score|score_level|
    # +------+------+-----+-----------+
    # |张三  |class1|99   |1          | ---> 按成绩排序，分到第1个等级
    # |李四  |class2|97   |1          |
    # |钱十一|class4|97   |1          |
    # |王五  |class3|95   |2          | ---> 按成绩排序，分到第2个等级
    # |赵六  |class4|95   |2          |
    # |田七  |class5|95   |2          |
    # |孙十二|class5|87   |3          | ---> 按成绩排序，分到第3个等级
    # |李十三|class1|76   |3          |
    # |吴九  |class2|66   |3          |
    # |周十四|class2|66   |4          | ---> 按成绩排序，分到第4个等级
    # |周八  |class1|55   |4          |
    # |钱十七|class5|32   |4          |
    # |郑十  |class3|11   |5          | ---> 按成绩排序，分到第5个等级
    # |郑十六|class4|11   |5          |
    # |吴十五|class3|3    |5          |
    # +------+------+-----+-----------+