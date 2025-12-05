from pyspark.sql import SparkSession
from pyspark.sql import functions as F


if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = (SparkSession.builder
             .appName("test")
             .master("local[*]")
             .getOrCreate()
             )

    # TODO 0: DSL 风格处理
    # 读取CSV文件
    df = (spark.read.format("csv")
          .option("sep", ";")
          .option("header", True)
          .option("encoding", "utf-8")
          .schema("name STRING, age INT, job STRING")
          .load("../data/input/sql/people.csv")
          )

    df.cache()
    # df.show(5)
    # +-----+---+---------+
    # | name|age|      job|
    # +-----+---+---------+
    # |Jorge| 30|Developer|
    # |  Bob| 32|Developer|
    # |  Ani| 11|Developer|
    # | Lily| 11|  Manager|
    # |  Put| 11|Developer|
    # +-----+---+---------+
    # TODO 1:  DataFrame 常用转换方法 (Verbs)
    # TODO 1.1: select API 选择列
    # df.select(["name", "age", "job"]).show(3)
    # df.select("name", "age", "job").show(3)
    # df.select(df["name"], df["age"], df["job"]).show(3)
    # df.select(df.name, df.age, df.job).show(3)
    df.select("*").show(3)
    # +-----+---+---------+
    # | name|age|      job|
    # +-----+---+---------+
    # |Jorge| 30|Developer|
    # |  Bob| 32|Developer|
    # |  Ani| 11|Developer|
    # +-----+---+---------+
    # TODO 1.2: filter / where API 过滤行
    # df.where(df["age"] >= 30).show()
    df.filter(df["age"] >= 30).show()
    # +-----+---+---------+
    # | name|age|      job|
    # +-----+---+---------+
    # |Jorge| 30|Developer|
    # |  Bob| 32|Developer|
    # +-----+---+---------+
    # TODO 1.3: withColumn API 添加/修改列
    df.withColumn("age1", df["age"] * 10).show(3)
    # +-----+---+---------+----+
    # | name|age|      job|age1|
    # +-----+---+---------+----+
    # |Jorge| 30|Developer| 300|
    # |  Bob| 32|Developer| 320|
    # |  Ani| 11|Developer| 110|
    # +-----+---+---------+----+
    # TODO 1.4: withColumnRenamed API 重命名列
    df.withColumnRenamed("job", "works").show(3)
    # +-----+---+---------+
    # | name|age|    works|
    # +-----+---+---------+
    # |Jorge| 30|Developer|
    # |  Bob| 32|Developer|
    # |  Ani| 11|Developer|
    # +-----+---+---------+
    # TODO 1.5: drop API 删除列
    # df.drop("job").show(3)
    df.drop(df["job"]).show(3)
    # +-----+---+
    # | name|age|
    # +-----+---+
    # |Jorge| 30|
    # |  Bob| 32|
    # |  Ani| 11|
    # +-----+---+
    # TODO 1.6: orderBy / sort API 排序
    # orderBy = sort
    # df.sort("age").show()
    # df.orderBy("age").show()
    # df.orderBy(df["age"]).show(3)
    df.orderBy(df.age).show(3)
    # +-----+----+-------+
    # | name| age|    job|
    # +-----+----+-------+
    # |Alice|NULL|Manager|
    # |Alice|   9|Manager|
    # |Alice|   9|Manager|
    # +-----+----+-------+
    # df.orderBy(df['age'].desc()).show(3)
    # df.orderBy(F.desc("age")).show(3)
    # df.orderBy(df.age.desc()).show(3)
    # df.orderBy("age", ascending=False).show(3)
    df.orderBy(df['age'], ascending=False).show(3)
    # +-----+---+---------+
    # | name|age|      job|
    # +-----+---+---------+
    # |  Bob| 32|Developer|
    # |Jorge| 30|Developer|
    # |  Ani| 11|Developer|
    # +-----+---+---------+

    # TODO 2:  常用函数 Functions (Actions)
    # 使用 functions 模块(F) 中的函数
    # TODO 2.1: 常用的列函数
    df.select(
        F.col("name"),                                                       # 等同于 df["name"]
        F.col("age"),
        (F.col("age") + F.lit(10)).alias("lit_hello"),                       # lit 字面值函数, 加法运算
        F.upper(df["job"]).alias("job_upper"),                               # 转大写
        F.concat(df["name"], F.lit("_"), df["job"]).alias("name_job"), # 字符串拼接
        F.round(F.lit(3.14159), 2).alias("pi_2")                       # 四舍五入保留2位小数
    ).show(3)
    # +-----+---+---------+---------+---------------+----+
    # | name|age|lit_hello|job_upper|       name_job|pi_2|
    # +-----+---+---------+---------+---------------+----+
    # |Jorge| 30|       40|DEVELOPER|Jorge_Developer|3.14|
    # |  Bob| 32|       42|DEVELOPER|  Bob_Developer|3.14|
    # |  Ani| 11|       21|DEVELOPER|  Ani_Developer|3.14|
    # +-----+---+---------+---------+---------------+----+
    # TODO 2.2: 条件函数 when otherwise
    df.withColumn("age1",
        F.when(df["age"] >= 30, "老年人")
        .when(df["age"] >= 10, "年轻人")
        .otherwise("小朋友")
    ).show()
    # +-----+----+---------+------+
    # | name| age|      job|  age1|
    # +-----+----+---------+------+
    # |Jorge|  30|Developer|老年人|
    # |  Bob|  32|Developer|老年人|
    # |  Ani|  11|Developer|年轻人|
    # | Lily|  11|  Manager|年轻人|
    # |  Put|  11|Developer|年轻人|
    # |Alice|   9|  Manager|小朋友|
    # |Alice|   9|  Manager|小朋友|

    # TODO 2.3: 聚合函数 aggregation functions
    # 通常配合 groupBy().agg() 使用。
    df.groupBy("age").count().show()
    # +----+-----+
    # | age|count|
    # +----+-----+
    # |NULL|    1|
    # |   9|    5|
    # |  32|    1|
    # |  11|    3|
    # |  30|    1|
    # +----+-----+
    df.groupBy("age").agg(
        F.count("*").alias("count"),
        F.min("age").alias("min_age"),
        F.max("age").alias("max_age"),
        F.avg("age").alias("avg_age"),
        F.sum("age").alias("sum_age")
    ).show()
    # +----+-----+-------+-------+-------+-------+
    # | age|count|min_age|max_age|avg_age|sum_age|
    # +----+-----+-------+-------+-------+-------+
    # |NULL|    1|   NULL|   NULL|   NULL|   NULL|
    # |   9|    5|      9|      9|    9.0|     45|
    # |  32|    1|     32|     32|   32.0|     32|
    # |  11|    3|     11|     11|   11.0|     33|
    # |  30|    1|     30|     30|   30.0|     30|
    # +----+-----+-------+-------+-------+-------+
    # TODO 2.4: 窗口函数 window functions
    from pyspark.sql.window import Window
    df.select(
        "name",
        "age",
        F.row_number().over(Window.orderBy("age")).alias("row_num"),
        F.rank().over(Window.orderBy("age")).alias("rank"),
        F.dense_rank().over(Window.orderBy("age")).alias("dense_rank"),
        F.sum("age").over(Window.orderBy("age")).alias("sum_age"),
        F.avg("age").over(Window.orderBy("age")).alias("avg_age")
    ).show()
    # +-----+----+-------+----+----------+-------+-------+
    # | name| age|row_num|rank|dense_rank|sum_age|avg_age|
    # +-----+----+-------+----+----------+-------+-------+
    # |Alice|NULL|      1|   1|         1|   NULL|   NULL|
    # |Alice|   9|      2|   2|         2|     45|    9.0|
    # |Alice|   9|      3|   2|         2|     45|    9.0|
    # |Alice|   9|      4|   2|         2|     45|    9.0|
    # |Alice|   9|      5|   2|         2|     45|    9.0|
    # |Alice|   9|      6|   2|         2|     45|    9.0|
    # |  Ani|  11|      7|   7|         3|     78|   9.75|
    # | Lily|  11|      8|   7|         3|     78|   9.75|
    # |  Put|  11|      9|   7|         3|     78|   9.75|
    # |Jorge|  30|     10|  10|         4|    108|   12.0|
    # |  Bob|  32|     11|  11|         5|    140|   14.0|
    # +-----+----+-------+----+----------+-------+-------+
    # TODO 2.5: 日期函数 current_date, current_timestamp
    df.select(
        F.current_date().alias("current_date"),
        F.current_timestamp().alias("current_timestamp")
    ).show(3)
    # +------------+--------------------+
    # |current_date|   current_timestamp|
    # +------------+--------------------+
    # |  2025-12-04|2025-12-04 15:56:...|
    # |  2025-12-04|2025-12-04 15:56:...|
    # |  2025-12-04|2025-12-04 15:56:...|
    # +------------+--------------------+
    # TODO 2.6: 空值处理函数 coalesce, when otherwise
    df.select(
        F.coalesce(df["age"], F.lit(0)).alias("age_coalesce"),
        F.when(df["age"].isNull(), F.lit(0)).otherwise(df["age"]).alias("age_when")
    ).show(3)
    # +------------+--------+
    # |age_coalesce|age_when|
    # +------------+--------+
    # |          30|      30|
    # |          32|      32|
    # |          11|      11|
    # +------------+--------+
