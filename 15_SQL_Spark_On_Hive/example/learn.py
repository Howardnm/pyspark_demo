# coding:utf8
from numpy.ma.core import count
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os ,time

os.environ['HADOOP_CONF_DIR'] = "/export/server/hadoop/etc/hadoop"
os.environ['JAVA_HOME'] = "/export/server/jdk"
os.environ['HADOOP_USER_NAME'] = 'hadoop'

spark = (SparkSession.builder
         .appName("sparkSQL Example")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", 2)
         .config("spark.sql.warehouse.dir", "hdfs://ct104:8020/user/hive/warehouse")
         .config("hive.metastore.uris", "thrift://ct104:9083")
         .enableHiveSupport()
         .getOrCreate()
         )

df = (spark.read.format("json")
      .load("hdfs://ct104:8020/mini.json")
      .dropna(thresh=1, subset=['storeProvince'])  # 省份缺失数据清洗
      .filter("storeProvince != 'null'")  # 过滤字符串为“null”的省份
      .filter("receivable < 10000")  # 过滤测试数据
      ).select("storeProvince", "storeID", "receivable", "dateTS", "payType") # 列值裁剪

df.persist(StorageLevel.MEMORY_AND_DISK)
df.show(5)
# +--------------+-------+----------+-------------+-------+
# | storeProvince|storeID|receivable|       dateTS|payType|
# +--------------+-------+----------+-------------+-------+
# |        湖南省|   4064|      22.5|1563758583000| alipay|
# |        湖南省|    718|       7.0|1546737450000| alipay|
# |        湖南省|   1786|      10.0|1546478081000|   cash|
# |        广东省|   3702|      10.5|1559133703000| wechat|
# |广西壮族自治区|   1156|      10.0|1548594458000|   cash|


# 1、各省 销售 指标 每个省份的销售额统计
# StoreProvince 省份
# receivable 销售金额
# df.createTempView("sales_tables")
# Province_sales = spark.sql("""
#           select storeProvince,
#                  round(sum(receivable), 2) as t1  -- 一定要先求和，再四舍五入，否则会有误差
#           from sales_tables
#           group by storeProvince
#           order by t1 desc
#           """)
#
# Province_sales.show(truncate=False)

Province_sales = (df.groupBy("storeProvince").sum("receivable")
                  .withColumnRenamed("sum(receivable)", "money")
                  .withColumn("money", F.round("money", 0))
                  .orderBy("money", ascending=False)
                  )

Province_sales.show(truncate=False) # truncate=False 不截断显示

# 2、TOP3 销售省份中,每个省份有多少家店铺 曾经单日销售额 1000+
# StoreProvince 省份
# storeID 店铺ID
# dateTS 订单日期
# receivable 销售金额
top3_Province = Province_sales.limit(3).select("storeProvince").withColumnRenamed("storeProvince", "top3")
top3_Province.show()

top3_Province_sales = df.join(top3_Province, on=df['storeProvince'] == top3_Province['top3'])

top3_Province_sales.printSchema()
top3_Province_sales.show()

store_sales = (top3_Province_sales
               .groupBy("storeProvince",
                        "storeID",
                        F.from_unixtime(df.dateTS.substr(0, 10),"yyyy-MM-dd").alias("day") # 精度裁剪到秒
                        )
               .sum("receivable")
               .withColumnRenamed("sum(receivable)", "money")
               .orderBy("money", ascending=False)
               .filter(F.col("money") > 1000)
               .dropDuplicates(subset=["storeID"])
               )
store_sales.show()

store_sales.groupBy("storeProvince").count().show()

# 3、TOP3 省份中 各个省份的平均单单价
# StoreProvince 省份
# orderID 订单ID
# receivable 销售金额


# 4、TOP3 省份中,各个省份的支付类型比例
# StoreProvince 省份
# orderID 订单ID
# payType 付款类型

# 5、需求结果写入到mysql


# 6、数据写入到spark on hive中