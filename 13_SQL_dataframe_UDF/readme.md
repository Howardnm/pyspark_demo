# 4.1 SparkSQL 定义UDF函数

无论Hive还是SparkSQL分析处理数据时，往往需要使用函数，SparkSQL模块本身自带很多实现公共功能的函数，在`pyspark.sql.functions`中。SparkSQL与Hive一样支持定义函数：UDF和UDAF，尤其是UDF函数在实际项目中使用最为广泛。


回顾Hive中自定义函数有三种类型：
- **第一种：UDF（User-Defined-Function）函数**
  - 一对一的关系，输入一个值经过函数以后输出一个值；
  - 在Hive中继承UDF类，方法名称为evaluate，返回值不能为void，其实就是实现一个方法；
- **第二种：UDAF（User-Defined Aggregation Function）聚合函数**
  - 多对一的关系，输入多个值输出一个值，通常与groupBy联合使用；
- **第三种：UDTF（User-Defined Table-Generating Functions）函数**
  - 一对多的关系，输入一个值输出多个值（一行变为多行）；
  - 用户自定义生成函数，有点像flatMap；

### 目前Spark框架各版本及语言对自定义函数的支持

| Apache Spark Version | Spark SQL UDF（Python, Java, Scala） | Spark SQL UDAF（Java, Scala） | Spark SQL UDF（R） | Hive UDF, UDAF, UDTF |
|----------------------|--------------------------------------|-------------------------------|--------------------|----------------------|
| 1.1-1.4              | ✔️                                    |                               |                    | ✔️                    |
| 1.5                  | ✔️                                    | experimental                  |                    | ✔️                    |
| 1.6                  | ✔️                                    | ✔️                             |                    | ✔️                    |
| 2.0                  | ✔️                                    | ✔️                             | ✔️                  | ✔️                    |


> ❗在SparkSQL中，目前仅仅支持UDF函数和UDAF函数，目前Python仅支持UDF
