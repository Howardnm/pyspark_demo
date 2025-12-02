# PySpark RDD 操作大全

本部分包含了 PySpark RDD 的常用操作，涵盖了 RDD 的创建、转换和行动等。

## 1. RDD 创建

### 1.1 从集合创建 (parallelize)
通过 `sc.parallelize()` 可以将 Python 中的 list 等集合转换成 RDD。

- **示例**: `01_RDD_create_parallelize.py`
- **关键代码**:
  ```python
  data = [1, 2, 3, 4, 5]
  rdd = sc.parallelize(data, numSlices=3) # numSlices 指定分区数
  ```

### 1.2 从外部存储创建 (textFile)
通过 `sc.textFile()` 可以从本地文件系统、HDFS 等创建 RDD，RDD 中每个元素是文件中的一行。

- **示例**: `02_RDD_create_textFile.py`
- **关键代码**:
  ```python
  rdd = sc.textFile("../data/input/words.txt", minPartitions=3) # minPartitions 指定最小分区数
  ```

### 1.3 从文件目录创建 (wholeTextFiles)
通过 `sc.wholeTextFiles()` 可以读取一个目录下的所有小文件，生成一个 (文件名, 文件内容) 的键值对 RDD。

- **示例**: `03_RDD_create_wholeTextFile.py`
- **关键代码**:
  ```python
  rdd = sc.wholeTextFiles("../data/input/tiny_files")
  ```

## 2. RDD 转换算子 (Transformation)

### 2.1 `map`
将 RDD 中的每个元素通过一个指定的函数进行转换。

- **示例**: `04_RDD_operators_map.py`
- **关键代码**: `rdd.map(lambda data: data * 10)`

### 2.2 `flatMap`
与 `map` 类似，但每个输入元素可以被映射为 0 或多个输出元素（所以函数应返回一个序列）。常用于“扁平化”操作，如切分单词。

- **示例**: `05_RDD_operators_flatMap.py`
- **关键代码**: `rdd.flatMap(lambda line: line.split(" "))`

### 2.3 `reduceByKey`
在一个 (K, V) 对的 RDD 上，将所有具有相同 key 的 value 进行聚合。

- **示例**: `06_RDD_operators_reduceByKey.py`
- **关键代码**: `rdd.reduceByKey(lambda a, b: a + b)`

### 2.4 `groupBy`
根据指定的函数对 RDD 中的元素进行分组。

- **示例**: `08_RDD_operators_groupBy.py`
- **关键代码**: `rdd.groupBy(lambda t: t[0])`

### 2.5 `filter`
过滤 RDD 中的元素，只保留函数返回值为 `True` 的元素。

- **示例**: `09_RDD_operators_filter.py`
- **关键代码**: `rdd.filter(lambda x: x % 2 == 1)`

### 2.6 `distinct`
对 RDD 中的元素进行去重。

- **示例**: `10_RDD_operators_distinct.py`
- **关键代码**: `rdd.distinct()`

### 2.7 `union`
合并两个 RDD，返回一个新的 RDD，不去重。

- **示例**: `11_RDD_operators_union.py`
- **关键代码**: `rdd1.union(rdd2)`

### 2.8 `join`
对两个 (K, V) 格式的 RDD 进行内连接操作，返回 (K, (V1, V2)) 格式的 RDD。

- **示例**: `12_RDD_operators_join.py`
- **关键代码**: `rdd1.join(rdd2)`

### 2.9 `leftOuterJoin` 和 `rightOuterJoin`
左外连接和右外连接。

- **示例**: `12_RDD_operators_join.py`
- **关键代码**:
  ```python
  rdd1.leftOuterJoin(rdd2)
  rdd1.rightOuterJoin(rdd2)
  ```

### 2.10 `intersection`
返回两个 RDD 的交集。

- **示例**: `13_RDD_operators_intersection.py`
- **关键代码**: `rdd1.intersection(rdd2)`

### 2.11 `glom`
将 RDD 每个分区中的所有元素合并成一个列表。

- **示例**: `14_RDD_operators_glom.py`
- **关键代码**: `rdd.glom()`

### 2.12 `groupByKey`
对 (K, V) 格式的 RDD，按 key 进行分组。

- **示例**: `15_RDD_operators_gorupByKey.py`
- **关键代码**: `rdd.groupByKey()`

### 2.13 `sortBy`
根据指定的函数对 RDD 进行排序。

- **示例**: `16_RDD_operators_sortBy.py`
- **关键代码**: `rdd.sortBy(lambda x: x[1], ascending=True, numPartitions=1)`

### 2.14 `sortByKey`
对 (K, V) 格式的 RDD，按 key 进行排序。

- **示例**: `17_RDD_operators_sortByKey.py`
- **关键代码**: `rdd.sortByKey(ascending=True, numPartitions=1, keyfunc=lambda key: str(key).lower())`

## 3. 综合案例

### 3.1 词频统计
一个经典的 WordCount 示例，综合运用了 `flatMap`, `map`, `reduceByKey`。

- **示例**: `07_RDD_wordcount_example.py`

### 3.2 JSON 数据处理
从文件中读取 JSON 数据，进行过滤和转换。

- **示例**: `18_RDD_operators_demo.py`

## 4. 集群部署

### 4.1 提交到 YARN
将 PySpark 作业提交到 YARN 集群运行。

- **示例**: `19_RDD_operators_demo_run_yarn.py`
- **关键配置**:
  ```python
  conf = SparkConf().setAppName("test-yarn-1").setMaster("yarn")
  conf.set("spark.submit.pyFiles", "defs_19.py") # 分发依赖文件
  sc.textFile("hdfs://...") # 使用 HDFS 路径
  ```
