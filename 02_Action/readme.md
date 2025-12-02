# PySpark Action 算子

本部分总结了 PySpark RDD 中常用的 Action 算子。Action 算子会触发 Spark 作业的实际计算，并返回一个结果给 Driver 程序或将数据写入外部存储。

## 1. 计数类算子

### 1.1 `count()`
返回 RDD 中元素的总数。

- **示例**: `20_Action_operators_count.py`
- **关键代码**: `rdd.count()`
- **返回值**: 一个数字 (Long)

### 1.2 `countByKey()`
对于 (K, V) 格式的 RDD，统计每个 key 出现的次数。

- **示例**: `20_Action_operators_countByKey.py`
- **关键代码**: `rdd.countByKey()`
- **返回值**: 一个 `collections.defaultdict` 对象，如 `{'key1': 10, 'key2': 3}`。

### 1.3 `countByValue()`
统计 RDD 中每个唯一的元素出现的次数。

- **示例**: `20_Action_operators_countByValue.py`
- **关键代码**: `rdd.countByValue()`
- **返回值**: 一个 `collections.defaultdict` 对象，如 `{'value1': 5, 'value2': 8}`。

## 2. 聚合类算子

### 2.1 `reduce()`
通过一个指定的函数，对 RDD 中的所有元素进行两两聚合，最终返回一个单一的值。

- **示例**: `21_Action_operators_reduce.py`
- **关键代码**: `rdd.reduce(lambda a, b: a + b)`
- **返回值**: 与 RDD 元素类型相同的一个值。

### 2.2 `fold()`
与 `reduce()` 类似，但需要提供一个“零值”（初始值）。这个初始值会参与每个分区的聚合以及分区间结果的聚合。

- **示例**: `22_Action_operators_fold.py`
- **关键代码**: `rdd.fold(10, lambda a, b: a + b)`
- **返回值**: 与 RDD 元素类型相同的一个值。

## 3. 提取数据类算子

### 3.1 `first()`
返回 RDD 中的第一个元素。

- **示例**: `22_Action_operators_first.py`
- **关键代码**: `rdd.first()`
- **返回值**: RDD 的第一个元素。

### 3.2 `take(n)`
返回 RDD 中的前 `n` 个元素。

- **示例**: `23_Action_operators_take.py`
- **关键代码**: `rdd.take(5)`
- **返回值**: 一个包含 `n` 个元素的列表。

### 3.3 `top(n)`
返回 RDD 中按**降序**排列的前 `n` 个元素（即最大的 `n` 个元素）。

- **示例**: `23_Action_operators_top.py`
- **关键代码**: `rdd.top(5)`
- **返回值**: 一个包含 `n` 个元素的列表。

### 3.4 `takeOrdered(n, key=...)`
返回 RDD 中按**升序**排列的前 `n` 个元素。可以提供一个可选的 `key` 函数来改变排序规则。

- **示例**: `24_Action_operators_takeOrdered.py`
- **关键代码**:
  ```python
  # 最小的3个元素
  rdd.takeOrdered(3)
  # 最大的3个元素
  rdd.takeOrdered(3, lambda x: -x)
  ```
- **返回值**: 一个包含 `n` 个元素的列表。

### 3.5 `takeSample(withReplacement, num, seed=...)`
从 RDD 中随机抽取 `num` 个元素。

- **示例**: `23_Action_operators_takeSample.py`
- **关键代码**: `rdd.takeSample(withReplacement=False, num=5, seed=1)`
- **参数**:
    - `withReplacement`: 是否放回抽样 (`True` / `False`)。
    - `num`: 抽样数量。
    - `seed`: 随机数种子，用于复现结果。
- **返回值**: 一个包含 `num` 个元素的列表。

## 4. 迭代与输出类算子

### 4.1 `foreach()`
对 RDD 中的每个元素执行指定的函数。这是一个没有返回值的 Action，通常用于执行具有副作用的操作（如写入数据库）。函数在 Executor 端执行。

- **示例**: `25_Action_operators_foreach.py`
- **关键代码**: `rdd.foreach(lambda x: print(x * 10))`
- **返回值**: `None`

### 4.2 `saveAsTextFile()`
将 RDD 的内容保存为文本文件。每个分区会生成一个 `part-xxxxx` 文件。

- **示例**: `26_Action_operators_saveAsTextFile.py`
- **关键代码**: `rdd.saveAsTextFile("hdfs://path/to/output_dir")`
- **注意**:
    - 输出路径必须是一个不存在的目录。
    - 操作在 Executor 端分布式执行。
- **返回值**: `None`
