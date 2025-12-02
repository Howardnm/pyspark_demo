# PySpark RDD 分区操作

本部分总结了 PySpark 中与 RDD 分区 (Partition) 相关的操作。分区是 Spark 并行计算的核心，理解和使用好分区操作对于性能优化至关重要。

## 1. 以分区为单位的处理

### 1.1 `mapPartitions` (Transformation)
`mapPartitions` 是一个转换算子，它以分区为单位对 RDD 进行处理。传递给它的函数会接收到一个包含分区内所有元素的迭代器，并需要返回一个包含新元素的迭代器。

- **示例**: `27_RDD_operators_mapPartitions.py`
- **与 `map` 的区别**:
    - `map`: 对每个元素调用一次函数。
    - `mapPartitions`: 对每个分区调用一次函数。
- **优势**: 当需要在处理数据前进行一些昂贵的初始化操作时（如建立数据库连接），`mapPartitions` 可以显著提高性能，因为它可以在每个分区中只初始化一次资源。
- **关键代码**:
  ```python
  def process_partition(iterator):
      # 在这里可以进行一次性的初始化操作
      # ...
      for element in iterator:
          yield element * 10

  rdd.mapPartitions(process_partition)
  ```

### 1.2 `foreachPartition` (Action)
`foreachPartition` 是一个 Action 算子，它以分区为单位执行“副作用”操作，没有返回值。

- **示例**: `28_Action_operators_foreachPartition.py`
- **与 `foreach` 的区别**:
    - `foreach`: 对每个元素执行一次操作。
    - `foreachPartition`: 对每个分区执行一次操作，函数接收一个迭代器。
- **优势**: 与 `mapPartitions` 类似，它非常适合需要将整个分区的数据高效写入外部系统（如数据库）的场景，可以显著减少连接创建和销毁的开销。
- **关键代码**:
  ```python
  def save_partition_to_db(iterator):
      # 建立一个数据库连接
      # ...
      for record in iterator:
          # 将 record 写入数据库
          pass
      # 关闭连接
      # ...

  rdd.foreachPartition(save_partition_to_db)
  ```

## 2. RDD 重分区

### 2.1 `partitionBy` (Transformation)
`partitionBy` 允许根据自定义规则对一个**键值对 (K,V) RDD** 进行重新分区。

- **示例**: `29_RDD_operators_partitionBy.py`
- **工作原理**: 它接收一个分区数和一个分区函数。分区函数根据 `key` 返回一个分区 ID（一个整数），Spark 据此将具有相同特征的 key 发送到同一个分区。
- **应用场景**: 在执行 `reduceByKey`, `groupByKey` 等聚合操作前，使用 `partitionBy` 可以将相同的 key 预先分配到同一个分区，从而避免后续的 shuffle，提升性能。
- **关键代码**:
  ```python
  def custom_partitioner(key):
      if key in ['a', 'b']:
          return 0
      else:
          return 1

  rdd.partitionBy(numPartitions=2, partitionFunc=custom_partitioner)
  ```

### 2.2 `repartition` 和 `coalesce` (Transformation)
这两个算子都用于改变 RDD 的分区数量。

- **示例**: `30_RDD_operators_repartition_and_coalesce.py`

#### `coalesce(numPartitions, shuffle=False)`
- **默认行为**: 通过合并现有分区来**减少**分区数，**不进行 shuffle**，因此效率更高。在默认模式下不能增加分区。
- **使用场景**: 当你只需要减少分区数且不需要数据重分布时，`coalesce` 是首选。

#### `repartition(numPartitions)`
- **行为**: 总是执行一次**全量 shuffle**，将数据重新打乱并均匀地分配到新的分区中。
- **使用场景**:
    1.  需要**增加**分区数。
    2.  需要**减少**分区数，并希望数据在剩余分区中**均匀分布**。
- **注意**: `repartition(n)` 实际上是 `coalesce(n, shuffle=True)` 的简写。

- **关键代码**:
  ```python
  # 减少分区数，无 shuffle (高效)
  rdd.coalesce(2)

  # 增加分区数，有 shuffle
  rdd.repartition(10)

  # 减少分区数并均匀分布，有 shuffle
  rdd.repartition(2)
  ```
