# PySpark RDD 持久化：Cache 与 Checkpoint

在 Spark 中，当一个 RDD 被多次使用时，为了避免重复计算，可以将其持久化。Spark 提供了两种主要的持久化机制：缓存 (Cache) 和检查点 (Checkpoint)。它们的目的和行为有显著不同。

## 1. 缓存 (Cache / Persist)

缓存主要用于**提升性能**。它将 RDD 的计算结果临时存储在 Executor 的内存或磁盘上。

- **示例**: `31_RDD_cache.py`

### 关键特性
- **API**:
    - `rdd.cache()`: 这是最常用的缓存方法，等同于 `rdd.persist(StorageLevel.MEMORY_ONLY)`。
    - `rdd.persist(storageLevel)`: 提供更灵活的存储级别控制。
- **存储级别 (`StorageLevel`)**:
    - `MEMORY_ONLY`: 仅内存（默认）。
    - `MEMORY_AND_DISK`: 优先存内存，内存不足时溢写到磁盘（常用推荐）。
    - `DISK_ONLY`: 仅磁盘。
    - `_2` 后缀 (如 `MEMORY_ONLY_2`): 表示创建2个副本以提高容错性。
- **惰性执行**: `cache()` 和 `persist()` 都是惰性算子，只有当第一个 Action 触发时，RDD 才会被计算并缓存。
- **血缘关系**: 缓存**保留**了 RDD 的血缘关系（Lineage）。如果某个缓存的分区丢失，Spark 可以根据血缘关系重新计算它。
- **生命周期**: 缓存数据是**临时**的，当 Spark 应用程序结束后，缓存数据会被清除。
- **释放**: 可以使用 `rdd.unpersist()` 手动释放缓存。

## 2. 检查点 (Checkpoint)

Checkpoint 主要用于**容错**。它将 RDD 的数据持久化到一个可靠的外部存储系统（如 HDFS）中。

- **示例**: `32_RDD_checkpoint.py`

### 关键特性
- **API**:
    - `sc.setCheckpointDir("hdfs://...")`: 在使用前必须设置一个可靠的存储目录。
    - `rdd.checkpoint()`: 标记一个 RDD 进行 checkpoint。
- **惰性执行**: `checkpoint()` 也是惰性的，当一个 Action 触发时，Spark 会启动一个独立的作业来计算 RDD 并将其写入 HDFS。
- **血缘关系**: 这是与缓存最核心的区别。一旦 checkpoint 完成，它会**切断** RDD 的血缘关系。这个 RDD 将不再依赖于它的父 RDD，而是直接从 checkpoint 文件中读取数据。这对于有非常长依赖链的 RDD 来说，可以极大地提高容错恢复的效率。
- **生命周期**: Checkpoint 数据是**持久**的。即使应用程序结束，数据依然保留在 HDFS 中，需要手动清理。

## 3. Cache 与 Checkpoint 的核心区别

| 特性 | Cache / Persist | Checkpoint |
| :--- | :--- | :--- |
| **主要目的** | 性能提升 | 容错 |
| **存储位置** | Executor 的内存 / 磁盘 | HDFS 等可靠的外部存储 |
| **血缘关系** | **保留** | **切断** |
| **生命周期** | 临时的 (随应用结束而清除) | 持久的 (需手动清理) |
| **可靠性** | 较低 (Executor 故障可能导致数据丢失) | 高 (依赖于 HDFS 等) |

## 4. 最佳实践：Cache + Checkpoint

对于一个计算成本高、依赖链长且需要被多次使用的 RDD，一个常见的最佳实践是**同时使用 Cache 和 Checkpoint**。

```python
# 1. 设置 checkpoint 目录
sc.setCheckpointDir("hdfs://...")

# 2. 先标记 RDD 进行缓存
rdd.cache()

# 3. 再标记 RDD 进行 checkpoint
rdd.checkpoint()

# 4. 触发 Action
rdd.count()
```

**工作流程**:
1. 当 Action (`count()`) 第一次被触发时，`rdd` 会被计算出来。
2. 因为 `rdd` 被标记为 `cache()`，所以计算结果会存入内存。
3. 同时，因为 `rdd` 也被标记为 `checkpoint()`，Spark 会启动一个作业来执行 checkpoint。这个作业会直接从内存中读取已经缓存的数据，并将其写入 HDFS，而不是从头开始重新计算 `rdd`。
4. Checkpoint 完成后，血缘关系被切断。

这样做的好处是，既利用了 Cache 带来的快速数据访问，又通过 Checkpoint 获得了高容错性，同时还加速了 Checkpoint 过程本身。
