# PySpark 共享变量：广播变量与累加器

在 Spark 的分布式计算模型中，任务（Task）在不同的 Executor 上并行执行。默认情况下，任务之间是完全隔离的，并且无法直接与 Driver 端的变量进行通信。为了支持一些常见的应用场景，Spark 提供了两种特殊的共享变量：广播变量 (Broadcast Variable) 和累加器 (Accumulator)。

## 1. 广播变量 (Broadcast Variable)

广播变量用于将一个**只读**的变量高效地分发给集群中的所有任务。

- **示例**: `33_RDD_broadcast.py`
- **综合案例**: `35_RDD_broadcast_and_accumulator_demo.py`

### 使用场景
当 RDD 的转换操作（如 `map`, `filter`）需要访问一个共同的、只读的、且数据量不大的本地集合（如 list, dict）时，应使用广播变量。

### 工作原理
- **不使用广播变量**: Spark 会将这个本地集合的副本随每个**任务**一起发送到 Executor。这会导致大量的网络 I/O 和 Executor 内存冗余。
- **使用广播变量**: 变量被高效地分发到每个 **Executor** 一次。该 Executor 上的所有任务共享这个变量的唯一副本。

### 如何使用
1.  **创建**: 在 Driver 端使用 `sc.broadcast(local_variable)` 创建广播变量。
2.  **访问**: 在 Executor 的任务中，通过广播变量的 `.value` 属性来获取其值。

```python
# Driver 端
local_data = {"a": 1, "b": 2}
broadcast_var = sc.broadcast(local_data)

# Executor 端 (在 map, filter 等算子内部)
def process_func(element):
    # 通过 .value 访问广播的数据
    value = broadcast_var.value.get(element, 0)
    return (element, value)

rdd.map(process_func)
```

### 优势
- **减少网络 I/O**: 变量只被发送到每个 Executor 一次。
- **降低 Executor 内存占用**: 同一个 Executor 上的所有任务共享一个副本。

## 2. 累加器 (Accumulator)

累加器提供了一种可靠的方式，让分布在不同节点上的任务能够对一个共享变量执行**聚合操作**（如计数或求和）。

- **示例**: `34_RDD_accumulator.py`
- **综合案例**: `35_RDD_broadcast_and_accumulator_demo.py`

### 使用场景
当你需要统计 RDD 中满足特定条件的元素数量，或对某些值进行全局求和时，应使用累加器。

### 工作原理
累加器遵循“只写”原则：Executor 上的任务只能向累加器“增加”值，但不能读取它的当前值。只有 Driver 程序可以在 Action 操作执行完毕后，读取累加器的最终结果。

### 如何使用
1.  **创建**: 在 Driver 端使用 `sc.accumulator(initial_value)` 初始化一个累加器。
2.  **更新**: 在 Executor 的任务中，使用 `+=` 操作符更新累加器的值。
3.  **读取**: 在 Driver 端，当一个 Action 执行完毕后，通过累加器的 `.value` 属性来获取最终结果。

```python
# Driver 端
counter = sc.accumulator(0)

# Executor 端
def process_func(element):
    global counter
    if element % 2 != 0:
        counter += 1 # 只能执行增加操作

rdd.foreach(process_func) # foreach 是一个 Action

# Driver 端
print("奇数的数量:", counter.value)
```

### 重要注意事项
- **Action 可靠性**: Spark 保证在 **Action** 操作中，每个任务对累加器的更新**只会执行一次**。
- **Transformation 风险**: 如果在**转换操作**（如 `map`）中更新累加器，而该 RDD 后续被多个 Action 使用，累加器可能会被**多次更新**，导致结果不准确。如果必须在转换操作中更新累加器，请考虑在该 RDD 上使用 `.cache()` 来避免重复计算。
