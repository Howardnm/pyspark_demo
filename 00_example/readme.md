# PySpark "Hello World" 示例

这个包提供了一个基础的 PySpark 词频统计 (WordCount) 示例，并展示了如何在不同的操作系统和集群环境中运行它。

## 1. 核心逻辑：词频统计 (WordCount)

所有示例都实现了相同的核心逻辑：
1.  通过 `sc.textFile()` 从文件系统读取数据，创建一个 RDD。
2.  使用 `flatMap()` 将每一行文本拆分成单词。
3.  使用 `map()` 将每个单词映射成一个 `(单词, 1)` 的键值对。
4.  使用 `reduceByKey()` 对相同的单词进行计数聚合。
5.  通过 `collect()` 将最终结果收集到 Driver 端并打印。

```python
# 核心代码
file_rdd = sc.textFile("path/to/your/file")
words_rdd = file_rdd.flatMap(lambda line: line.split(" "))
word_pair_rdd = words_rdd.map(lambda x: (x, 1))
word_count_rdd = word_pair_rdd.reduceByKey(lambda a, b: a + b)
print(word_count_rdd.collect())
```

## 2. 不同环境下的运行示例

### 2.1 Windows 本地模式

- **文件**: `HelloWorld_win.py`
- **运行方式**: 在本地机器上模拟 Spark 环境运行。
- **关键配置**:
    - `setMaster("local[*]")`: 使用本地所有可用的 CPU核心。
    - **`PYSPARK_PYTHON` 环境变量**: 在 Windows 上，Java 进程可能找不到正确的 Python 解释器。代码中通过 `os.environ` 临时设置了此环境变量，这是确保 PySpark 在 Windows 上顺利运行的关键。建议在系统中永久设置此环境变量。

    ```python
    # 示例
    os.environ["PYSPARK_PYTHON"] = "D:\\path\\to\\your\\python.exe"
    conf = SparkConf().setMaster("local[*]").setAppName("HelloWorld")
    ```

### 2.2 Linux 本地模式

- **文件**: `HelloWorld_linux_local.py`
- **运行方式**: 在 Linux 机器上以本地模式运行。
- **关键配置**:
    - `setMaster("local[*]")`: 同上。
    - **HDFS 路径**: 示例直接从 HDFS 读取文件。这要求运行环境能够访问 HDFS 集群。
    - 在 Linux 环境下，`PYSPARK_PYTHON` 和 `JAVA_HOME` 等环境变量通常已在 `.bashrc` 或系统配置文件中设置好，因此代码中无需再次指定。

    ```python
    # 示例
    conf = SparkConf().setMaster("local[*]").setAppName("HelloWorld")
    sc.textFile("hdfs://namenode_host:8020/path/to/file")
    ```

### 2.3 Linux YARN 集群模式

- **文件**: `HelloWorld_linux_yarn.py`
- **运行方式**: 将应用提交到 YARN 集群上执行。
- **关键配置**:
    - `setMaster("yarn")`: 指定应用由 YARN 集群管理和调度。
    - **`HADOOP_CONF_DIR` 环境变量**: 必须设置此环境变量，指向 Hadoop 的配置文件目录（包含 `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml` 等），以便 Spark 客户端能找到并与 YARN ResourceManager 通信。
    - 其他环境变量如 `JAVA_HOME` 和 `HADOOP_USER_NAME` 也可能需要设置，以确保权限和环境正确。

    ```python
    # 示例
    os.environ['HADOOP_CONF_DIR'] = "/path/to/hadoop/etc/hadoop"
    conf = SparkConf().setMaster("yarn").setAppName("HelloWorld")
    ```
