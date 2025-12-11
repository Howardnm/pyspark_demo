"""
演示Kafka的生产者代码

先创建topic主题:
cd /export/server/kafka
bin/kafka-topics.sh --create --bootstrap-server ct104:9092,ct105:9092,ct106:9092 --topic testpython --partitions 3

去kafka tool工具查看数据

"""
# 导入包，导入KafkaProducer对象
from kafka import KafkaProducer
import random

# 构建生产者对象(class)
producer = KafkaProducer(bootstrap_servers=['ct104:9092', 'ct105:9092', 'ct106:9092'])

nums = [0,0,0,1,1,1,2,2,2,2,2,2] # 模拟数据量不均衡的分区选择

# TODO: partition 指定分区发送数据

# 调用生产者的send方法即可发送数据
for i in range(10):
    futher = producer.send(
        topic='testpython',
        value=f"I love you".encode("UTF-8"),
        partition=random.choice(nums) # 指定分区编号发送数据（不写该参数，默认是随机分区）
    )

    # 输出futher对象
    result = futher.get(10) # 阻塞等待10秒钟，获取发送结果
    print(result.topic) # 输出主题名称
    print(result.partition) # 输出分区编号
    print(result.offset) # 输出偏移量，表示当前消息在分区中的位置
