"""
演示Kafka的生产者代码

先创建topic主题:
cd /export/server/kafka
bin/kafka-topics.sh --create --bootstrap-server ct104:9092,ct105:9092,ct106:9092 --topic testpython --partitions 3

去kafka tool工具查看数据

"""
# 导入包，导入KafkaProducer对象
from kafka import KafkaProducer

# 构建生产者对象(class)
producer = KafkaProducer(bootstrap_servers=['ct104:9092', 'ct105:9092', 'ct106:9092'])

# 调用生产者的send方法即可发送数据
for i in range(10):
    producer.send('testpython', f"I love you {i}".encode("UTF-8"))

# 清空缓冲区提交
producer.flush()
