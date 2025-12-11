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
# 设置ack参数，控制消息发送的可靠性
producer = KafkaProducer(
    bootstrap_servers=['ct104:9092', 'ct105:9092', 'ct106:9092'],
    ack=0,	    # 设置ack0，表示不等待broker确认
    #ack=1      # 设置ack1，表示等待leader确认
    #ack='all'	# 设置ack -1，表示等待leader和follower（所有副本）确认
)
# - ack为0，性能最好，数据很可能丢失
# - ack为1，性能略好，数据也有一定安全性
# - ack为-1，性能最差，安全性最高

# TODO: 设置ack参数，控制消息发送的可靠性

# 调用生产者的send方法即可发送数据
# 发送一条消息，并获取返回的futher对象
futher = producer.send('testpython', f"I love you".encode("UTF-8"))

# 输出futher对象
result = futher.get(10) # 阻塞等待10秒钟，获取发送结果
print(result.topic) # 输出主题名称
print(result.partition) # 输出分区编号
print(result.offset) # 输出偏移量，表示当前消息在分区中的位置

