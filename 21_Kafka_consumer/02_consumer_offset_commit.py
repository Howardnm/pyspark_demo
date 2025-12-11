"""
演示kafak的消费者代码
"""
# 导入KafkaConsumer对象
from kafka import KafkaConsumer

# 构建KafkaConsumer对象
consumer = KafkaConsumer(
    'testpython',  # topic
    group_id="mygp",    # 组id
    bootstrap_servers=['ct104:9092', 'ct105:9092', 'ct106:9092'],    # broker地址
    enable_auto_commit=False, # 关闭自动提交偏移量, 默认是True
    auto_commit_interval_ms=5000, # 自动提交偏移量的时间间隔，默认5000毫秒（开启enable_auto_commit=True才生效）
    auto_offset_reset='earliest'  # 当没有初始偏移量或当前偏移量在服务器上不存在时使用(默认latest从最新位置开始消费, earliest从最早位置开始消费)
)

# TODO: 关闭自动提交偏移量, 此时每次启动消费者, 都会从上次提交的偏移量开始消费数据，导致重复消费

# 通过for循环从consumer对象中取出message对象
for message in consumer:
    # for循环是无限循环,启动后就等着，有数据就干活没数据就一直等
    # 我们 需要的数据都在message对象内
    topic = message.topic           # 数据来自于哪个主题
    partition = message.partition   # 数据来自于哪个分区
    offset = message.offset         # 数据在当前分区内的下标
    key = message.key               # 数据的key
    value = message.value.decode("UTF-8")           # 数据本身，是字节数组需要反转回字符串

    print(f"取出数据，topic是：{topic}")
    print(f"取出数据，partition是：{partition}")
    print(f"取出数据，offset是：{offset}")
    print(f"取出数据，key是：{key}")
    print(f"取出数据，value是：{value}")

# TODO : 手动提交偏移量，就可以避免重复消费
    consumer.commit() # 手动提交偏移量
