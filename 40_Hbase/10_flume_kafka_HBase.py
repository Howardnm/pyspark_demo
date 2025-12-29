# coding:utf8
"""
读取kafka写入hbase
"""
from kafka import KafkaConsumer
import happybase
from hashlib import md5

# 1.1、构建kafka的消费者
consumer = KafkaConsumer(
    'MOMO',  # topic
    group_id="mygp1",  # 组id
    bootstrap_servers=['ct104:9092', 'ct105:9092', 'ct106:9092']  # broker地址
)

# 1.2、构建HBase的数据库连接
conn = happybase.Connection("ct104")
# 获取HBase的table对象
table = conn.table("momo")


# 2、取出kafka的数据
for message in consumer:
    value = message.value.decode("utf-8")
    arr = value.split('\x01')
    # print(arr)
    # ['2025-12-29 13:10:23', '刘宏扬', '13284435285', '女', '12.177.114.127', 'Android 6', '华为 荣耀Play4T', '4G', '86.756945,31.461029', '单于孤兰', '65.199.49.186', '13385404842', 'IOS 9.0', '华为 荣耀畅玩9A', '4G', '111.188555,29.678076 ', '男', 'TEXT', '87.07KM', '有一种想见不敢见的伤痛，这一种爱还埋藏在我心中，让我对你的思念越来越浓，我却只能把你你放在我心中。 ']

    # 写入HBase的rowkey设置为：md5(发件人_收件人)_时间，采用md5的16进制字符串
    rowkey = md5(f"{arr[1]}_{arr[9]}".encode("utf-8")).hexdigest() + "_" + arr[0]
    # print(rowkey)
    # 0031f51ac58bd4d7495bda23ca774731_2025-12-29 13:15:44

    data_dict = {
        b'cf1:msg_time': bytes(arr[0], encoding="UTF-8"),
        b'cf1:sender_nickname': bytes(arr[1], encoding="UTF-8"),
        b'cf1:sender_account': bytes(arr[2], encoding="UTF-8"),
        b'cf1:sender_sex': bytes(arr[3], encoding="UTF-8"),
        b'cf1:sender_ip': bytes(arr[4], encoding="UTF-8"),
        b'cf1:sender_os': bytes(arr[5], encoding="UTF-8"),
        b'cf1:sender_phone_type': bytes(arr[6], encoding="UTF-8"),
        b'cf1:sender_network': bytes(arr[7], encoding="UTF-8"),
        b'cf1:sender_gps': bytes(arr[8], encoding="UTF-8"),
        b'cf1:receiver_nickname': bytes(arr[9], encoding="UTF-8"),
        b'cf1:receiver_ip': bytes(arr[10], encoding="UTF-8"),
        b'cf1:receiver_account': bytes(arr[11], encoding="UTF-8"),
        b'cf1:receiver_os': bytes(arr[12], encoding="UTF-8"),
        b'cf1:receiver_phone_type': bytes(arr[13], encoding="UTF-8"),
        b'cf1:receiver_network': bytes(arr[14], encoding="UTF-8"),
        b'cf1:receiver_gps': bytes(arr[15], encoding="UTF-8"),
        b'cf1:receiver_sex': bytes(arr[16], encoding="UTF-8"),
        b'cf1:msg_type': bytes(arr[17], encoding="UTF-8"),
        b'cf1:distance': bytes(arr[18], encoding="UTF-8"),
        b'cf1:message': bytes(arr[19], encoding="UTF-8")
    }

    # 插入数据到hbase的momo表
    table.put(rowkey, data_dict)
    print(f"插入Rowkey: {rowkey}")

# 关闭HBase和Kafka链接
conn.close()
consumer.close()