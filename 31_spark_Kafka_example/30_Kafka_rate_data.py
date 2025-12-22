# coding:utf8
"""
模拟生成物联网设备数据的代码
"""

import json
import random
import sys
import time
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError

# # 锁定远端操作环境, 避免存在多个版本环境的问题
# os.environ['SPARK_HOME'] = '/export/server/spark'
# os.environ["PYSPARK_PYTHON"] = "/root/anaconda3/bin/python"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "/root/anaconda3/bin/python"

# 快捷键:  main 回车
if __name__ == '__main__':
    print("模拟物联网数据")

    # 1- 构建一个kafka的生产者:
    producer = KafkaProducer(
        bootstrap_servers=['ct104:9092', 'ct105:9092', 'ct106:9092'],
        acks='all',
        value_serializer=lambda m: json.dumps(m).encode("utf-8")
    )
    # 2- 物联网设备类型
    deviceTypes = ["洗衣机", "油烟机", "空调", "窗帘", "灯", "窗户", "煤气报警器", "水表", "燃气表"]

    while True:
        index = random.choice(range(0, len(deviceTypes)))
        deviceID = f'device_{index}_{random.randrange(1, 20)}'
        deviceType = deviceTypes[index]
        deviceSignal = random.choice(range(10, 100))

        # 组装数据集
        print({'deviceID': deviceID, 'deviceType': deviceType, 'deviceSignal': deviceSignal,
               'time': time.strftime('%Y%m%d')})

        # 发送数据到iot主题
        producer.send(topic='iot',
                      value={'deviceID': deviceID, 'deviceType': deviceType, 'deviceSignal': deviceSignal,
                                       'time': time.strftime('%Y%m%d')}
        )

        # 间隔时间 5s内随机
        time.sleep(random.choice(range(1, 5)))
