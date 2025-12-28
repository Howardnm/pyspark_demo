# coding:utf8
"""
演示使用happybase库，查询HBase表单条数据
"""

import happybase

# 1、获取数据库的连接
conn = happybase.Connection(
    host="ct104",
    port=9090 # Port使用HBase的ThriftServer，默认服务端口是：9090
)

# 2、获取表对象
table = conn.table("table1")

# 3、通过表对象中的row方法，查询表中单条数据
result = table.row(
    row='rk001',          # 指定查询rowkey
    columns=[             # 指定查询列族、二级列（如果不写，默认显示rowkey的一整行数据）
        'cf1',
        'cf2:math'
    ]
)

# 4、打印输出，输出为字典类型（HBase取出的数据都是字符串类型，请自行转换）
print(result)
print(type(result))

# 4.1、遍历输出（并utf8解码）
for key in result.keys():
    print(key.decode('UTF-8'),'\t',result[key].decode('UTF-8'))

# 3、关闭连接
conn.close()


# 输出结果：
# {b'cf1:name': b'xiaoming'}
# <class 'dict'>

# 如果不指定列族，输出结果：
# {b'cf1:age': b'18', b'cf1:name': b'xiaoming', b'cf2:chinese': b'99', b'cf2:math': b'79'}
# <class 'dict'>

# 遍历输出：
# cf1:age 	 18
# cf1:name 	 xiaoming
# cf2:math 	 79