# coding:utf8
"""
演示使用happybase库，删除HBase表数据
"""

import happybase

# 1、获取数据库的连接
conn = happybase.Connection(
    host="ct104",
    port=9090 # Port使用HBase的ThriftServer，默认服务端口是：9090
)

# 2、获取表对象
table = conn.table("table1")

# 3、通过表对象中的delete方法，删除表数据
# 删除一个rowkey的一整行数据
table.delete('rk001')
# 删除指定rowkey的指定二级列
table.delete(
    row='rk002',
    columns=[
        'cf1:name',
        'cf2:chinese'
    ]
)

# 3、关闭连接
conn.close()
