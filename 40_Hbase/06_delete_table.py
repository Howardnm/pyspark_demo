# coding:utf8
"""
演示使用happybase库，删除HBase表
"""

import happybase

# 1、获取数据库的连接
conn = happybase.Connection(
    host="ct104",
    port=9090 # Port使用HBase的ThriftServer，默认服务端口是：9090
)
# 2、删除表
conn.delete_table(name='table1', disable=True) # disable表示先停用该表，然后删除

# 3、关闭连接
conn.close()
