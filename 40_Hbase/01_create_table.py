# coding:utf8
"""
演示使用happybase库，操作HBase创建表
"""

import happybase

# 1、获取数据库的连接
conn = happybase.Connection(
    host="ct104",
    port=9090 # Port使用HBase的ThriftServer，默认服务端口是：9090
)

# 2、通过连接对象的create_table API创建表
conn.create_table(
    name="table1",
    families={
        'cf1': dict(),                 # cf1列族，默认属性
        'cf2': dict(max_versions=10),  # cf2列族，版本最大10
    }
)

# 3、关闭连接
conn.close()

# 去base shell终端查看
# [hadoop@CT104 ~]$ hbase shell
# hbase(main):001:0> list
# table1