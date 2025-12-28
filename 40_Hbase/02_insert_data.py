# coding:utf8
"""
演示使用happybase库，向HBase表写入数据
"""

import happybase

# 1、获取数据库的连接
conn = happybase.Connection(
    host="ct104",
    port=9090 # Port使用HBase的ThriftServer，默认服务端口是：9090
)

# 2、获取表对象
table = conn.table("table1")

# 3、通过表对象中的put方法，插入数据
table.put(
    row='rk001', # 指定rowkey
    data={
        'cf1:name': 'xiaoming',
        'cf1:age': '18',
        'cf2:chinese': '99',
        'cf2:math': '79'
    }
)
# 注意：写入HBase的数据都 必须是字节数组
# 如果是字符串的话，会自动转换为字节数组

# 3、关闭连接
conn.close()

# 去base shell终端查看
# [hadoop@CT104 ~]$ hbase shell
# hbase(main):004:0> scan 'table1'
# ROW       COLUMN+CELL
#  rk001    column=cf1:age, timestamp=1766910302696, value=18
#  rk001    column=cf1:name, timestamp=1766910302696, value=xiaoming
#  rk001    column=cf2:chinese, timestamp=1766910302696, value=99
#  rk001    column=cf2:math, timestamp=1766910302696, value=79
# 1 row(s)



# 另一个例子：
def test_insert_data_table(self) -> None:
    """插入一条数据"""
    # 获得表对象
    table = self.conn.table("WATER_BILL")
    table.put(row=b'4944191', data={
        # 中文建议使用bytes(str, encoding)转换为bytes对象
        b'C1:name': bytes('登卫红', encoding="utf-8"),
        b'C1:address': bytes('贵州省铜仁市德江县7单元267室', encoding="utf-8"),
        b'C1:sex': bytes('男', encoding="utf-8")
    })
