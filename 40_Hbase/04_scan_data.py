# coding:utf8
"""
演示使用happybase库，扫描HBase表的数据
"""

import happybase

# 1、获取数据库的连接
conn = happybase.Connection(
    host="ct104",
    port=9090 # Port使用HBase的ThriftServer，默认服务端口是：9090
)

# 2、获取表对象
table = conn.table("table1")

# 3、通过表对象中的scan方法，扫描表的数据
result = table.scan(        # 不指定参数，默认扫描全表
    # row_start=10,
    # row_stop=20,
    # row_prefix=None,
    # columns=[             # 指定查询列族、二级列（如果不写，默认显示rowkey的一整行数据）
    #     'cf1',
    #     'cf2:math'
    # ],
    # filter=None,
    # timestamp=None,
)

# 4、打印输出
# print(result)
# print(type(result))
# print(list(result))
    # <generator object Table.scan at 0x7690bd12f900>
    # <class 'generator'>
    # [(b'rk001', {b'cf1:age': b'18', b'cf1:name': b'xiaoming', b'cf2:chinese': b'99', b'cf2:math': b'79'})]

# 解释：
# result对象是一个生成器，可以通过for循环取出
# 由于生成器特效，只能for循环一次，第二次for循环为空数据
# 输出为list类型,每个元素为二元元组：('rowkey主键', {一整行数据})
# 二元元组中，元素1：rowkey主键
# 二元元组中，元素2：{'列族:二级列': '数据', '列族:二级列': '数据', '列族:二级列': '数据'}


# 4.1、遍历输出
for row in result:
    rowkey = row[0].decode('UTF-8')
    data_dict:dict = row[1]
    for key, value in data_dict.items():
        print(rowkey, '\t', key.decode('UTF-8'),'\t',value.decode('UTF-8'))


# 3、关闭连接
conn.close()

# 输出结果：
# rk001 	 cf1:age 	 19
# rk001 	 cf1:name 	 dada
# rk001 	 cf2:chinese 	 60
# rk001 	 cf2:math 	 10
# rk002 	 cf1:age 	 19
# rk002 	 cf1:name 	 lili
# rk002 	 cf2:chinese 	 70
# rk002 	 cf2:math 	 60