# coding:utf8

from pyhive import hive

if __name__ == '__main__':
    # 获取到Hive(Spark ThriftServer的链接)
    conn = hive.Connection(host="ct104", port=10000, username="hadoop")

    # 获取一个游标对象
    cursor = conn.cursor()

    # 执行SQL
    cursor.execute("SELECT * FROM test")

    # 通过fetchall API 获得返回值
    result = cursor.fetchall()

    print(result)

    # [(1, '王力红', '男'), (2, '周杰轮', '男'), (3, '林志灵', '女')]