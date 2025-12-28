# coding:utf8
"""
演示使用happybase库，操作HBase创建表

### 前期准备：
    - Hbase 的 pyhton api 需要安装一个第三方的库：`happybase`
    - Hbase 需要启动 ThriftServer 服务
1、安装HappyBase库（我就在ct104）
    # 切换到spark、python3.8的虚拟环境：
    conda activate pyspark
    # 安装happybase库
    pip install happybase

2、在其中一台HBase服务器，安装相关依赖
    yum -y install automake libtool flex bison pkgconfig gcc-c++ boost-devel libevent-devel zlib-devel python-devel ruby-devel openssl-devel
    # 启动HBase的ThriftServer，他的服务端口是：9090
        su - hadoop
        $HBASE_HOME/bin/hbase-daemon.sh start thrift
    # 验证启动端口
        netstat -anp|grep 9090
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