# 目的

从hdfs上读取源数据写入ignite


# 需求

1. 日志记录

2. 错误信息输出到kafka

3. 执行状态记录到mysql

4. 需要链接安全的kerberos的hadoop集群

5. 可定制输入参数


# 环境

spark 2.2

# 编译

mvn clean scala:compile package

执行参考bin目录

# 遗留问题

由于spark的saveMode支持'overwrite', 'append', 'ignore', 'error'四种模式，并没有提供update，有空看看怎么实现比较好


