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


```
/opt/spark/bin/spark-submit \
--principal bigf/bigf \
--keytab bigf.keytab \
--num-executors 2 --executor-cores 2 --executor-memory 2G --driver-memory 2G \
--driver-class-path /usr/share/java/mysql-connector-java.jar:jcommander-1.72.jar \
--class com.yunchen.acctIngest.AcctCustomerIngest \
--master yarn \
--deploy-mode client \
--files /opt/ignite/ignite-config-client.xml \
./AcctCustIngestScala-0.1-SNAPSHOT.jar \
-encoding GBK \
-cachename JENNY \
-inputpath  /tmp/spark/d.akhzh_20171110.dat \
-signalfilepath /tmp/spark/akhzh_bigf_20171110.list \
-erroroutpath /tmp/ \
-igniteconfxml ignite-config-client.xml \
-errortopic spdblog -brokerlist 10.10.10.132:9092 \
-jobid 9
```

# 遗留问题

由于spark的saveMode支持'overwrite', 'append', 'ignore', 'error'四种模式，并没有提供update，有空看看怎么实现比较好


