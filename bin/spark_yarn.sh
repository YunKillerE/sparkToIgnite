#!/bin/bash


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