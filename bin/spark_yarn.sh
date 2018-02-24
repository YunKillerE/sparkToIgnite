#!/bin/bash

spark2-submit \
--class com.batch.acctingest.AcctCustomerIngest \
--master yarn --deploy-mode cluster \
--files /bigf/admin/ignite/ignite/ignite-config-client.xml \
--num-executors 4 --executor-cores 2 --executor-memory 4G --driver-memory 6G \
./AcctCustIngest-0.0.1-SNAPSHOT.jar \
-encoding GBK -cachename D_ACCTCUST_MP \
-inputpath  /tmp/d.akhzh_20171110.dat \
-signalfilepath /tmp/akhzh_bigf_20171110.list \
-erroroutpath /tmp/ \
-igniteconfxml ignite-config-client.xml \
-errortopic spdblog \
-kerberosuser {{kerberos_user}} \
-kerberoskeypath {{kerberos_keypath}} \
-brokerlist {{kafka_address}} \
-mysqluser {{mysql_user}} \
-mysqlpassword {{mysql_password}} \
-jobid 6666