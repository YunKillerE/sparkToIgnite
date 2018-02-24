#!/bin/bash

spark2-submit \
--class com.batch.acctingest.AcctCustomerIngest \
--master local \
--num-executors 2 --executor-cores 2 --executor-memory 2G --driver-memory 2G \
--conf spark.driver.userClassPathFirst=true \
--files /bigf/admin/ignite/ignite/ignite-config-client.xml \
./AcctCustIngest-0.0.1-SNAPSHOT.jar \
-encoding GBK -cachename D_ACCTCUST_MP \
-inputpath /tmp/d.akhzh_51001.dat \
-signalfilepath /tmp/akhzh_bigf_51001.list \
-igniteconfxml ignite-config-client.xml \
-errortopic spdblog \
-kerberosuser {{kerberos_user}} \
-kerberoskeypath {{kerberos_keypath}} \
-brokerlist {{kafka_address}} \
-mysqluser {{mysql_user}} \
-mysqlpassword {{mysql_password}} \
-jobid 6666
