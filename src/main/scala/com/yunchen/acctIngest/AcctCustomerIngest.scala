package com.yunchen.acctIngest

import java.util.Properties

import com.beust.jcommander.JCommander
import org.apache.spark.sql.{SparkSession}
import com.yunchen.utils._
import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast

class AcctCustomerIngest(){


}

object AcctCustomerIngest {

  private val log = Logger.getLogger(classOf[AcctCustomerIngest])
  val sTime: Long = System.currentTimeMillis
  val nameNode = "hdfs://10.10.10.132:8020"

  def main(args: Array[String]): Unit = {

    customerIdToIgnite(args)

  }

  def customerIdToIgnite(args: Array[String]): Unit ={
    //获取传入参数
    log.info("========================================== 初始化jcommander ==========================================")
    val argv = new Args()
    JCommander.newBuilder().addObject(argv).build().parse(args: _*)

    //创建sparksession
    val spark = SparkSession
      .builder()
      .appName("com.yunchen.acctCustomerIgest")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._


    val putNum = spark.sparkContext.longAccumulator("sendCount")
    val failNum = spark.sparkContext.longAccumulator("failCount")

    //初始化igniteContext
    log.info("========================================== 初始化ignite ==========================================")
    val igniteContext = new IgniteContext(spark.sparkContext, argv.igniteconfxml, true)
    val fromCache:IgniteRDD[String, String]  = igniteContext.fromCache(argv.cachename)

    //创建jdbc连接
    log.info("========================================== 初始化jdbc ==========================================")
    new jdbcUtils(jdbcUrl = "jdbc:mysql://10.10.10.132:3306", dbTable = "cm.zcustomer", dbUser = "root", dbPasswd = "cloudera")

    new datasetStructure(argv)

    log.info("========================================== 初始化kafka producer ==========================================")
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", argv.brokerlist)
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      log.warn("kafka producer init done!")
      spark.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    log.info("========================================== 初始化数据库链接、数据结构、kafkaSink成功.... ========================================== ")

    //判断信号文件,并将初始值写入数据库
    val eTime = System.currentTimeMillis
    val df1 = datasetStructure.jdbcDataListToDF(spark = spark)

    log.info("========================================== 信号文件检查 业务逻辑开始 ==========================================")
    if (CheckFile.isMatch(argv, false, "matchDay", log)) {
      log.info("信号文件检查成功")
      //Accepted save modes are 'overwrite', 'append', 'ignore', 'error'
      jdbcUtils.writeToDatabase(df1, "append",log)
      if (CheckFile.isMatch(argv, false, "matchSize",log)) {
        val df2 = datasetStructure.jdbcDataListToDF(finishTime = datasetStructure.dateToString,
          successNum = 0, failNum = 0, execTime = (eTime - sTime).toString, taskStatus = "SUCCESS", spark = spark)
        jdbcUtils.writeToDatabase(df2, "append",log)
      } else {
        val startTime = System.currentTimeMillis

        //读取数据
        log.info("========================================== 开始读取文件 ==========================================")
        val readFile = spark.read.textFile(argv.inputpath)
        val mapMsg = readFile.map(x => (x.split("\\!\\^")(0), x.split("\\!\\^")(1)))
        val filterErrMsg = mapMsg.filter(x => x._1.equals("NA"))
        val filterSuccMsg = mapMsg.filter(x => !(x._1.equals("NA")))

        //累加器
        val errCount = filterErrMsg.count()
        failNum.add(errCount)

        //写入kafka
        log.info("============================== filterErrMsg： "+errCount+" =========================================")
        if (errCount != 0) {
          if(hdfsUtls.exists(nameNode + argv.erroroutpath + argv.jobid)){
            hdfsUtls.deleteFile(nameNode + argv.erroroutpath + argv.jobid)
            filterErrMsg.write.format("csv").save(nameNode + argv.erroroutpath + argv.jobid)
          }else {
            filterErrMsg.write.format("csv").save(nameNode + argv.erroroutpath + argv.jobid)
          }

          log.info("=============================== json: " + datasetStructure.json("begin", argv.jobid, "9001", "parserError") + "============================================")

          filterErrMsg.rdd.foreach(x=>{
            val json = x._2
            log.info("=============================== json: " + datasetStructure.json(json, argv.jobid, "9001", "parserError") + "============================================")
            kafkaProducer.value.send(argv.topic, datasetStructure.json(json, argv.jobid, "9001", "parserError"))
          })

          /*          filterErrMsg.rdd.map(x=>{
                      val json = x._2
                      log.info("=============================== json: " + datasetStructure.json(json, argv.jobid, "9001", "parserError") + "============================================")
                      kafkaProducer.value.send(argv.topic, datasetStructure.json(json, argv.jobid, "9001", "parserError"))
                    }).collect()
         */
        }

        //往ignite里面写会覆盖掉相同key的数据
        log.info("========================================== 写入ignite ==========================================")
        //经测试这个可以实现自动覆盖相同key的值
        fromCache.savePairs(filterSuccMsg.rdd,true)

        //将运行结果写入数据库
        val endTime = System.currentTimeMillis
        val df3 = datasetStructure.jdbcDataListToDF(finishTime = datasetStructure.dateToString,
          successNum = putNum.value.toInt, failNum = failNum.value.toInt, execTime = (endTime - startTime).toString,
          taskStatus = if (0 == failNum.value) "SUCCESS";else "FAIL", spark = spark)

        jdbcUtils.writeToDatabase(df3, "append",log)

      }

    }

    igniteContext.close(true)
    spark.close()
  }

}
