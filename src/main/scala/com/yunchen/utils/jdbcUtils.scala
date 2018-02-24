package com.yunchen.utils

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

class jdbcUtils {

  def this(jdbcUrl:String) {
    this()
    jdbcUtils.jdbcUrl = jdbcUrl
  }

  def this(jdbcUrl:String, dbTable:String, dbUser:String, dbPasswd:String, batchNum:String = "100",
               isTruncate:String = "false") {
    this(jdbcUrl)
    jdbcUtils.dbTable = dbTable
    jdbcUtils.dbUser = dbUser
    jdbcUtils.dbPasswd = dbPasswd
    jdbcUtils.batchNum = batchNum
    jdbcUtils.isTruncate = isTruncate
  }
}

object jdbcUtils {

  private var jdbcUrl,dbTable,dbUser,dbPasswd,batchNum,isTruncate,driver = ""

  private def jdbcArgv():Map[String, String] = {
    val map =  Map(
      "driver" -> "com.mysql.jdbc.Driver",
      "url" -> jdbcUtils.jdbcUrl,
      "dbtable" -> jdbcUtils.dbTable,
      "user" -> jdbcUtils.dbUser,
      "password" -> jdbcUtils.dbPasswd,
      "batchsize" -> jdbcUtils.batchNum,
      "truncate" -> jdbcUtils.isTruncate)
     map
  }

  def writeToDatabase(df:DataFrame, mode:String, log:Logger) = {
    log.info("================== jdbcMap = "+jdbcArgv()+"====================================")
    df.write.mode(mode).format("jdbc").options(jdbcArgv()).save()
  }


  def loadTabelToDF(spark:SparkSession) = {
    spark.read.format("jdbc").options(jdbcArgv()).load()
  }

  /**
    * 希望可以实现更新，但是spark并没有提供更新的savemode，只支持'overwrite', 'append', 'ignore', 'error'四种
    * 方法是先整表读取出来，然后比较两个df，然后更新df，然后再overwrite写入
    * @param df
    * @param spark
    * @param mode
    * @param log
    */
  def upsert(df:DataFrame, spark:SparkSession, mode:String, log:Logger) = {
    val mysqlDF = loadTabelToDF(spark)
    mysqlDF.createOrReplaceTempView("t1")
    df.createOrReplaceTempView("t1")
    spark.sql(s"update t1 set t1.dc=t2.dc1 FROM t1,t2 WHERE t1.jobid = t2.jobid")

  }

}
