package com.yunchen.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

class datasetStructure() extends Serializable {
  def this(argv:Args){
    this()
    datasetStructure.argv = argv
  }
}

object datasetStructure {

  var argv: Args = null

  private def jdbcColArray(): Array[String] = {
    return Array("taskId", "jobId", "taskStatus", "startTime", "finishTime", "execTime", "timeOut",
      "nodeName", "nodeIp", "nodeUser", "resultName", "successNum", "failNum", "batchNum")
  }

  def jdbcDataListToDF(taskId: String = "AKHZH_24", jobId: Int = argv.jobid.toInt, taskStatus: String = "RUNNING", startTime: String = dateToString,
                   finishTime: String = "", execTime: String = "", timeOut: String = "30", nodeName: String = "Spark任务", nodeIp: String = "",
                   nodeUser: String = "", resultName: String = "KafkaTopic:"+argv.topic, successNum: Int = 0, failNum: Int = 0,
                   batchNum: String = argv.inputpath.substring(argv.inputpath.lastIndexOf("_") + 1, argv.inputpath.length() - 4),
                   spark:SparkSession):DataFrame = {

    val dataList: List[(String, Int, String, String, String, String, String, String, String, String, String, Int, Int, String)]
    = List((taskId, jobId, taskId, startTime, finishTime, execTime, timeOut,
      nodeName, nodeIp, nodeUser, resultName, successNum, failNum, batchNum))

    import spark.implicits._
    val df = dataList.toDF(jdbcColArray(): _*)

    df
  }

  def json(parserFailMsg: String, jobid: String, errorCode: String, errorDesc: String): String = {
    val json = "{\"process\":" + "\"batcherror\"" + ",\"taskid\":" + "\"AKHZH_DEZJBD_BATCH\"" + ",\"jobid\":" + "\"" + jobid + "\"" + ",\"errorCode\":\"" + errorCode + "\",\"errorDesc\":\"" + errorDesc + "\",\"message\":\"" + parserFailMsg + "\",\"failnum\":\"1\"" + ",\"starttime\":\"" + dateToString + "\",\"finishtime\":\"" + dateToString + "\"}"
    json
  }

  def dateToString: String = {
    val date = new Date
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    val newDate = calendar.getTime
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val formatDate = sdf.format(newDate)
    formatDate
  }

}
