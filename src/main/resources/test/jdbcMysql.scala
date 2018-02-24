package com.yunchen.test

import org.apache.spark.sql.SparkSession

object jdbcMysql {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .enableHiveSupport()
      .getOrCreate()

    val jdbcDF11 = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://10.10.10.132:3306")
      .option("dbtable", "cm.USERS")
      .option("user", "root")
      .option("password", "cloudera")
      .option("fetchsize", "3")
      .load()

    jdbcDF11.show

    val jdbcDF12 = spark.read.format("jdbc").options(
      Map(
        "driver" -> "com.mysql.jdbc.Driver",
        "url" -> "jdbc:mysql://10.10.10.132:3306",
        "dbtable" -> "cm.USERS",
        "user" -> "root",
        "password" -> "cloduera",
        "fetchsize" -> "3")).load()

    jdbcDF12.show


    val dataList: List[(Double, String, Double, Double, String, Double, Double, Double, Double)] = List(
      (0, "male", 37, 10, "no", 3, 18, 7, 4),
      (0, "female", 27, 4, "no", 4, 14, 6, 4),
      (0, "female", 32, 15, "yes", 1, 12, 1, 4),
      (0, "male", 57, 15, "yes", 5, 18, 6, 5),
      (0, "male", 22, 0.75, "no", 2, 17, 6, 3),
      (0, "female", 32, 1.5, "no", 2, 17, 5, 5),
      (0, "female", 22, 0.75, "no", 2, 12, 1, 3),
      (0, "male", 57, 15, "yes", 2, 14, 4, 4),
      (0, "female", 32, 15, "yes", 4, 16, 1, 2))

    val colArray: Array[String] = Array("affairs", "gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")

    import spark.implicits._

    val df = dataList.toDF(colArray: _*)

    df.write.mode("overwrite").format("jdbc").options(
      Map(
        "driver" -> "com.mysql.jdbc.Driver",
        "url" -> "jdbc:mysql://10.10.10.132:3306",
        "dbtable" -> "cm.spark",
        "user" -> "root",
        "password" -> "cloudera",
        "batchsize" -> "100",
        "truncate" -> "false")).save()

    df.write.mode("overwrite").format("jdbc").options(
      Map(
        "driver" -> "com.mysql.jdbc.Driver",
        "url" -> "jdbc:mysql://10.10.10.132:3306",
        "dbtable" -> "cm.spark",
        "user" -> "root",
        "password" -> "cloudera",
        "batchsize" -> "100",
        "truncate" -> "false")).save()


  }


}
