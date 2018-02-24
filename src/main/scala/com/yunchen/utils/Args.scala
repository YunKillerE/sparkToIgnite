package com.yunchen.utils

import com.beust.jcommander.Parameter

class Args extends Serializable {

  /**
    * IgniteCacheName
    */
  @Parameter(names = Array("-cachename"), required = true) var cachename: String = null

  /**
    * IgniteConfigMXL
    */
  @Parameter(names = Array("-igniteconfxml"), required = true) var igniteconfxml: String = null

  /**
    * InputEncoding
    */
  @Parameter(names = Array("-encoding"), required = true) var encoding: String = null

  /**
    * InputPath
    */
  @Parameter(names = Array("-inputpath"), required = true) var inputpath: String = null

  /**
    * JobId
    */
  @Parameter(names = Array("-jobid"), required = true) var jobid: String = null

  /**
    * topic
    */
  @Parameter(names = Array("-errortopic"), required = true) var topic: String = null

  /**
    * KafkaBrokerList
    */
  @Parameter(names = Array("-brokerlist"), required = true) var brokerlist: String = null

  /**
    * Data signalFilePath
    */
  @Parameter(names = Array("-signalfilepath"), required = true) var signalfilepath: String = null

  /**
    * kerberosUser
    */
  @Parameter(names = Array("-kerberosuser"), required = false) var kerberosuser: String = null

  /**
    * kerberosPass
    */
  @Parameter(names = Array("-kerberospass"), required = false) var kerberospass: String = null

  /**
    * errorOutPath
    */
  @Parameter(names = Array("-erroroutpath"), required = true) var erroroutpath: String = null

  /**
    * OracleURL
    */
  @Parameter(names = Array("-oracleurl"), required = false) var oracleurl: String = null

  /**
    * OracleDriverName
    */
  @Parameter(names = Array("-driver"), required = false) var driver = "oracle.jdbc.driver.OracleDriver"

  /**
    * OracleDBTable
    */
  @Parameter(names = Array("-dbtable"), required = false) var dbtable = "TBL_JOB"

  /**
    * Oracleusername
    */
  @Parameter(names = Array("-user"), required = false) var user: String = null

  /**
    * Oraclepassword
    */
  @Parameter(names = Array("-password"), required = false) var password: String = null


}
