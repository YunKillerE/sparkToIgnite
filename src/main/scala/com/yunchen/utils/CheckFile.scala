package com.yunchen.utils

import org.apache.log4j.Logger

/**
  * 1，检查文件日期是否正确
  * 2，检查信号文件大小是否对的上
  */

object CheckFile {


  def isMatch(args: Args, isCopy: Boolean = false, matchDayOrSize: String, log:Logger):Boolean = {

    var flag = false

    val filesize = hdfsUtls.getFileSize(args.inputpath).toString()
    log.info("================ filesize = "+filesize+"=======================")

    val filecontent = hdfsUtls.getFileContent(args.signalfilepath).toString().replaceAll("(\0|\\s*|\r|\n)", "")
    log.info("================ filecontent = "+filecontent+"=======================")

    if (isCopy){
      hdfsUtls.copyFolder(args.inputpath,"/user/batch/D_ACCTCUST_MP/akhzh24")
    }

    val spl = filecontent.split("\\|")
    log.info("================ spl = "+spl+"=======================")
    val split = filesize.split(" ")
    log.info("================ split = "+split+"=======================")

    matchDayOrSize match {
      case "matchDay" =>
        log.info("================ split(0) = "+split(0)+"=======================")
        log.info("================ spl(1) = "+spl(1)+"=======================")
        if (split(0) == spl(1) && args.inputpath.contains(spl(0))) flag = true
      case "matchSize" =>
        log.info("================ split(0) = "+split(0)+"=======================")
        log.info("================ spl(1) = "+spl(1)+"=======================")
        if (spl(1) == "0" && split(0) == "0") flag = true
      case _ =>
        flag = false
    }

    flag
  }

  object MatchType extends Enumeration{

    //这行是可选的，类型别名，在使用import语句的时候比较方便，建议加上
    type MatchType = Value
    //枚举的定义
    val matchDay = "matchDay"
    val matchSize = "matchSize"
  }

}
