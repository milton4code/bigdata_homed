package cn.ipanel.homed.realtime

import cn.ipanel.common.SparkSession
import cn.ipanel.utils.DateUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * 实时统计入口程序 created by lizhy@20190307
  */
object RealtimeCountProcesser {
  def main(args: Array[String]): Unit = {
    println("实时统计开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    val sparkSession: SparkSession = SparkSession("RealtimeCountProcesser")
    val sc: SparkContext = sparkSession.sparkContext
    val hiveContext: HiveContext = sparkSession.sqlContext
    var duration = 2
    var partAmt = 200
    try{
      if(args.length == 2){
        duration = args(0).toInt
        partAmt = args(1).toInt
      }else if(args.length == 1){
        duration = args(0).toInt
      }
    }catch {
      case e:Exception => {
        println("输入参数无效,按默认变量值执行程序！")
        e.printStackTrace()
      }
    }
    while (true){
      val currDateTime = DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS)
      if(currDateTime.substring(15,16).toInt%2 == 0 && currDateTime.substring(16) == ":30"){
        val nodeTime = DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMM)
        println("    1.在线用户统计开始：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
        OnlineUsers.countOnlineUsers(sparkSession,hiveContext,duration,partAmt,nodeTime) //在线用户统计
        println("      在线用户统计结束：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
        println("    2.直播统计开始：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
        ChannelLive.channelLiveCount(sparkSession,hiveContext,duration,partAmt,nodeTime) //直播统计
        println("      直播统计结束：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
        println("    3.点播统计开始：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
        ProgramDemand.programDemandCount(sparkSession,hiveContext,duration,partAmt,nodeTime) //点播统计
        println("      点播统计结束：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
        println("    4.回看统计开始：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
        ProgramLookback.programLookbackCount(sparkSession,hiveContext,duration,partAmt,nodeTime) //点播统计
        println("      回看统计结束：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
        println("实时统计结束："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
        hiveContext.clearCache()
      }else{
//        println(DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
        Thread.sleep(1000)
      }
    }
  }
}
