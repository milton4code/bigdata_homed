package cn.ipanel.homed.general

import cn.ipanel.common.{CluserProperties, GatherType, SourceType, SparkSession}
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ListBuffer

/**
  *开机用户导出报表
  * create  liujjy
  * time    2019-06-16 0016 17:18
  */
object OnlineUserExport {

  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("请输入日期,例如 20190101")
      sys.exit(-1)
    }


    val sparkSession = SparkSession("OnlineUserExport")
    val sc=sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    val day = args(0)
    val nextDay = DateUtils.getAfterDay(day,targetFormat = DateUtils.YYYY_MM_DD)

    //用户ID、设备号、类型、姓名、电话、身份证、地址,时长

    //截止当天用户总量
//    val allUserDF =
     val allUserSql =
       s"""
         |(SELECT f_uniq_id,f_user_id,f_device_id,f_terminal,f_account_name,f_nick_name,f_address_name,f_mobile_no,f_certificate_no
         |, f_province_id,f_city_id,f_region_id,f_province_name,f_city_name,f_region_name
         |from t_user
         |where f_create_time <= '$nextDay') t
       """.stripMargin
    val allUserDF = DBUtils.loadDataFromPhoenix2(sqlContext,allUserSql)

    //在线用户
    val onlinUserDF = sqlContext.sql(s"select user_id f_user_id,device_id f_device_id from bigdata.orc_user where day='$day' ")

    //获取每个用户在线时长
    // 计算开机时长只支持4中情况:用户上报,run日志,websocket,用户上报websocket日志混合
    val resources = CluserProperties.LOG_SOURCE
    var df:DataFrame =  getDefaultDF(sqlContext)
    //只有用户上报
    if(resources.contains(SourceType.USER_REPORT)  && ! resources.contains(SourceType.WEBSOCKET)){
      df = getReportLog(sqlContext,day)
    }
    //只有run日志
    if(resources.contains(SourceType.RUN_LOG) &&
      ! resources.contains(SourceType.WEBSOCKET) &&  !resources.contains(SourceType.USER_REPORT)){
      df = getRunLogDF(sqlContext,day)
    }

    //只有websocket
    if(resources.contains(SourceType.WEBSOCKET)  && ! resources.contains(SourceType.USER_REPORT)){
      df = getWebsocketDF(sqlContext,day)
    }

    //用户上报websocket日志混合两种混用
    if ( resources.contains(SourceType.USER_REPORT)  && resources.contains(SourceType.WEBSOCKET) ){
      val webSocketDF = getWebsocketDF(sqlContext,day)
      val reportDF = getReportLog(sqlContext,day)
      webSocketDF.persist()
      reportDF.persist()

      webSocketDF.unionAll(reportDF).registerTempTable("t_all_users")
      df = sqlContext.sql(
        """
          |select  f_user_id,f_device_id,max(f_play_time) f_play_time
          |from t_all_users
          |group by  user_id,device_id
        """.stripMargin)

      webSocketDF.unpersist()
      reportDF.unpersist()
    }


    val tmp = df.join(onlinUserDF,Seq("f_device_id","f_user_id")).join(allUserDF,Seq("f_device_id","f_user_id"))
         .selectExpr(s"'$day' as f_date","f_user_id","f_account_name","f_nick_name","f_province_id","f_city_id",
          "f_region_id","f_province_name","f_city_name","f_region_name","f_address_name","f_mobile_no","f_certificate_no",
        "f_device_id","f_terminal","f_uniq_id","f_play_time")

    DBUtils.saveDataFrameToPhoenixNew(tmp,"t_user_boot")

  }




  /**
    * 获取websokect日志开机时间
    * @return user_id|device_id|device_type|region_id|play_time
    */
  private def getWebsocketDF(hiveContext: HiveContext,day: String): DataFrame = {
    hiveContext.udf.register("getTime"
      ,(loginTime_time: String, logoutTime: String, day: String) => getTime(loginTime_time, logoutTime, day))

    val day2 = DateUtils.transformDateStr(day, format2 = DateUtils.YYYY_MM_DD_HHMMSS)
    val _websocketDF = hiveContext.sql(
      s"""
         |select f_user_id  ,f_device_id ,
         |       getTime(f_login_time,f_logout_time,'$day2') as  f_play_time
         |from bigdata.t_user_online_history
         |where day = '$day'
      """.stripMargin)
     _websocketDF
  }

  private def getTime(loginTime_time: String, logoutTime: String, day: String): Long = {
    val f_login_time = if (loginTime_time < day) DateUtils.dateToUnixtime(day) else DateUtils.dateToUnixtime(loginTime_time)
    val f_logout_time = DateUtils.dateToUnixtime(logoutTime)
    val onLineTime = f_logout_time - f_login_time
    Math.ceil(onLineTime).toLong
  }

  /**
    * 获取run日志数据
    */
  private def getRunLogDF(hiveContext: HiveContext,day: String):DataFrame = {
    hiveContext.sql(
      s"""
         |select userid f_user_id ,deviceid f_device_id, playtime f_play_time
         |from bigdata.orc_video_play
         |where day='$day'
      """.stripMargin)
  }

  /**
    * 获取上报日志开机时间
    * @return user_id|device_id|device_type|region_id|play_time
    */
  private def getReportLog(hiveContext: HiveContext, day: String):DataFrame ={
    hiveContext.sql(
      s"""
         |select userId as f_user_id,deviceId f_device_id,
         |count(1) * ${CluserProperties.HEART_BEAT} as f_play_time
         |from bigdata.orc_report_behavior
         |where day='$day' and reporttype='${GatherType.SYSTEM_HEARTBEAT}'
         |group by userId,deviceId
       """.stripMargin)
  }
  /**
    *默认DataFrame
    */
  private def getDefaultDF(hiveContext: HiveContext): DataFrame = {
    var listBuffer = new ListBuffer[User]()
    listBuffer.append(User())
    hiveContext.createDataFrame(listBuffer)
  }

  private case class User(f_uniq_id:String="",f_user_id:String="",f_device_id:Long=0L,f_terminal:Long=0L,
                          f_account_name:String="",f_address_name:String="",f_mobile_no:String="",
                          f_certificate_no:String="",f_play_time:Double=0.0D)
}
