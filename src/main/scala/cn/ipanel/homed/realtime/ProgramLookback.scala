package cn.ipanel.homed.realtime

import cn.ipanel.common.{DBProperties, GatherType, SparkSession, Tables}
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

object ProgramLookback {
  def main(args: Array[String]): Unit = {
    println("实时回看统计开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    val sparkSession: SparkSession = SparkSession("LookbackRealtime")
    val sc: SparkContext = sparkSession.sparkContext
    val sqlContext: HiveContext = sparkSession.sqlContext
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
    /*while (true){
      val currDateTime = DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS)
      if(currDateTime.substring(15,16).toInt%2 == 0 && currDateTime.substring(16) == ":30"){*/
        val nodeTime = DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMM)
        println("    4.回看统计开始：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
        ProgramLookback.programLookbackCount(sparkSession,sqlContext,duration,partAmt,nodeTime) //点播统计
        println("      回看统计结束：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
        println("实时统计结束："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
      /*}else{
        Thread.sleep(1000)
      }
    }*/
  }
  /**
    * 统计回看
    * @param sparkSession
    * @param sqlContext
    * @param duration
    * @param partAmt
    * @param nodeTime
    */
  def programLookbackCount(sparkSession: SparkSession, sqlContext:HiveContext, duration:Int, partAmt:Int, nodeTime:String): Unit ={
    import sqlContext.implicits._
    val sc = sparkSession.sparkContext
    val nodeTimeBC = sc.broadcast(nodeTime)
    //区域信息
    val regionBC = sc.broadcast(UserRegion.loadRegionInfoMap(sparkSession))
    //回看媒资信息获取
    val lookbackMediaDf = getLookbackMediaDf(sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    /*println("lookbackMediaDf:")
    lookbackMediaDf.show(100)*/
    //当前点播在线用户
    val lookbackOnlineDf = getLookbackFromRunLogDf(sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    /*println("lookbackOnlineDf:")
    lookbackOnlineDf.show(100)*/
    val lookbackGroupDf = lookbackOnlineDf
      .groupBy("f_terminal","f_region_id","f_program_id")
      .agg("f_user_id" -> "count")
      .withColumnRenamed("count(f_user_id)","f_user_count")
    val lookbackJoinDf = lookbackGroupDf.join(lookbackMediaDf,lookbackGroupDf("f_program_id")===lookbackMediaDf("f_event_id"),"left_outer")
      .map(x => {
        val terminal = x.getAs[Int]("f_terminal")
        val regionId = x.getAs[String]("f_region_id")
        val (provinceId,provinceName,cityId,cityName,regionName) = regionBC.value.getOrElse(regionId,("unknown","unknown","unknown","unknown","unknown"))
        val channelId = x.getAs[String]("f_channel_id")
        val channelName = x.getAs[String]("f_channel_name")
        val programId = x.getAs[String]("f_event_id")
        val programName = x.getAs[String]("f_event_name")
        val contentType = x.getAs[Int]("f_content_type").toString
        val userCount = x.getAs[Long]("f_user_count").toInt
        val playTime = userCount.toFloat * duration / 60
        (terminal,provinceId,provinceName,cityId,cityName,regionId,regionName,channelId,channelName,programId,programName,contentType,playTime,userCount,nodeTimeBC.value)
      }).toDF()
      .withColumnRenamed("_1","f_terminal")
      .withColumnRenamed("_2","f_province_id")
      .withColumnRenamed("_3","f_province_name")
      .withColumnRenamed("_4","f_city_id")
      .withColumnRenamed("_5","f_city_name")
      .withColumnRenamed("_6","f_region_id")
      .withColumnRenamed("_7","f_region_name")
      .withColumnRenamed("_8","f_channel_id")
      .withColumnRenamed("_9","f_channel_name")
      .withColumnRenamed("_10","f_program_id")
      .withColumnRenamed("_11","f_program_name")
      .withColumnRenamed("_12","f_type_id")
      .withColumnRenamed("_13","f_play_time")
      .withColumnRenamed("_14","f_user_count")
      .withColumnRenamed("_15","f_date_time")

    val lookbackTypeDf = lookbackJoinDf.groupBy("f_date_time","f_terminal","f_province_id","f_province_name","f_city_id","f_city_name","f_region_id"
      ,"f_region_name","f_type_id")
      .agg("f_user_count" -> "sum")
      .withColumnRenamed("sum(f_user_count)","f_user_count")
      .selectExpr("f_date_time","f_terminal","f_province_id","f_province_name","f_city_id","f_city_name","f_region_id"
        ,"f_region_name","f_type_id","f_user_count")
    try{
      /*println("saveDemandDf:")
      lookbackJoinDf.show(100)
      println("lookbackTypeDf:")
      lookbackTypeDf.show(100)*/
      println("lookbackJoinDf记录数："+lookbackJoinDf.count())
      println("lookbackTypeDf记录数："+lookbackTypeDf.count())
      DBUtils.saveToMysql(lookbackJoinDf, Tables.T_PROGRAM_LOOKBACK_REALTIME, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD) //保存点播节目统计信息
      DBUtils.saveToMysql(lookbackTypeDf, Tables.T_PROGRAM_LOOKBACK_TYPE_REALTIME, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD) //保存点播内容类型统计信息
      if(!lookbackJoinDf.rdd.isEmpty()){
        NodeTime.updateTimeNode(nodeTime,"lookback")
      }
      if(!lookbackTypeDf.rdd.isEmpty()){
        NodeTime.updateTimeNode(nodeTime,"lookbackContentType")
      }
      NodeTime.deleteHistRealtimeInfo(nodeTime,"lookback")
      NodeTime.deleteHistRealtimeInfo(nodeTime,"lookbackContentType")
    }catch {
      case e:Exception => {
        println("保存节目回看统计失败！节点：" + nodeTime)
        e.printStackTrace()
      }
    }
    nodeTimeBC.destroy()
    regionBC.destroy()
    lookbackMediaDf.unpersist()
    lookbackOnlineDf.unpersist()
    sqlContext.clearCache()
  }


  /**
    * 统计表中回看记录
    * @param sqlContext
    */
  def getLastLookbackDf(sqlContext:HiveContext,currNodeTime:String) ={
    val lastDemandNodeTime = NodeTime.getLastNodeTime(sqlContext,"demand")
    val lastNodeDay = DateUtils.dateStrToDateTime(lastDemandNodeTime,DateUtils.YYYY_MM_DD_HHMM).getDayOfMonth
    val currNodeDay = DateUtils.dateStrToDateTime(currNodeTime,DateUtils.YYYY_MM_DD_HHMM).getDayOfMonth
    val whereStr = if(currNodeDay == lastNodeDay) s"f_date_time = '${lastDemandNodeTime}'" else "1 = 0"
    val lastDemandSql =
      s"""
         |(select f_province_id as l_province_id ,f_province_name as l_province_name,f_city_id as l_city_id,f_city_name as l_city_name,
         |f_region_id as l_region_id,f_region_name as l_region_name,f_terminal as l_terminal,f_program_id as l_program_id,f_program_name as l_program_name,
         |f_type_id as l_type_id,f_play_count as l_play_count,f_user_count as l_user_count
         |from ${Tables.T_PROGRAM_DEMAND_REALTIME}
         |where ${whereStr}) as last_demand
      """.stripMargin
    //    println(lastDemandSql)
    val lastDemand  = DBUtils.loadMysql(sqlContext,lastDemandSql, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)
    lastDemand
  }
  /**
    * 当前runlog回看用户列表
    * @param sqlContext
    * @return
    */
  def getLookbackFromRunLogDf(sqlContext:HiveContext) ={
    val runlogLookback =
      s"""
         |(select f_user_id,f_terminal,f_region_id,f_program_id,f_update_time,f_play_count
         |from ${Tables.T_RUNLOG_USER_STATUS_REALTIME}
         |where f_service_type = '${GatherType.LOOK_BACK_NEW}' and f_online_status = 1) as lookback_online
      """.stripMargin
//    println("runlogLookback-sql:" )
//    println(runlogLookback)
    DBUtils.loadDataFromPhoenix2(sqlContext,runlogLookback)
  }

  /**
    * 获取homed回看媒资信息
    * @param sqlContext
    * @return
    */
  def getLookbackMediaDf(sqlContext:HiveContext) ={
    val sql =
      """(
       |SELECT a.event_id as f_event_id ,a.event_name as f_event_name ,
       |time_to_sec(a.duration) as f_duration,
       |b.chinese_name as f_channel_name,b.channel_id as f_channel_id,
       |a.f_series_id,
       |a.start_time as f_start_time
       |FROM homed_eit_schedule_history a
       |JOIN channel_store b ON a.homed_service_id = b.channel_id
       ) as vod_info
      """.stripMargin
    val backMeiZiInfoDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val sql1 =
      """
        |(select series_id,series_name,series_num as f_series_num,f_content_type from event_series) as series_info
      """.stripMargin
    val seriesDF = DBUtils.loadMysql(sqlContext, sql1, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    backMeiZiInfoDF.registerTempTable("backMeiZiInfoDF")
    seriesDF.registerTempTable("seriesDF")
    val lookBackDF = sqlContext.sql(
      """
        |select cast(a.f_event_id as string) as f_event_id ,a.f_event_name,a.f_channel_name,cast(a.f_channel_id as string) as f_channel_id,cast(b.f_content_type as string) as f_content_type
        |from backMeiZiInfoDF a
        |join seriesDF b
        |on a.f_series_id = b.series_id
        |where f_series_id >0
      """.stripMargin)
    lookBackDF
  }
}
