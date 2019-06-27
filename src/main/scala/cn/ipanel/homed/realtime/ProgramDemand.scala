package cn.ipanel.homed.realtime

import cn.ipanel.common.{DBProperties, GatherType, SparkSession, Tables}
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

object ProgramDemand {
  def main(args: Array[String]): Unit = {
    println("实时节目点播统计开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    val sparkSession: SparkSession = SparkSession("ChannelLiveRealtime")
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
    val nodeTime = DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMM)
    programDemandCount(sparkSession,hiveContext,duration,partAmt,nodeTime)
    println("实时节目点播统计结束："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    sparkSession.stop()
  }

  /**
    * 统计点播节目
    * @param sparkSession
    * @param sqlContext
    * @param duration
    * @param partAmt
    * @param nodeTime
    */
  def programDemandCount(sparkSession: SparkSession,sqlContext:HiveContext,duration:Int,partAmt:Int,nodeTime:String): Unit ={
    import sqlContext.implicits._
    val sc = sparkSession.sparkContext
    val nodeTimeBC = sc.broadcast(nodeTime)
    //区域信息
    val regionBC = sc.broadcast(UserRegion.loadRegionInfoMap(sparkSession))
    //点播节目列表信息
    val demandVideoListDf = getDemandVideoDf(sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER) //f_video_id,a.f_video_name,b.f_content_type
    //上批统计数据
    val lastDemandDf = getLastDemandDf(sqlContext,nodeTime).persist(StorageLevel.MEMORY_AND_DISK_SER)
      //l_province_id ,l_province_name,l_city_id,l_city_name,l_region_id,f_region_name,l_terminal,l_program_id,l_program_name,l_type_id,f_play_count,l_user_count
    //当前点播在线用户
    val demandCurrDf = getDemandFromRunLogDf(sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val demandGroupDf = demandCurrDf
        //f_user_id,f_terminal,f_region_id,f_program_id,f_update_time,f_play_count
        .groupBy("f_terminal","f_region_id","f_program_id")
        .agg("f_play_count" -> "sum","f_user_id" -> "count")
        .withColumnRenamed("sum(f_play_count)","f_play_count")
        .withColumnRenamed("count(f_user_id)","f_user_count")
      val demandJoinDf = demandGroupDf.join(demandVideoListDf,demandGroupDf("f_program_id")===demandVideoListDf("f_video_id"),"left_outer")
        .map(x => {
          val terminal = x.getAs[Int]("f_terminal")
          val regionId = x.getAs[String]("f_region_id")
          val (provinceId,provinceName,cityId,cityName,regionName) = regionBC.value.getOrElse(regionId,("unknown","unknown","unknown","unknown","unknown"))
          val programId = x.getAs[String]("f_program_id")
          val programName = x.getAs[String]("f_video_name")
          val seriesId = x.getAs[String]("f_series_id")
          val programType = x.getAs[Int]("f_content_type").toString
          val userCount = x.getAs[Long]("f_user_count").toInt
          val playCount = x.getAs[Long]("f_play_count")
          (terminal,provinceId,provinceName,cityId,cityName,regionId,regionName,programId,programName,seriesId,programType,userCount,playCount)
        }).toDF()
        .withColumnRenamed("_1","f_terminal")
        .withColumnRenamed("_2","f_province_id")
        .withColumnRenamed("_3","f_province_name")
        .withColumnRenamed("_4","f_city_id")
        .withColumnRenamed("_5","f_city_name")
        .withColumnRenamed("_6","f_region_id")
        .withColumnRenamed("_7","f_region_name")
        .withColumnRenamed("_8","f_program_id")
        .withColumnRenamed("_9","f_program_name")
        .withColumnRenamed("_10","f_series_id")
        .withColumnRenamed("_11","f_type_id")
        .withColumnRenamed("_12","f_user_count")
        .withColumnRenamed("_13","f_play_count")

      val saveDemandDf = demandJoinDf.join(lastDemandDf,demandJoinDf("f_region_id")===lastDemandDf("l_region_id")
          && demandJoinDf("f_terminal")===lastDemandDf("l_terminal")
          && demandJoinDf("f_program_id")===lastDemandDf("l_program_id")
          && demandJoinDf("f_type_id")===lastDemandDf("l_type_id"),"outer")
        .selectExpr(s"'${nodeTimeBC.value}' as f_date_time","nvl(f_province_id,l_province_id) as f_province_id","nvl(f_province_name,l_province_name) as f_province_name"
          ,"nvl(f_city_id,l_city_id) as f_city_id","nvl(f_city_name,l_city_name) as f_city_name","nvl(f_region_id,l_region_id) as f_region_id"
          ,"nvl(f_region_name,l_region_name) as f_region_name","nvl(f_terminal,l_terminal) as f_terminal","nvl(f_program_id,l_program_id) as f_program_id"
          ,"nvl(f_program_name,l_program_name) as f_program_name ","nvl(f_series_id,l_series_id) as f_series_id","nvl(f_type_id,l_type_id) as f_type_id"
          ,"nvl(f_play_count,0) + nvl(l_play_count,0) as f_play_count","nvl(f_user_count,0) as f_user_count")
    val demandHisDf = demandJoinDf.selectExpr(s"'${nodeTimeBC.value}' as f_date_time","f_region_id","f_terminal","f_program_id"
      ,"f_program_name","f_type_id","f_user_count","f_play_count")
    val demandTypeDf = demandJoinDf.groupBy("f_terminal","f_province_id","f_province_name","f_city_id","f_city_name","f_region_id","f_region_name","f_type_id")
      .agg("f_user_count" -> "sum").withColumnRenamed("sum(f_user_count)","f_user_count")
      .selectExpr(s"'${nodeTimeBC.value}' as f_date_time","f_terminal","f_province_id","f_province_name","f_city_id","f_city_name","f_region_id","f_region_name","f_type_id","f_user_count")
    try{
      /*println("saveDemandDf:")
      saveDemandDf.show(100)*/
      println("saveDemandDf记录数："+saveDemandDf.count())
      println("demandTypeDf记录数："+demandTypeDf.count())
      DBUtils.saveToMysql(saveDemandDf, Tables.T_PROGRAM_DEMAND_REALTIME, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD) //保存点播节目统计信息
      DBUtils.saveToMysql(demandTypeDf, Tables.T_DEMAND_CONTENT_TYPE_REALTIME, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD) //保存点播内容类型统计信息
      if(!saveDemandDf.rdd.isEmpty()){
        NodeTime.updateTimeNode(nodeTime,"demand")
      }
      if(!demandTypeDf.rdd.isEmpty()){
        NodeTime.updateTimeNode(nodeTime,"demandContentType")
      }
      NodeTime.deleteHistRealtimeInfo(nodeTime,"demand")
      NodeTime.deleteHistRealtimeInfo(nodeTime,"demandContentType")
    }catch {
      case e:Exception => {
        println("保存点播节目统计失败！节点：" + nodeTime)
        e.printStackTrace()
      }
    }
    try{
      DBUtils.saveDataFrameToPhoenixNew(demandHisDf,Tables.T_PROGRAM_DEMAND_HIS_REALTIME)
    }catch{
      case e:Exception => {
        println("历史点播节目保存失败！节点：" + nodeTime)
        e.printStackTrace()
      }
    }
    nodeTimeBC.destroy()
    regionBC.destroy()
    demandVideoListDf.unpersist()
    lastDemandDf.unpersist()
    demandCurrDf.unpersist()
    sqlContext.clearCache()
  }


  /**
    * 统计表中点播记录
    * @param sqlContext
    */
  def getLastDemandDf(sqlContext:HiveContext,currNodeTime:String) ={
    val lastDemandNodeTime = NodeTime.getLastNodeTime(sqlContext,"demand")
    val lastNodeDay = DateUtils.dateStrToDateTime(lastDemandNodeTime,DateUtils.YYYY_MM_DD_HHMM).getDayOfMonth
    val currNodeDay = DateUtils.dateStrToDateTime(currNodeTime,DateUtils.YYYY_MM_DD_HHMM).getDayOfMonth
    val whereStr = if(currNodeDay == lastNodeDay) s"f_date_time = '${lastDemandNodeTime}'" else "1 = 0"
    val lastDemandSql =
      s"""
        |(select f_province_id as l_province_id ,f_province_name as l_province_name,f_city_id as l_city_id,f_city_name as l_city_name,
        |f_region_id as l_region_id,f_region_name as l_region_name,f_terminal as l_terminal,f_program_id as l_program_id,f_program_name as l_program_name,
        |f_series_id as l_series_id,f_type_id as l_type_id,f_play_count as l_play_count,f_user_count as l_user_count
        |from ${Tables.T_PROGRAM_DEMAND_REALTIME}
        |where ${whereStr}) as last_demand
      """.stripMargin
//    println(lastDemandSql)
    val lastDemand  = DBUtils.loadMysql(sqlContext,lastDemandSql, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)
    lastDemand
  }
  /**
    * 当前runlog点播用户列表
    * @param sqlContext
    * @return
    */
  def getDemandFromRunLogDf(sqlContext:HiveContext) ={
    val runlogDemand =
      s"""
        |(select f_user_id,f_terminal,f_region_id,f_program_id,f_update_time,f_play_count
        |from ${Tables.T_RUNLOG_USER_STATUS_REALTIME}
        |where f_service_type = '${GatherType.DEMAND_NEW}' and f_online_status = 1) as demand_online
      """.stripMargin
    DBUtils.loadDataFromPhoenix2(sqlContext,runlogDemand)
  }
  /**
    * 获取节目列表
    * @param sqlContext
    */
  def getDemandVideoDf(sqlContext:HiveContext) ={
    val videoSql =
    """
      |(SELECT DISTINCT(a.video_id) AS f_video_id,a.video_name AS f_video_name,b.f_content_type,cast(a.f_series_id as char) as f_series_id
      |FROM video_info a JOIN video_series b
      |ON a.f_series_id = b.series_id) as video
    """.stripMargin
    DBUtils.loadMysql(sqlContext, videoSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
  }
}
