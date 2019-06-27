package cn.ipanel.homed.repots

import java.sql.DriverManager

import cn.ipanel.common.{DBProperties, SparkSession}
import cn.ipanel.homed.repots.DemandWatch.{getColumnIdName, getColumnRoot}
import cn.ipanel.homed.repots.MultiScreen.writeToHive
import cn.ipanel.homed.repots.OnlineRate.{getHalfHourPierod, getTimeSecond}
import cn.ipanel.utils.{DBUtils, DateUtils, RegionUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

case class UserDemand(f_user_id: String, f_region_id: Int, f_hour: Int, f_timerange: String, f_device_id: String, f_terminal: Int, f_play_type: String, f_video_id: String, f_start_time: String, f_play_time: Long, f_play_time1: Long, videoPlay: Int, f_column_id: String)

/**
  * 点播报表基础数据
  */
object DemandReport {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("DemandReport")
    val sc = sparkSession.sparkContext
    val hiveContext = sparkSession.sqlContext
    val regionCode = RegionUtils.getRootRegion
    val regionInfo = Lookback.getRegionInfo(hiveContext, regionCode)
    val time = args(0)
    val partnum = args(1)
    val month = time.substring(0, 6)
    //月
    val year = time.substring(0, 4)
    //点播
    val demandMeiZiInfoDF = sc.broadcast(getDemandMeiZi(hiveContext))
    // 栏目
    val columnMap = getColumnInfo(hiveContext)
    val columndf = getColumn(hiveContext: HiveContext)
    // 得到基本数据信息  以半小时为单位 读取日志
    val user_df_basic1 = getDemandBasicInfo(time, hiveContext, regionInfo)
    val userBehaviour = getDemandBehaviour(time, hiveContext)
    val multiScreen = getMultiScreen(hiveContext, time)
    val basicDemand = getBasicDF(time, hiveContext, demandMeiZiInfoDF.value, user_df_basic1, columnMap, userBehaviour, multiScreen)
    user_df_basic1.unpersist()
    basicDemand.repartition(partnum.toInt).registerTempTable("basicDemand")
    //得到半小时栏目时长
    getColumnDetailsByHalfhour(time, hiveContext)
    //按内容类型分
    getDemandUser(basicDemand, hiveContext)
    println("点播半小时按内容类型分人数统计结束")
    //按天基础数据
    getBasicDemand(time, hiveContext, month, year, userBehaviour, multiScreen)
    println("点播按天基础数据统计结束")
    sc.stop()
  }

  def getColumnDetailsByHalfhour(time: String, hiveContext: HiveContext) = {
    val df = hiveContext.sql(
      s"""
         |select  '$time' as f_date,a.f_hour,
         |cast(a.f_timerange as int) as f_timerange,
         |a.f_terminal,
         |cast(a.f_region_id as string) as f_region_id,a.f_region_name,
         |cast(a.f_city_id as string) as f_city_id,
         |a.f_city_name,
         |cast(a.f_province_id as string) as f_province_id,
         |a.f_province_name,a.f_column_id,
         |a.f_column_name,a.f_parent_column_id,a.f_parent_parent_column_id,
         |a.f_parent_column_name,a.f_parent_parent_column_name,
         |cast(a.f_series_id as string) as f_video_id,a.series_name as f_video_name,
         |sum(a.videoPlay) as f_play_count,sum(a.f_play_time) as f_play_time
         |from basicDemand a
         |group by
         |a.f_terminal,a.f_region_id,a.f_region_name,
         |a.f_city_id,a.f_city_name,
         |a.f_province_id,a.f_province_name,a.f_column_id,
         |a.f_column_name,a.f_parent_column_id,a.f_parent_parent_column_id,
         |a.f_parent_column_name,a.f_parent_parent_column_name,
         |a.f_series_id,a.series_name,a.f_hour,a.f_timerange
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(df, cn.ipanel.common.Tables.t_demand_column_by_halfhour)
  }

  def getDemandMeiZi(hiveContext: HiveContext): DataFrame = {

    val sql =
      """(
           |SELECT distinct(a.video_id) as f_video_id,a.video_name as f_video_name,
           |b.f_content_type,b.f_sub_type,
           |a.f_cp_id,a.f_copyright,
           |time_to_sec(a.video_time) as duration,a.f_series_id,b.series_name,
           | b.series_num,a.f_upload_complete_time
           |FROM video_info a join video_series b
           |on a.f_series_id = b.series_id
            ) as vod_info
      """.stripMargin
    val vodMeiZiInfoDF = DBUtils.loadMysql(hiveContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    vodMeiZiInfoDF
  }

  //栏目
  def getColumn(hiveContext: HiveContext): DataFrame = {
    val sql =
      """
        |(SELECT distinct(f_column_id),f_column_name
        |FROM	`t_column_info` WHERE f_column_status = 1) as aa
      """.stripMargin
    val columnDF = DBUtils.loadMysql(hiveContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    columnDF
  }

  /**
    * 从数据库栏目所需信息
    */
  def getColumnInfo(hiveContext: HiveContext) = {
    val map = new java.util.HashMap[Long, String]()
    val connection = DriverManager.getConnection(DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val statement = connection.createStatement
    val queryRet = statement.executeQuery(
      "SELECT f_column_id,f_parent_id,f_column_name FROM	`t_column_info` WHERE f_column_status = 1")
    while (queryRet.next) {
      val f_column_id = queryRet.getLong("f_column_id")
      val f_column_name = queryRet.getString("f_column_name")
      val f_parent_id = queryRet.getLong("f_parent_id")
      map.put(f_column_id, f_parent_id + "," + f_column_name)
    }
    connection.close
    map
  }

  def getBasicDemand(time: String, hiveContext: HiveContext, month: String, year: String, userBehaviour: DataFrame, multiScreen: DataFrame) = {
    val sql1 =
      s"""
         |insert overwrite table t_demand_video_basic partition(day='$time',month='$month',year='$year')
         |select
         |a.f_user_id,
         |a.f_video_id,
         |regexp_replace(a.f_video_name, ',', '，') as f_video_name,
         |a.f_province_id,a.f_province_name,a.f_city_id,a.f_city_name,
         |a.f_region_id,a.f_region_name,
         |a.f_terminal,
         |a.f_content_type,
         |a.f_cp_id,a.f_copyright,
         |f_column_level,f_column_id,f_column_name,
         |f_parent_column_id,f_parent_column_name,
         |f_parent_parent_column_id,f_parent_parent_column_name,
         |sum(a.f_play_time) as f_play_time,max(duration) as f_duration,sum(a.videoPlay) as videoPlay,
         |0 as f_screen,
         |0 as f_share,
         |a.series_num as f_series_num,a.f_series_id,
         |regexp_replace(a.series_name, ',', '，') as f_series_name,
         |f_device_id
         |from basicDemand a
         |group by
         |a.f_user_id,
         |a.f_video_id,a.f_video_name,
         |a.f_province_id,a.f_province_name,a.f_city_id,a.f_city_name,
         |a.f_region_id,a.f_region_name,
         |a.f_terminal,
         |a.f_content_type,
         |a.f_cp_id,a.f_copyright,
         |f_column_level,f_column_id,f_column_name,
         |f_parent_column_id,f_parent_column_name,
         |f_parent_parent_column_id,f_parent_parent_column_name,
         |a.series_num,a.f_series_id,a.series_name,
         |f_device_id
         """.stripMargin
    writeToHive(sql1, hiveContext, "t_demand_video_basic", time)
  }

  def getDemandUser(basicDemand: DataFrame, hiveContext: HiveContext) = {
    val demandWatchdf = hiveContext.sql(
      s"""
         |select f_date,
         |cast(f_hour as STRING) as f_hour,
         |f_timerange,f_terminal,
         |f_province_id,f_province_name,f_city_id,f_city_name,
         |f_region_id,f_region_name,
         |cast(f_content_type as STRING) as f_content_type,
         |'' as  f_user_ids,
         |count(distinct(f_user_id)) as f_count,
         |sum(f_play_time) as f_play_time
         |from basicDemand
         |where f_content_type is not null
         |group by f_date,f_hour,f_timerange,
         |f_terminal,f_content_type,f_province_name,
         |f_province_id,f_city_name,f_city_id,
         |f_region_name,f_region_id
             """.stripMargin)
    //ALTER TABLE t_demand_user_by_halfhour ADD f_play_time bigint
    DBUtils.saveDataFrameToPhoenixNew(demandWatchdf, cn.ipanel.common.Tables.t_demand_user_by_halfhour)
  }

  def getDemandWatchBasic(date: String, hiveContext: HiveContext) = {
    val demandWatchFinal = hiveContext.sql(
      s"""
         |select '$date' as  f_date,
         |cast(a.f_hour as STRING) as f_hour,
         |a.f_timerange,
         |'' as f_user_ids,
         |count(distinct(a.f_user_id)) as f_user_count,
         |a.f_video_id,a.f_video_name,
         |a.f_province_id,a.f_province_name,a.f_city_id,a.f_city_name,
         |a.f_region_id,a.f_region_name,
         |a.f_terminal,a.f_content_type,
         |a.f_cp_id,a.f_copyright,f_column_level,f_column_id,f_column_name,
         |f_parent_column_id,f_parent_column_name,
         |f_parent_parent_column_id,f_parent_parent_column_name,
         |sum(a.f_play_time) as f_video_time_sum,
         |max(a.f_duration) as f_duration,sum(a.videoPlay) as f_click_num,
         |sum(a.f_screen) as f_screen,sum(a.f_share) as f_share,
         |a.f_series_num,a.f_series_id,a.f_series_name
         |from basicDemand a
         |group by
         |a.f_hour,a.f_timerange,a.f_video_name,a.f_video_id,a.f_terminal,a.f_content_type,a.f_cp_id,a.f_copyright,
         |a.f_province_name,a.f_city_name,a.f_province_id,a.f_city_id,a.f_region_id,
         |a.f_region_name,
         |a.f_series_num,a.f_column_level,a.f_column_id,a.f_column_name,
         |a.f_parent_column_id,a.f_parent_column_name,a.f_series_id,a.f_series_name,
         |a.f_parent_parent_column_id,a.f_parent_parent_column_name
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(demandWatchFinal, cn.ipanel.common.Tables.t_demand_watch_statistics_report)
  }

  def getMultiScreen(hiveContext: HiveContext, time: String) = {
    hiveContext.sql("use userprofile")
    hiveContext.sql(
      s"""
         |select *
         |from t_multi_screen where day = '$time'
      """.stripMargin)
  }

  def getDemandBehaviour(time: String, hiveContext: HiveContext) = {
    hiveContext.sql("use bigdata")
    val use_behavior_basic = hiveContext.sql(
      s"""
         |select
         |regionId as f_region_id,
         |hour(reportTime) as f_hour,
         |(case when minute(reportTime)>30 then 60 else 30 end) as f_timerange,
         |cast(deviceType AS INT) as f_terminal,
         |exts['ID'] as f_video_id,count(*) as f_share,userid as f_user_id
         |from orc_user_behavior
         |where day='$time'
         |and reportType = 'ShareSuccess' and reportType= 'demand'
         |and exts['ID'] !=""
         |group by regionId,deviceType,exts['ID'],userid,hour(reportTime),
         |(case when minute(reportTime)>30 then 60 else 30 end)
            """.stripMargin)
    use_behavior_basic
  }

  case class column1(f_column_level: Int, f_column_id: String, f_column_name: String, f_parent_column_id: Long, f_parent_column_name: String, f_parent_parent_column_id: Long, f_parent_parent_column_name: String)

  /**
    * 从hive得到点播数据信息(基础表)
    */
  def getBasicDF(time: String, hiveContext: HiveContext,
                 demandMeiZiInfoDF: DataFrame,
                 user_df_basic1: DataFrame, columnMap: java.util.HashMap[Long, String],
                 userBehaviour: DataFrame, multiScreen: DataFrame) = {
    import hiveContext.implicits._
    user_df_basic1.registerTempTable("user_df_basic1")
    val basic_df1 = hiveContext.sql(
      s"""
         |select '$time' as f_date,
         |f_hour,sum(videoPlay) as videoPlay,
         |f_timerange,f_user_id,
         |f_region_id,f_region_name,f_city_id,f_city_name,f_province_id,f_province_name,
         |f_terminal,
         | f_video_id,max(f_column_id) as f_column_id,
         |sum(f_play_time) as f_play_time,f_device_id
         |from user_df_basic1
         |group by f_hour,f_timerange,f_user_id,
         |f_region_id,f_region_name,f_city_id,f_city_name,f_province_id,f_province_name,f_terminal,f_video_id,f_device_id
             """.stripMargin)
    val columnInfo = basic_df1.map(x => {
      val f_column_id = x.getAs[String]("f_column_id")
      val f_column_info = getColumnIdName((getColumnRoot(try {
        x.getAs[String]("f_column_id").toInt
      } catch {
        case e: Exception => 0
      }, columnMap)))
      val level = f_column_info._1
      val columnId = f_column_info._2
      val columnName = f_column_info._3
      val columnParId = f_column_info._4
      val columnParName = f_column_info._5
      val columnParPId = f_column_info._6
      val columnParPName = f_column_info._7
      column1(level, f_column_id, columnName, columnParId, columnParName, columnParPId, columnParPName)
    }).toDF().distinct()
    val demandInfo = demandMeiZiInfoDF.join(basic_df1, Seq("f_video_id"))
    val demandInfo1 = columnInfo.join(demandInfo, Seq("f_column_id")) //.registerTempTable("demandInfo1")
    demandInfo1
  }


  def getDemandBasicInfo(time: String, hiveContext: HiveContext, regionInfo: DataFrame) = {
    import hiveContext.implicits._
    hiveContext.sql("use bigdata")
    val user_basic = hiveContext.sql(
      s"""
         |select userId as f_user_id,deviceId as f_device_id,
         |regionId as f_region_id,
         |hour(startTime) as f_hour,
         |(case when minute(startTime)>30 then '60' else '30' end) as f_timerange,
         |deviceType as f_terminal,playType as f_play_type,
         |serviceId as f_program_id,endTime,
         |startTime as f_start_time,playTime as f_play_time,
         |if ((ext['column_id'] is null),'0',ext['column_id'])  as f_column_id
         |from orc_video_play
         |where day='$time' and playType = 'demand'
            """.stripMargin)
    val user_timerange = user_basic.map(x => {
      val f_user_id = x.getAs[String]("f_user_id")
      val f_hour = x.getAs[Int]("f_hour")
      val f_timerange = x.getAs[String]("f_timerange")
      val f_region_id = x.getAs[String]("f_region_id").toInt
      val f_device_id = x.getAs[Long]("f_device_id").toString
      val f_terminal = x.getAs[String]("f_terminal").toInt
      val f_play_type = x.getAs[String]("f_play_type")
      val f_program_id = x.getAs[Long]("f_program_id").toString
      val f_start_time = x.getAs[String]("f_start_time")
      val f_play_time = x.getAs[Long]("f_play_time")
      val f_column_id = x.getAs[String]("f_column_id")
      val videoPlay = 1
      UserDemand(f_user_id, f_region_id, f_hour, f_timerange, f_device_id, f_terminal, f_play_type, f_program_id, f_start_time, f_play_time, f_play_time, videoPlay, f_column_id)
    }).map(x => {
      val listBuffer = new ListBuffer[(String, String, Int, String, String, Int, String, Long, Long, String, Int, Int, String)]()
      val initialTime = if (getTimeSecond(x.f_start_time) > 1800) {
        (getTimeSecond(x.f_start_time) - 1800)
      } else getTimeSecond(x.f_start_time)
      var playTime = 0L
      var time = initialTime
      var playTime1 = x.f_play_time
      var hour1 = x.f_hour
      var timerange1 = x.f_timerange
      if (time + playTime1 <= 1800) {
        listBuffer += ((x.f_user_id, x.f_device_id, x.f_terminal, x.f_play_type, x.f_start_time, hour1, timerange1, playTime1, x.f_play_time, x.f_video_id, x.f_region_id, x.videoPlay, x.f_column_id))
      } else {
        val loop = new Breaks;
        loop.breakable {
          playTime = 1800 - initialTime
          listBuffer += ((x.f_user_id, x.f_device_id, x.f_terminal, x.f_play_type, x.f_start_time, hour1, timerange1, playTime, x.f_play_time, x.f_video_id, x.f_region_id, x.videoPlay, x.f_column_id))
          var i = 0
          while (true) {
            //      for (j <- 0 to Comments.LOOP_Num) {
            time = 1800
            playTime1 = x.f_play_time - (1800 - initialTime) - 1800 * i
            i = i + 1
            if (playTime1 <= 0) {
              loop.break;
            }
            if (time >= playTime1) {
              playTime = playTime1
            } else {
              playTime = time
            }
            var timeRange = getHalfHourPierod(hour1, timerange1)
            hour1 = timeRange._1
            timerange1 = timeRange._2
            if (hour1 > 23) {
              loop.break;
            }
            listBuffer += ((x.f_user_id, x.f_device_id, x.f_terminal, x.f_play_type, x.f_start_time, hour1, timerange1, playTime, x.f_play_time, x.f_video_id, x.f_region_id, 0, x.f_column_id))
          }
        }
      }
      listBuffer.toList
    })
    val user_timerange2 = user_timerange.flatMap(x => x).map(x => {
      UserDemand(x._1, x._11, x._6, x._7, x._2, x._3, x._4, x._10, x._5, x._8, x._9, x._12, x._13)
    }).toDF()
    //关联区域
    val user_df_basic1 = getFinalUser(user_timerange2, hiveContext, regionInfo)
    user_df_basic1
  }

  //得到区域相关信息 转换终端
  def getFinalUser(user_df: DataFrame, hiveContext: HiveContext, regionInfo: DataFrame) = {
    val user_df2 = regionInfo.join(user_df, Seq("f_region_id"))
    user_df2
  }
}
