package cn.ipanel.homed.repots

import cn.ipanel.common.{CluserProperties, DBProperties, GatherType, SparkSession}
import cn.ipanel.homed.repots.DemandReport.{getDemandBehaviour, getMultiScreen}
import cn.ipanel.homed.repots.MultiScreen.writeToHive
import cn.ipanel.homed.repots.OnlineRate.{getFinalUser, getHalfHourPierod, getTimeSecond}
import cn.ipanel.utils.{DBUtils, DateUtils, RegionUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

case class LookBackRdd(f_user_id: String, f_region_id: Int, f_hour: Int, f_timerange: String, f_device_id: String, f_terminal: Int, f_play_type: String, f_event_id: String, f_start_time: String, f_play_time: Long, f_play_time1: Long, videoPlay: Int, f_column_id: String)

/**
  * 回看报表基础数据
  */
object Lookback {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("Lookback")
    val sc = sparkSession.sparkContext
    val hiveContext = sparkSession.sqlContext
    val regionCode = RegionUtils.getRootRegion
    val regionInfo = getRegionInfo(hiveContext, regionCode)
    val time = args(0)
    val partnum = args(1)
    val month = time.substring(0, 6)
    val year = time.substring(0, 4)
    val basic = getLookbackBasicInfo(time, hiveContext, regionInfo).cache()
    val backMeizi = sc.broadcast(getDtvsLookback(hiveContext))
    val userBehaviour = getDemandBehaviour(time, hiveContext)
    val multiScreen = getMultiScreen(hiveContext, time)
    val user_df_basic1 = getLookBasicInfo(time, hiveContext, backMeizi.value, basic, userBehaviour, multiScreen)
    basic.unpersist()
    user_df_basic1.repartition(partnum.toInt).registerTempTable("user_df_basic1")
    getLookBackUser(hiveContext: HiveContext)
    println("回看半小时人数统计结束")
    //按天 基础数据
    getBasicLookBasic(time, hiveContext, month, year)
    println("回看按天基础数据统计结束")
    sc.stop()
  }


  def getRegionInfo(hiveContext: HiveContext, regionCode: String) = {
    val sql1 =
      s"""
         |(SELECT province_id as f_province_id,province_name as f_province_name
         |from province
          ) as aa
     """.stripMargin
    val provinceInfoDF = DBUtils.loadMysql(hiveContext, sql1, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    var sql2 = ""
    if (CluserProperties.REGION_VERSION.toInt.equals(0)) {
      sql2 =
        s"""
           |(SELECT province_id,city_id as f_city_id,city_name as f_city_name
           |from city
          ) as aa
     """.stripMargin
    } else {
      sql2 =
        s"""
           |(SELECT province_id,city_id as f_city_id,city_name as f_city_name
           |from city where province_id='$regionCode' or city_id='$regionCode'
          ) as aa
     """.stripMargin
    }
    val cityInfoDF2 = DBUtils.loadMysql(hiveContext, sql2, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val sql3 =
      s"""
         |(SELECT city_id,area_id as f_region_id,area_name as f_region_name
         |from area
          ) as aa
     """.stripMargin
    val areaInfoDF3 = DBUtils.loadMysql(hiveContext, sql3, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    //addressInfoDF.registerTempTable("address")
    provinceInfoDF.registerTempTable("province")
    cityInfoDF2.registerTempTable("city")
    areaInfoDF3.registerTempTable("region")
    val basic_df1 = hiveContext.sql(
      s"""
         |select a.f_province_id,a.f_province_name,
         |b.f_city_id,b.f_city_name,c.f_region_id,c.f_region_name
         |from province a
         |join city b on a.f_province_id = b.province_id
         |join region c on b.f_city_id = c.city_id
          """.stripMargin)
    val basic_df = basic_df1.distinct()
    basic_df
  }

  //从数据库取出回看节目相关信息
  def getDtvsLookback(hiveContext: HiveContext): DataFrame = {
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
    val backMeiZiInfoDF = DBUtils.loadMysql(hiveContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val sql1 =
      """
        |(select series_id,series_name,series_num as f_series_num,f_content_type from event_series) as series_info
      """.stripMargin
    val seriesDF = DBUtils.loadMysql(hiveContext, sql1, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    backMeiZiInfoDF.registerTempTable("backMeiZiInfoDF")
    seriesDF.registerTempTable("seriesDF")
    val lookBackDF = hiveContext.sql(
      """
        |select a.f_event_id ,a.f_event_name ,
        |a.f_duration,
        |a.f_channel_name,a.f_channel_id,
        |b.f_content_type,a.f_series_id,
        |b.series_name as f_series_name ,b.f_series_num,
        |a.f_start_time,b.f_content_type
        |from backMeiZiInfoDF a
        |join seriesDF b
        |on a.f_series_id = b.series_id
        |where f_series_id >0
      """.stripMargin)
    val map = Map("f_series_name" -> "")
    val lookBackDF1 = lookBackDF.na.fill(map)
    lookBackDF1
  }

  def getBasicLookBasic(time: String, hiveContext: HiveContext, month: String, year: String) = {
    val sql1 =
      s"""
         |insert overwrite table t_lookback_event_basic partition(day='$time',month='$month',year='$year')
         |select
         |max(a.f_start_time) as f_start_time,
         |a.f_province_id,a.f_province_name,a.f_city_id,a.f_city_name,
         |a.f_region_id,a.f_region_name,
         |a.f_terminal,a.f_event_id,a.f_event_name,
         |a.f_user_id,a.f_channel_id,a.f_channel_name,
         |a.f_content_type,sum(a.videoPlay) as videoPlay,
         |sum(a.f_play_time) as f_play_time,a.f_duration,
         |a.f_series_num,a.f_series_id,a.f_series_name,
         |0 as f_screen,
         |0 as f_share
         |from user_df_basic1 a
         |group by
         |a.f_user_id,
         |a.f_event_id,a.f_event_name,
         |a.f_province_id,a.f_province_name,a.f_city_id,a.f_city_name,
         |a.f_region_id,a.f_region_name,a.f_duration,
         |a.f_terminal,a.f_channel_id,a.f_channel_name,
         |a.f_content_type,
         |a.f_series_num,a.f_series_id,a.f_series_name
         |""".stripMargin
    writeToHive(sql1, hiveContext, "t_lookback_event_basic", time)
  }


  def getLookBackUser(hiveContext: HiveContext) = {
    val finalLookBack = hiveContext.sql(
      """select f_date,
        |if((f_hour>9),f_hour,concat('0',cast(f_hour as STRING))) as f_hour,
        |f_timerange,f_terminal,
        |f_province_id,f_province_name,f_city_id,f_city_name,
        |f_region_id,f_region_name,
        |cast(f_content_type as STRING) as f_content_type,
        |'' as f_user_ids,
        |count(distinct(f_user_id)) as f_count
        | from user_df_basic1
        | where f_content_type is not null
        | group by
        | f_date,f_hour,f_timerange,f_province_name,f_province_id,f_city_name,f_city_id,
        |f_region_name,f_region_id,f_content_type,
        |f_terminal
      """.stripMargin
    )
    DBUtils.saveDataFrameToPhoenixNew(finalLookBack, cn.ipanel.common.Tables.t_lookback_online_report)
  }


  def getLookBackReport(date: String, hiveContext: HiveContext) = {
    val lookBackFinal = hiveContext.sql(
      s"""
         |select '$date' as f_date,
         |f_start_time,cast(f_hour as string) as f_hour,f_timerange,
         |f_province_id,
         |f_province_name,f_city_id,f_city_name,
         |f_region_id,f_region_name,
         |f_terminal,f_event_id,f_event_name,
         |'' as f_user_ids,count(distinct(f_user_id)) as f_user_count,
         |f_channel_id,f_channel_name,
         | f_content_type,
         | sum(videoPlay) as f_click_num,
         |sum(f_play_time) as f_video_time_sum,
         |f_duration,f_series_num,
         | f_series_id,f_series_name,
         | sum(f_screen) as f_screen,sum(f_share) as f_share
         |from user_df_basic1
         |group by
         |f_start_time,f_hour,f_timerange,
         |f_province_id,
         |f_province_name,f_city_id,f_city_name,
         |f_region_id,f_region_name,
         |f_terminal,f_event_id,f_event_name,f_channel_id,f_channel_name,
         | f_content_type,f_duration,f_series_num,
         | f_series_id,f_series_name
      """.stripMargin)
    // DBUtils.saveToHomedData_2(lookBackFinal, cn.ipanel.common.Tables.t_look_back_statistics_report)
    DBUtils.saveDataFrameToPhoenixNew(lookBackFinal, cn.ipanel.common.Tables.t_look_back_statistics_report)
  }

  def getLookbackBasicInfo(time: String, hiveContext: HiveContext, regionInfo: DataFrame) = {
    import hiveContext.implicits._
    hiveContext.sql("use bigdata")
    val user_basic = hiveContext.sql(
      s"""
         |select userId as f_user_id,deviceId as f_device_id,
         |regionId as f_region_id,
         |hour(startTime) as f_hour,
         |(case when minute(startTime)>30 then '60' else '30' end) as f_timerange,
         |deviceType as f_terminal,playType as f_play_type,
         |serviceId as f_event_id,endTime,
         |startTime as f_start_time,playTime as f_play_time,
         |if ((ext['column_id'] is null),'0',ext['column_id'])  as f_column_id
         |from orc_video_play
         |where day='$time' and playType = 'lookback'
            """.stripMargin)
    val user_timerange = user_basic.map(x => {
      val f_user_id = x.getAs[String]("f_user_id")
      val f_hour = x.getAs[Int]("f_hour")
      val f_timerange = x.getAs[String]("f_timerange")
      val f_region_id = x.getAs[String]("f_region_id").toInt
      val f_device_id = x.getAs[Long]("f_device_id").toString
      val f_terminal = x.getAs[String]("f_terminal").toInt
      val f_play_type = x.getAs[String]("f_play_type")
      val f_program_id = x.getAs[Long]("f_event_id").toString
      val f_start_time = x.getAs[String]("f_start_time")
      val f_play_time = x.getAs[Long]("f_play_time")
      val f_column_id = x.getAs[String]("f_column_id")
      val videoPlay = 1
      UserRdd(f_user_id, f_region_id, f_hour, f_timerange, f_device_id, f_terminal, f_play_type, f_program_id, f_start_time, f_play_time, f_play_time, videoPlay, f_column_id)
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
        listBuffer += ((x.f_user_id, x.f_device_id, x.f_terminal, x.f_play_type, x.f_start_time, hour1, timerange1, playTime1, x.f_play_time, x.f_program_id, x.f_region_id, x.videoPlay, x.f_column_id))
      } else {
        val loop = new Breaks;
        loop.breakable {
          playTime = 1800 - initialTime
          listBuffer += ((x.f_user_id, x.f_device_id, x.f_terminal, x.f_play_type, x.f_start_time, hour1, timerange1, playTime, x.f_play_time, x.f_program_id, x.f_region_id, x.videoPlay, x.f_column_id))
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
            listBuffer += ((x.f_user_id, x.f_device_id, x.f_terminal, x.f_play_type, x.f_start_time, hour1, timerange1, playTime, x.f_play_time, x.f_program_id, x.f_region_id, 0, x.f_column_id))
          }
        }
      }
      listBuffer.toList
    })
    val user_timerange2 = user_timerange.flatMap(x => x).map(x => {
      LookBackRdd(x._1, x._11, x._6, x._7, x._2, x._3, x._4, x._10, x._5, x._8, x._9, x._12, x._13)
    }).toDF()
    //关联区域
    val user_df_basic1 = getFinalUser(user_timerange2, hiveContext, regionInfo)
    user_df_basic1
  }

  //节目名,节目时间,所属频道,观看总时长,点击次数,节目观看人数
  def getLookBasicInfo(time: String, hiveContext: HiveContext, backMeizi: DataFrame, basic: DataFrame, userBehaviour: DataFrame, multiScreen: DataFrame) = {
    basic.registerTempTable("user_df_basic1")
    val basic_df1 = hiveContext.sql(
      s"""
         |select '$time' as f_date,
         |f_hour,f_timerange,f_terminal,
         |f_event_id,
         |f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name,
         |f_user_id,sum(f_play_time) as f_play_time,
         |sum(videoPlay) as videoPlay
         |from user_df_basic1
         |where f_play_type='${GatherType.LOOK_BACK_NEW}' and f_city_name is not null
         |group by f_hour,f_timerange,f_terminal,f_event_id,f_province_id,f_province_name,
         |f_city_id,f_city_name,
         |f_region_id,f_region_name,
         |f_user_id
      """.stripMargin)
    val lookBackInfo = backMeizi.join(basic_df1, Seq("f_event_id")) //.registerTempTable("lookback")
    //    userBehaviour.registerTempTable("userBehaviour")
    //    multiScreen.registerTempTable("multiScreen")
    //    val lookBackFinal = hiveContext.sql(
    //      """
    //        |select a.f_date,
    //        |a.f_start_time,a.f_hour,
    //        |a.f_timerange,a.f_province_id,
    //        |a.f_province_name,a.f_city_id,a.f_city_name,
    //        |a.f_region_id,a.f_region_name,
    //        |a.f_terminal,
    //        |cast(a.f_event_id as string) as f_event_id,
    //        |a.f_event_name,
    //        |a.f_user_id,a.f_channel_id,a.f_channel_name,
    //        |a.f_content_type,a.videoPlay,
    //        |a.f_play_time,
    //        |a.f_duration,a.f_series_num,
    //        |a.f_series_id,a.f_series_name,
    //        |if((c.f_screen is null),0,c.f_screen) as f_screen,
    //        |if((b.f_share is null),0,c.f_screen) as f_share
    //        |from lookback a left join
    //        |userBehaviour b
    //        |on (a.f_hour = b.f_hour and a.f_timerange= b.f_timerange and
    //        | a.f_event_id = b.f_video_id and a.f_region_id = b.f_region_id
    //        | and a.f_terminal= b.f_terminal and a.f_user_id = b.f_user_id)
    //        | left join
    //        | multiScreen c
    //        | on (a.f_hour = c.f_hour and a.f_timerange= c.f_timerange and
    //        | a.f_event_id = c.f_program_id and c.f_region_id = c.f_region_id
    //        | and a.f_terminal= c.f_terminal and a.f_user_id =c.f_user_id)
    //      """.stripMargin)
    lookBackInfo
  }


}
