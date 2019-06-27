package cn.ipanel.homed.repots

import cn.ipanel.common.{CluserProperties, DBProperties, GatherType, SparkSession}
import cn.ipanel.utils.DBUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * 回看统计
  */
@deprecated
object LookBackReport {

  //节目名,节目时间,所属频道,观看总时长,点击次数,节目观看人数
  def getBasicDF(time: String, hiveContext: HiveContext, backMeizi: DataFrame, regionId: DataFrame, REGION: String, user_df_basic1: DataFrame) = {
    user_df_basic1.registerTempTable("user_df_basic1")
    val basic_df1 = hiveContext.sql(
      s"""
         |select '$time' as f_date,
         |f_hour,
         |f_timerange,
         |f_terminal,
         |f_program_id as f_event_id,
         |f_province_id,f_province_name,
         |f_city_id,f_city_name,
         |f_region_id,f_region_name,
         |f_user_id,f_play_time as f_watch_time,
         |videoPlay
         |from user_df_basic1
         |where f_play_type='${GatherType.LOOK_BACK_NEW}' and f_city_name is not null
      """.stripMargin)
    val lookBackInfo = backMeizi.join(basic_df1, Seq("f_event_id"), "right")
    lookBackInfo
  }

  //从数据库取出回看节目相关信息
  def getLookBackMeiZi(hiveContext: HiveContext): DataFrame = {
    val sql =
      """(
       |SELECT a.event_id as f_event_id ,a.event_name as f_event_name ,
       |time_to_sec(a.duration) as duration,
       |b.chinese_name as f_chinese_name,b.channel_id as f_channel_id,
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
        |a.duration,
        |a.f_chinese_name,a.f_channel_id,
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

  //得到区域相关信息

  def getRegionInfo(hiveContext: HiveContext, regionCode: String) = {
    val sql1 =
      s"""
         |(SELECT province_id as f_province_id,province_name as f_province_name
         |from province
          ) as aa
     """.stripMargin
    val provinceInfoDF = DBUtils.loadMysql(hiveContext, sql1, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val sql2 =
      s"""
         |(SELECT province_id,city_id as f_city_id,city_name as f_city_name
         |from city
          ) as aa
     """.stripMargin
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

  //得到基础表
  def getLookBackReport(basicLookBack: DataFrame, hiveContext: HiveContext, userBehaviour: DataFrame, multiScreen: DataFrame) = {
    basicLookBack.registerTempTable("t_look_back")
    val finalLookBack = hiveContext.sql(
      """select f_date,f_start_time,
        |if((f_hour>9),f_hour,concat('0',cast(f_hour as STRING))) as f_hour,
        |f_timerange,f_province_id,f_province_name,f_city_id,f_city_name,
        |f_region_id,f_region_name,f_terminal,f_event_id,f_event_name,
        |concat_ws(',', collect_set(f_user_id)) as f_user_ids,
        |f_channel_id,f_chinese_name as f_channel_name,
        | f_content_type,
        | sum(videoPlay) as f_click_num,
        | if(sum(f_watch_time)=0,1,sum(f_watch_time)) as f_video_time_sum,
        | duration as f_duration,
        | f_series_id,f_series_num,
        | f_series_name
        | from t_look_back
        | where f_event_name is not null and  f_city_name is not null 
        | group by
        | f_date,f_hour,f_timerange,f_event_id,f_event_name,
        | f_channel_id,f_chinese_name,f_content_type,
        | f_province_name,f_city_name,f_region_name,f_terminal,
        |  f_province_id,f_city_id,f_region_id,duration,f_start_time, f_series_id,
        | f_series_name,f_series_num
      """.stripMargin)
    finalLookBack.registerTempTable("finalLookBack")
    userBehaviour.registerTempTable("userBehaviour")
    multiScreen.registerTempTable("multiScreen")
    val lookBackFinal = hiveContext.sql(
      """
        |select a.f_date,
        |a.f_start_time,
        |a.f_hour,
        |a.f_timerange,
        |a.f_province_id,
        |a.f_province_name,a.f_city_id,a.f_city_name,
        |a.f_region_id,a.f_region_name,
        |a.f_terminal,a.f_event_id,a.f_event_name,
        |a.f_user_ids,
        |a.f_channel_id,a.f_channel_name,
        | a.f_content_type,
        | a.f_click_num,
        |a.f_video_time_sum,
        |a.f_duration,a.f_series_num,
        | a.f_series_id,
        | a.f_series_name,
        | if((c.f_screen is not null),c.f_screen,0) as f_screen,
        |  if((b.f_share is not null),b.f_share,0) as f_share
        |from finalLookBack a left join userBehaviour b
        |on (a.f_hour = b.f_hour and a.f_timerange= b.f_timerange and
        | a.f_event_id = b.f_video_id and a.f_region_id = b.f_region_id
        | and a.f_terminal= b.f_terminal)
        | left join multiScreen c
        | on (a.f_hour = c.f_hour and a.f_timerange= c.f_timerange and
        | a.f_event_id = c.f_program_id and c.f_region_id = c.f_region_id
        | and a.f_terminal= c.f_terminal)
      """.stripMargin)
     // DBUtils.saveToHomedData_2(lookBackFinal, cn.ipanel.common.Tables.t_look_back_statistics_report)
     DBUtils.saveDataFrameToPhoenixNew(lookBackFinal, cn.ipanel.common.Tables.t_look_back_statistics_report)
  }

  def getLookBackUser(basicLookBack: DataFrame, hiveContext: HiveContext) = {
    //  basicLookBack.registerTempTable("t_look_back")
    val finalLookBack = hiveContext.sql(
      """select f_date,
        |if((f_hour>9),f_hour,concat('0',cast(f_hour as STRING))) as f_hour,
        |f_timerange,f_terminal,
        |f_province_id,f_province_name,f_city_id,f_city_name,
        |f_region_id,f_region_name,f_content_type,
        |concat_ws(',', collect_set(f_user_id)) as f_user_ids,
        |count(distinct(f_user_id)) as f_count
        | from t_look_back
        | where f_content_type is not null
        | group by
        | f_date,f_hour,f_timerange,f_province_name,f_province_id,f_city_name,f_city_id,
        |f_region_name,f_region_id,f_content_type,
        |f_terminal
      """.stripMargin
    )
    //  DBUtils.saveToHomedData_2(finalLookBack, cn.ipanel.common.Tables.t_lookback_online_report)
      DBUtils.saveDataFrameToPhoenixNew(finalLookBack, cn.ipanel.common.Tables.t_lookback_online_report)

  }


}



