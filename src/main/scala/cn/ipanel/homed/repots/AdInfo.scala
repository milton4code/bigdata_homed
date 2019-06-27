package cn.ipanel.homed.repots

import cn.ipanel.common.DBProperties
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

object AdInfo {

  def getAdvertising(columndf: DataFrame, backMeizi: DataFrame, demandMeiZiInfoDF: DataFrame, time: String, user_df_basic1: DataFrame, hiveContext: HiveContext, adLocation: DataFrame) = {
    user_df_basic1.registerTempTable("user_df_basic1")
    adLocation.registerTempTable("adLocation")
    val ad_df = hiveContext.sql(
      s"""
         |select c.*,
         |row_number() over (partition by c.f_user_id,c.f_terminal,c.f_region_id order by unix_timestamp(c.f_start_time)) as f_rank
         |from
         |(
         |select a.f_user_id,a.f_column_id,
         |a.f_region_id,a.videoPlay,a.f_province_id,a.f_province_name,
         |a.f_city_id,a.f_city_name,a.f_region_id,a.f_region_name,
         |a.f_hour,a.f_start_time,a.f_play_time,
         |a.f_timerange,a.f_column_id,
         |a.f_terminal,a.f_play_type,
         |a.f_program_id,b.f_position
         |from
         |user_df_basic1 a
         |left join adLocation b
         |on a.f_program_id = b.f_asset_id
         |) c
      """.stripMargin)
    ad_df.registerTempTable("ad_df")
    val finalinfo = hiveContext.sql(
      """
        |select a.f_user_id,a.f_terminal,a.f_region_id,a.videoPlay,
        |a.f_hour,a.f_timerange,a.f_play_time,a.f_province_id,a.f_province_name,
        |a.f_city_id,a.f_city_name,a.f_region_id,a.f_region_name,
        |b.f_start_time,a.f_program_id as f_ad_id,
        |a.f_position as f_location_id,b.f_column_id,
        |(case when (a.f_position= -1) then "前置"
        |when (a.f_position = 1) then "后置"
        |when (a.f_position = 0) then "中置"
        |end) as f_location_name,
        |(case when (b.f_play_type= 'live' or b.f_play_type ='timeshift' or b.f_play_type ='one-key-timeshift') then 3
        |when (b.f_play_type= 'demand') then 1
        |when (b.f_play_type= 'lookback') then 2
        |end) as f_type,
        |b.f_program_id as f_event_id
        |from ad_df a
        |left join ad_df b
        |on
        |(case when a.f_position = -1 then a.f_rank =b.f_rank-1
        |when (a.f_position = 1) then a.f_rank =b.f_rank+1
        |when (a.f_position = 0) then a.f_rank =b.f_rank-1
        |end) and a.f_user_id = b.f_user_id and a.f_terminal = b.f_terminal
        |and a.f_region_id = b.f_region_id
      """.stripMargin).filter("f_event_id is not null").join(columndf, Seq("f_column_id"), "left")
    finalinfo.registerTempTable("finalinfo1")
    hiveContext.sql(
      """
        |select f_start_time,
        |f_hour,f_timerange,f_province_id,f_province_name,
        |f_city_id,f_city_name,f_region_id,f_region_name,f_terminal,
        |f_location_id,f_location_name,
        |f_column_id,f_column_name,
        |f_ad_id,
        |f_event_id,
        |f_type,sum(videoPlay) as f_click_num,sum(f_play_time) as f_video_time_sum
        |from finalinfo1
        |group by f_start_time,
        |f_hour,f_timerange,f_province_id,f_province_name,
        |f_city_id,f_city_name,f_region_id,f_region_name,f_terminal,
        |f_location_id,f_location_name,
        |f_column_id,f_column_name,
        |f_ad_id,
        |f_event_id,
        |f_type
      """.stripMargin).registerTempTable("finalinfo")
    backMeizi.registerTempTable("backMeizi") //回看
    demandMeiZiInfoDF.registerTempTable("demandMeiZiInfoDF")
    //点播
    //得到广告名称
    val adDfInfo1 = hiveContext.sql(
      """
        |select a.f_start_time,
        |a.f_hour,a.f_timerange,a.f_province_id,a.f_province_name,
        |a.f_city_id,a.f_city_name,a.f_region_id,a.f_region_name,a.f_terminal,
        |a.f_location_id,a.f_location_name,
        |a.f_column_id,a.f_column_name,
        |a.f_ad_id,b.f_video_name as f_ad_name,
        |a.f_event_id,
        |a.f_type,a.f_click_num,a.f_video_time_sum,b.duration as f_duration
        |from finalinfo a
        |left join demandMeiZiInfoDF b
        |on (a.f_ad_id = b.f_video_id)
      """.stripMargin)
    adDfInfo1.filter("f_type = 1").registerTempTable("demanddf")
    adDfInfo1.filter("f_type = 3").registerTempTable("livedf")
    adDfInfo1.filter("f_type = 2").registerTempTable("lookbackdf")
    val adlookbackDF = hiveContext.sql(
      """
        |select
        |a.f_hour,a.f_timerange,a.f_province_id,a.f_province_name,
        |a.f_city_id,a.f_city_name,a.f_region_id,a.f_region_name,a.f_terminal,
        |a.f_location_id,a.f_location_name,
        |b.f_channel_id as f_column_id,b.f_chinese_name as f_column_name,
        |a.f_ad_id,a.f_ad_name,
        |a.f_event_id,b.f_event_name,
        |a.f_type,a.f_click_num,a.f_video_time_sum,a.f_duration
        |from lookbackdf a
        |left join backMeizi b
        |on (a.f_event_id =b.f_event_id)
      """.stripMargin)
    val epgDF = getChannelAndProgram(hiveContext, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS,
      time, DateUtils.getAfterDay(time))
    epgDF.registerTempTable("epgDF")
    val adliveDF = hiveContext.sql(
      """
        |select
        |a.f_hour,a.f_timerange,a.f_province_id,a.f_province_name,
        |a.f_city_id,a.f_city_name,a.f_region_id,a.f_region_name,a.f_terminal,
        |a.f_location_id,a.f_location_name,
        |a.f_event_id as f_column_id,
        |b.chinese_name as f_column_name,
        |a.f_ad_id,a.f_ad_name,
        |b.event_id as f_event_id,b.event_name as f_event_name,
        |a.f_type,a.f_click_num,a.f_video_time_sum,a.f_duration
        |from livedf a
        |join epgDF b
        |on a.f_event_id=b.homed_service_id
        |where a.f_start_time>=b.start_time and a.f_start_time<=b.end_time
      """.stripMargin)
    val addemandDF = hiveContext.sql(
      s"""
         |select '$time' as f_date,
         |a.f_hour,a.f_timerange,a.f_province_id,a.f_province_name,
         |a.f_city_id,a.f_city_name,a.f_region_id,a.f_region_name,a.f_terminal,
         |a.f_location_id,a.f_location_name,
         |a.f_column_id,if((a.f_column_name is null),'',a.f_column_name) as f_column_name,
         |a.f_ad_id,a.f_ad_name,
         |a.f_event_id,b.f_video_name as f_event_name,
         |a.f_type,a.f_click_num,a.f_video_time_sum,a.f_duration
         |from demanddf a
         |left join demandMeiZiInfoDF b
         |on (a.f_event_id = b.f_video_id)
      """.stripMargin)
    val finalAd = adlookbackDF.unionAll(adliveDF).unionAll(addemandDF)
    DBUtils.saveToHomedData_2(finalAd, "t_ad_report")
  }

  def getChannelAndProgram(sqlContext: SQLContext, url: String, user: String, pass: String, startDate: String, endDate: String) = {
    //    val liveProgram="(select homed_service_id,event_id,event_name,f_director,f_actor,f_content_type,f_sub_type,language,f_country,f_screen_time,f_score,start_time,date_add(start_time,interval duration hour_second) as end_time,time_to_sec(duration) as duration from homed_eit_schedule_history where start_time>='"+startDate+"' and start_time<='"+endDate+"') t"
    val sql =
      s"""
         |( select a.homed_service_id,a.event_id ,a.event_name,
         |a.f_content_type,a.start_time,
         |date_add(a.start_time,interval a.duration hour_second) as end_time,
         |time_to_sec(a.duration) as duration,b.chinese_name
         |from homed_eit_schedule_history a
         |left join
         |channel_store b
         |on a.homed_service_id = b.channel_id
         |where a.start_time>='$startDate' and a.start_time<='$endDate') as aa
      """.stripMargin
    val liveProgramDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    liveProgramDF
  }

  def getAdLocation(hiveContext: HiveContext) = {
    //    val sql =
    //      """(
    //           |SELECT a.f_period_id,a.f_asset_id,a.f_position,
    //           |cast(b.f_data as char) as f_data
    //           |from t_ad_period_materials_asset a
    //           |left join t_ad_period b
    //           |on a.f_period_id =b.f_period_id
    //            ) as ad_info
    //      """.stripMargin
    val sql =
    """(
           |SELECT distinct(f_asset_id),f_position
           |from t_ad_period_materials_asset
            ) as ad_info
      """.stripMargin
    val adLocationDF = DBUtils.loadMysql(hiveContext, sql, DBProperties.JDBC_URL_ILOG, DBProperties.USER_ILOG, DBProperties.PASSWORD_ILOG)
    //    adLocationDF.registerTempTable("adLocationDF")
    //    val adDF = hiveContext.sql(
    //      """
    //        |select f_period_id,f_position,f_asset_id,
    //        |get_json_object(regexp_replace((regexp_replace(get_json_object(get_json_object(f_data,'$.position'),'$.columns'),'\\[','')),'\\]','') ,"$.columnId") as f_ad_column
    //        |from adLocationDF
    //      """.stripMargin)
    adLocationDF
  }
}
