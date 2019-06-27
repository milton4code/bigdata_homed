package cn.ipanel.homed.repots

import cn.ipanel.common.{SparkSession, Tables}
import cn.ipanel.utils.{DBUtils, DateUtils, RegionUtils}
import org.apache.spark.sql.hive.HiveContext

/**
  * 回看报表数据
  */
object LookbackReportPeriod {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("LookbackReportPeriod")
    val sc = sparkSession.sparkContext
    val hiveContext = sparkSession.sqlContext
    val regionCode = RegionUtils.getRootRegion
    val time = if (args.length == 1) args(0) else DateUtils.getYesterday()
    val month = time.substring(0, 6)
    //月
    val year = time.substring(0, 4)
    //年
    val week = DateUtils.getFirstDateOfWeek(time)
    //周
    val beforeWeek = DateUtils.getNDaysBefore(6,time)
    val beforeMonth = DateUtils.getDateByDays(time, 29)
    getLookbackReport(time, time, hiveContext, "day", Tables.t_lookback_report_by_day)
    getLookbackReport(month, month, hiveContext, "month", Tables.t_lookback_report_by_month)
    getLookbackReport(week, time, hiveContext, "day", Tables.t_lookback_report_by_week)
    getLookbackReportHistory(beforeWeek, time, hiveContext, "day", Tables.t_lookback_report_by_history, 3)
   // getLookbackReportHistory(beforeMonth, time, hiveContext, "day", Tables.t_lookback_report_by_history, 2)
    println("回看报表统计结束")
    sc.stop()
  }

  def getLookbackReportHistory(startDay: String, endDay: String, hiveContext: HiveContext, columnName: String, tableName: String, ttype: Int) = {
    hiveContext.sql("use userprofile")
    val finaldf = hiveContext.sql(
      s"""
          select '$startDay' as  f_start_date, '$endDay' as  f_end_date,
         |count(distinct(a.f_user_id)) as f_count,
         |a.f_event_id,a.f_event_name,a.f_channel_id,a.f_channel_name,
         |a.f_province_id,a.f_province_name,a.f_city_id,a.f_city_name,
         |a.f_region_id,a.f_region_name,
         |a.f_terminal,a.f_content_type,
         |sum(a.f_play_time) as f_video_time_sum,
         |max(a.f_duration) as f_duration,sum(a.videoPlay) as f_click_num,
         |sum(a.f_screen) as f_screen,sum(a.f_share) as f_share,
         |max(a.f_series_num) as f_series_num,a.f_series_id,a.f_series_name,
         |max(a.f_start_time) as f_start_time,
         | $ttype as f_type
         |from t_lookback_event_basic a
         |where $columnName between $startDay and $endDay
         |group by
         |a.f_event_name,a.f_event_id,a.f_terminal,a.f_content_type,a.f_channel_id,a.f_channel_name,
         |a.f_province_name,a.f_city_name,a.f_province_id,a.f_city_id,a.f_region_id,a.f_region_name,
         |a.f_series_id,a.f_series_name
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(finaldf, tableName)
  }

  def getLookbackReport(startDay: String, endDay: String, hiveContext: HiveContext, columnName: String, tableName: String) = {
    hiveContext.sql("use userprofile")
    val finaldf = hiveContext.sql(
      s"""
          select '$startDay' as  f_date,
         |count(distinct(a.f_user_id)) as f_count,
         |a.f_event_id,a.f_event_name,a.f_channel_id,a.f_channel_name,
         |a.f_province_id,a.f_province_name,a.f_city_id,a.f_city_name,
         |a.f_region_id,a.f_region_name,
         |a.f_terminal,max(a.f_content_type) as f_content_type,
         |sum(a.f_play_time) as f_video_time_sum,
         |max(a.f_duration) as f_duration,sum(a.videoPlay) as f_click_num,
         |sum(a.f_screen) as f_screen,sum(a.f_share) as f_share,
         |max(a.f_series_num) as f_series_num,a.f_series_id,a.f_series_name,
         |max(a.f_start_time) as f_start_time
         |from t_lookback_event_basic a
         |where $columnName between $startDay and $endDay
         |group by
         |a.f_event_name,a.f_event_id,a.f_terminal,a.f_channel_id,a.f_channel_name,
         |a.f_province_name,a.f_city_name,a.f_province_id,a.f_city_id,a.f_region_id,a.f_region_name,
         |a.f_series_id,a.f_series_name
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(finaldf, tableName)
  }


}
