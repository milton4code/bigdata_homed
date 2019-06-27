package cn.ipanel.homed.repots

import cn.ipanel.common.{SparkSession, Tables}
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.hive.HiveContext

/**
  * 点播报表数据
  */
object DemandReportPeriod {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("DemandReportPeriod")
    val sc = sparkSession.sparkContext
    val hiveContext = sparkSession.sqlContext
    val time = if (args.length == 1) args(0) else DateUtils.getYesterday()
    val month = time.substring(0, 6)
    //月
    val year = time.substring(0, 4)
    //年
    val week = DateUtils.getFirstDateOfWeek(time)
    //周
    val beforeWeek = DateUtils.getNDaysBefore(6, time)
    val beforeMonth = DateUtils.getDateByDays(time, 29)
    getDemandReport(time, time, hiveContext, "day", Tables.t_demand_report_by_day)
    getDemandReport(month, month, hiveContext, "month", Tables.t_demand_report_by_month)
    getDemandReport(week, time, hiveContext, "day", Tables.t_demand_report_by_week)
    getDemandReportHistory(beforeWeek, time, hiveContext, "day", Tables.t_demand_report_by_history, 3)
    //getDemandReportHistory(beforeMonth, time, hiveContext, "day", Tables.t_demand_report_by_history, 2)
    println("点播报表按时间段统计结束")
    getSeriesDemandReport(time, time, hiveContext, "day", Tables.t_demand_series_report_by_day)
    getSeriesDemandReport(month, month, hiveContext, "month", Tables.t_demand_series_report_by_month)
    getSeriesDemandReport(week, time, hiveContext, "day", Tables.t_demand_series_report_by_week)
    getSeriesDemandReportHistory(beforeWeek, time, hiveContext, "day", Tables.t_demand_series_report_by_history, 3)
    println("点播剧集统计结束")
    sc.stop()
  }

  def getDemandReportHistory(startDay: String, endDay: String, hiveContext: HiveContext, columnName: String, tableName: String, f_type: Int) = {
    hiveContext.sql("use userprofile")
    val finaldf = hiveContext.sql(
      s"""
          select '$startDay' as  f_start_date, '$endDay' as  f_end_date,
         |count(distinct(a.f_user_id)) as f_count,
         |a.f_video_id,a.f_video_name,
         |a.f_province_id,a.f_province_name,a.f_city_id,a.f_city_name,
         |a.f_region_id,a.f_region_name,
         |a.f_terminal,a.f_content_type,
         |a.f_cp_id,a.f_copyright,0 as f_column_level, '' as f_column_id,'' as f_column_name,
         |0 as f_parent_column_id,'' as f_parent_column_name,
         |0 as f_parent_parent_column_id,'' as f_parent_parent_column_name,
         |sum(a.f_play_time) as f_video_time_sum,
         |max(a.f_duration) as f_duration,sum(a.videoPlay) as f_click_num,
         |sum(a.f_screen) as f_screen,sum(a.f_share) as f_share,
         |max(a.f_series_num) as f_series_num,a.f_series_id,a.f_series_name,
         |$f_type as f_type
         |from t_demand_video_basic a
         |where $columnName between $startDay and $endDay
         |group by
         |a.f_video_name,a.f_video_id,a.f_terminal,a.f_content_type,a.f_cp_id,a.f_copyright,
         |a.f_province_name,a.f_city_name,a.f_province_id,a.f_city_id,a.f_region_id,
         |a.f_region_name,a.f_series_id,a.f_series_name
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(finaldf, tableName)
  }

  def getDemandReport(startDay: String, endDay: String, hiveContext: HiveContext, columnName: String, tableName: String) = {
    hiveContext.sql("use userprofile")
    val finaldf = hiveContext.sql(
      s"""
          select '$startDay' as  f_date,
         |count(distinct(a.f_user_id)) as f_count,
         |a.f_video_id,a.f_video_name,
         |a.f_province_id,a.f_province_name,a.f_city_id,a.f_city_name,
         |a.f_region_id,a.f_region_name,
         |a.f_terminal,a.f_content_type,
         |a.f_cp_id,a.f_copyright,0 as f_column_level,'' as f_column_id,'' as f_column_name,
         |0 as f_parent_column_id,'' as f_parent_column_name,
         |0 as f_parent_parent_column_id,'' as f_parent_parent_column_name,
         |sum(a.f_play_time) as f_video_time_sum,
         |max(a.f_duration) as f_duration,sum(a.videoPlay) as f_click_num,
         |sum(a.f_screen) as f_screen,sum(a.f_share) as f_share,
         |max(a.f_series_num) as f_series_num,a.f_series_id,a.f_series_name
         |from t_demand_video_basic a
         |where $columnName between $startDay and $endDay
         |group by
         |a.f_video_name,a.f_video_id,a.f_terminal,a.f_content_type,a.f_cp_id,a.f_copyright,
         |a.f_province_name,a.f_city_name,a.f_province_id,a.f_city_id,a.f_region_id,
         |a.f_region_name,a.f_series_id,a.f_series_name
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(finaldf, tableName)
  }

  def getSeriesDemandReport(startDay: String, endDay: String, hiveContext: HiveContext, columnName: String, tableName: String) = {
    hiveContext.sql("use userprofile")
    val finaldf = hiveContext.sql(
      s"""
          select '$startDay' as  f_date,
         |count(distinct(a.f_user_id)) as f_count,
         |cast(a.f_province_id as string) as f_province_id,a.f_province_name,
         |cast(a.f_city_id as string) as f_city_id,a.f_city_name,
         |cast(a.f_region_id as string) as f_region_id,a.f_region_name,
         |a.f_terminal,a.f_content_type,
         |a.f_cp_id,a.f_copyright,0 as f_column_level,'' as f_column_id,'' as f_column_name,
         |0 as f_parent_column_id,'' as f_parent_column_name,
         |0 as f_parent_parent_column_id,'' as f_parent_parent_column_name,
         |sum(a.f_play_time) as f_video_time_sum,
         |sum(a.videoPlay) as f_click_num,
         |sum(a.f_screen) as f_screen,sum(a.f_share) as f_share,
         |max(a.f_series_num) as f_series_num,
         |cast(a.f_series_id as string) as f_series_id,
         |a.f_series_name
         |from t_demand_video_basic a
         |where $columnName between $startDay and $endDay
         |group by
         |a.f_terminal,a.f_content_type,a.f_cp_id,a.f_copyright,
         |a.f_province_name,a.f_city_name,a.f_province_id,a.f_city_id,a.f_region_id,
         |a.f_region_name,a.f_series_id,a.f_series_name
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(finaldf, tableName)
  }

  def getSeriesDemandReportHistory(startDay: String, endDay: String, hiveContext: HiveContext, columnName: String, tableName: String, f_type: Int) = {
    hiveContext.sql("use userprofile")
    val finaldf = hiveContext.sql(
      s"""
          select '$startDay' as  f_start_date, '$endDay' as  f_end_date,
         |count(distinct(a.f_user_id)) as f_count,
         |cast(a.f_province_id as string) as f_province_id,
         |a.f_province_name,
         |cast(a.f_city_id as string) as f_city_id,a.f_city_name,
         |cast(a.f_region_id as string) as f_region_id,
         |a.f_region_name,
         |a.f_terminal,a.f_content_type,
         |a.f_cp_id,a.f_copyright,0 as f_column_level, '' as f_column_id,'' as f_column_name,
         |0 as f_parent_column_id,'' as f_parent_column_name,
         |0 as f_parent_parent_column_id,'' as f_parent_parent_column_name,
         |sum(a.f_play_time) as f_video_time_sum,
         |sum(a.videoPlay) as f_click_num,
         |sum(a.f_screen) as f_screen,sum(a.f_share) as f_share,
         |max(a.f_series_num) as f_series_num,
         |cast(a.f_series_id as string) as f_series_id,
         |a.f_series_name,
         |$f_type as f_type
         |from t_demand_video_basic a
         |where $columnName between $startDay and $endDay
         |group by
         |a.f_terminal,a.f_content_type,a.f_cp_id,a.f_copyright,
         |a.f_province_name,a.f_city_name,a.f_province_id,a.f_city_id,a.f_region_id,
         |a.f_region_name,a.f_series_id,a.f_series_name
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(finaldf, tableName)
  }
}
