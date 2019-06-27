package cn.ipanel.homed.repots

import cn.ipanel.common.SparkSession
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.hive.HiveContext

object ColumnDemandProgramDetails {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("RecommedColumn")
    val sqlContext = sparkSession.sqlContext
    val date = args(0)
    val month = date.substring(0, 6)
    val year = date.substring(0, 4)
    val week = DateUtils.getFirstDateOfWeek(date)
    getColumnBasic(date, date, sqlContext, "day", cn.ipanel.common.Tables.t_demand_column_by_day)
    getColumnBasic(week, date, sqlContext, "day", cn.ipanel.common.Tables.t_demand_column_by_week)
    getColumnBasic(month, month, sqlContext, "month", cn.ipanel.common.Tables.t_demand_column_by_month)
    //前7天 前30天
    val f_week_date = DateUtils.getNDaysBefore(6, date)
    //    val f_month_date = DateUtils.getDateByDays(date, 29)
    getColumnBasicHistory(f_week_date, date, sqlContext, "day", cn.ipanel.common.Tables.t_demand_column_by_history, 1)
  }

  def getColumnBasic(start: String, end: String, sqlContext: HiveContext, columnName: String, tableName: String) = {
    sqlContext.sql("use userprofile")
    val df = sqlContext.sql(
      s"""
         |select  '$start' as f_date,a.f_terminal,
         |cast(a.f_region_id as string) as f_region_id,a.f_region_name,
         |cast(a.f_city_id as string) as f_city_id,
         |a.f_city_name,
         |cast(a.f_province_id as string) as f_province_id,
         |a.f_province_name,a.f_column_id,
         |a.f_column_name,a.f_parent_column_id,a.f_parent_parent_column_id,
         |a.f_parent_column_name,a.f_parent_parent_column_name,
         |cast(a.f_series_id as string) as f_video_id,a.f_series_name as f_video_name,
         |count(*) as f_play_count,sum(a.f_play_time) as f_play_time
         |from t_demand_video_basic a
         |where $columnName between $start and $end
         |group by
         |a.f_terminal,a.f_region_id,a.f_region_name,
         |a.f_city_id,a.f_city_name,
         |a.f_province_id,a.f_province_name,a.f_column_id,
         |a.f_column_name,a.f_parent_column_id,a.f_parent_parent_column_id,
         |a.f_parent_column_name,a.f_parent_parent_column_name,
         |a.f_series_id,a.f_series_name
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(df, tableName)
  }


  def getColumnBasicHistory(start: String, end: String, sqlContext: HiveContext, columnName: String, tableName: String, f: Int) = {
    sqlContext.sql("use userprofile")
    val df = sqlContext.sql(
      s"""
         |select  '$start' as f_start_date,
         |  '$end' as f_end_date,
         |a.f_terminal,
         |cast(a.f_region_id as string) as f_region_id,a.f_region_name,
         |cast(a.f_city_id as string) as f_city_id,
         |a.f_city_name,
         |cast(a.f_province_id as string) as f_province_id,
         |a.f_province_name,a.f_column_id,
         |a.f_column_name,a.f_parent_column_id,a.f_parent_parent_column_id,
         |a.f_parent_column_name,a.f_parent_parent_column_name,
         |cast(a.f_series_id as string) as f_video_id,a.f_series_name as f_video_name,
         |count(*) as f_play_count,sum(a.f_play_time) as f_play_time,
         |$f as f_type
         |from t_demand_video_basic a
         |where $columnName between $start and $end
         |group by
         |a.f_terminal,a.f_region_id,a.f_region_name,
         |a.f_city_id,a.f_city_name,
         |a.f_province_id,a.f_province_name,a.f_column_id,
         |a.f_column_name,a.f_parent_column_id,a.f_parent_parent_column_id,
         |a.f_parent_column_name,a.f_parent_parent_column_name,
         |a.f_series_id,a.f_series_name
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(df, tableName)
  }
}
