package cn.ipanel.homed.repots

import cn.ipanel.common.SparkSession
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.hive.HiveContext

object DemandProgramPackage {
  def main(args: Array[String]): Unit = {
    val date = args(0) //yyyy-MM-dd
    val partnum = args(1).toInt
    val nowdate = DateUtils.getAfterDay(date)
    val sparkSession = SparkSession("DemandProgramPackage")
    val sc = sparkSession.sparkContext
    val hiveContext = sparkSession.sqlContext
    val month = DateUtils.getFirstDateOfMonth(date)
    val year = date.substring(0, 4)
    val week = DateUtils.getFirstDateOfWeek(date)
    getProgramPackage(date, date, hiveContext, cn.ipanel.common.Tables.t_package_demand_video_by_day)
    getProgramPackageByMonth(month, date, hiveContext, cn.ipanel.common.Tables.t_package_demand_video_by_month)
    getProgramPackage(week, date, hiveContext, cn.ipanel.common.Tables.t_package_demand_video_by_week)
    //历史记录
    val f_week_date = DateUtils.getDateByDays(date, 6)
    val f_month_date = DateUtils.getDateByDays(date, 29)
    getProgramPackageHistory(f_week_date, date, hiveContext, cn.ipanel.common.Tables.t_package_demand_video_by_history,1)
    //getProgramPackageHistory(f_month_date, date, hiveContext, cn.ipanel.common.Tables.t_package_demand_video_by_history,2)
  }

  def getProgramPackageHistory(start: String, end: String, hiveContext: HiveContext, tableName: String,f:Int) = {
    hiveContext.sql("use userprofile")
    val df = hiveContext.sql(
      s"""
         |select '$start' as f_start_date,'$end' as f_end_date,
         |f_terminal,
         |f_region_id,f_region_name,
         |cast(f_province_id as string) as f_province_id,f_province_name,
         |cast(f_city_id as string) as f_city_id,f_city_name,f_cp_sp,
         | f_package_id,f_package_name,sum(f_count) as f_play_count,f_video_id,f_video_name,
         | f_series_id,f_series_name,
         |sum(f_play_time) as f_play_time,
         |$f as f_type
         |from orc_user_package where day between '$start' and '$end'
         |group by
         |f_terminal,
         |f_region_id,f_region_name,f_city_id,f_city_name,f_province_id,f_province_name,
         |f_package_id ,f_package_name,f_cp_sp,
         |f_video_id,f_video_name,
         | f_series_id,f_series_name
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(df, tableName)
  }

  def getProgramPackageByMonth(start: String, end: String, hiveContext: HiveContext, tableName: String) = {
    hiveContext.sql("use userprofile")
    val df = hiveContext.sql(
      s"""
         |select substr('$start',0,6) as f_date,f_terminal,
         |f_region_id,f_region_name,
         |cast(f_province_id as string) as f_province_id,f_province_name,
         |cast(f_city_id as string) as f_city_id,f_city_name,f_cp_sp,
         | f_package_id,f_package_name,sum(f_count) as f_play_count,f_video_id,f_video_name,
         | f_series_id,f_series_name,
         |sum(f_play_time) as f_play_time
         |from orc_user_package where day between '$start' and '$end'
         |group by
         |f_terminal,
         |f_region_id,f_region_name,f_city_id,f_city_name,f_province_id,f_province_name,
         |f_package_id ,f_package_name,f_cp_sp,
         |f_video_id,f_video_name,
         | f_series_id,f_series_name
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(df, tableName)
  }


  def getProgramPackage(start: String, end: String, hiveContext: HiveContext, tableName: String) = {
    hiveContext.sql("use userprofile")
    val df = hiveContext.sql(
      s"""
         |select '$start' as f_date,f_terminal,
         |f_region_id,f_region_name,
         |cast(f_province_id as string) as f_province_id,f_province_name,
         |cast(f_city_id as string) as f_city_id,f_city_name,f_cp_sp,
         | f_package_id,f_package_name,sum(f_count) as f_play_count,f_video_id,f_video_name,
         | f_series_id,f_series_name,
         |sum(f_play_time) as f_play_time
         |from orc_user_package where day between '$start' and '$end'
         |group by
         |f_terminal,
         |f_region_id,f_region_name,f_city_id,f_city_name,f_province_id,f_province_name,
         |f_package_id ,f_package_name,f_cp_sp,
         |f_video_id,f_video_name,
         | f_series_id,f_series_name
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(df, tableName)
  }


}
