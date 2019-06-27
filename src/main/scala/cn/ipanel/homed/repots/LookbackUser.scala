package cn.ipanel.homed.repots

import cn.ipanel.common.{SparkSession, Tables}
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.hive.HiveContext

/**
  * 回看在线人数
  */
object LookbackUser {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("LookbackUser")
    val sc = sparkSession.sparkContext
    val hiveContext = sparkSession.sqlContext
    val time = if (args.length == 1) args(0) else DateUtils.getYesterday()
    val month = time.substring(0, 6)
    //月
    val year = time.substring(0, 4)
    //年
    val week = DateUtils.getFirstDateOfWeek(time)
    //周
    val beforeWeek = DateUtils.getNDaysBefore(6,time)
    val beforeMonth = DateUtils.getDateByDays(time, 29)
    getLookUser(time, time, hiveContext, "day", Tables.t_look_user_by_day)
    getLookUser(month, month, hiveContext, "month", Tables.t_look_user_by_month)
    getLookUser(week, time, hiveContext, "day", Tables.t_look_user_by_week)
    getLookUserHistory(beforeWeek, time, hiveContext, "day", Tables.t_look_user_by_history, 3)
   // getLookUserHistory(beforeMonth, time, hiveContext, "day", Tables.t_look_user_by_history, 2)
    println("回看用户统计结束")
    sc.stop()
  }

  def getLookUserHistory(startDay: String, endDay: String, hiveContext: HiveContext, columnName: String, tableName: String, f_type: Int) = {
    hiveContext.sql("use userprofile")
    val finaldf = hiveContext.sql(
      s"""
          select '$startDay' as  f_start_date,'$endDay' as  f_end_date,
         |count(distinct(a.f_user_id)) as f_count,
         |a.f_province_id,a.f_province_name,a.f_city_id,a.f_city_name,
         |a.f_region_id,a.f_region_name,
         |a.f_terminal,a.f_content_type,
         |$f_type as f_type
         |from t_lookback_event_basic a
         |where $columnName between $startDay and $endDay
         |and f_content_type !=0
         |group by
         |a.f_terminal,a.f_content_type,
         |a.f_province_name,a.f_city_name,a.f_province_id,a.f_city_id,a.f_region_id,
         |a.f_region_name
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(finaldf, tableName)
  }


  def getLookUser(startDay: String, endDay: String, hiveContext: HiveContext, columnName: String, tableName: String) = {
    hiveContext.sql("use userprofile")
    val finaldf = hiveContext.sql(
      s"""
          select '$startDay' as  f_date,
         |count(distinct(a.f_user_id)) as f_count,
         |a.f_province_id,a.f_province_name,a.f_city_id,a.f_city_name,
         |a.f_region_id,a.f_region_name,
         |a.f_terminal,a.f_content_type
         |from t_lookback_event_basic a
         |where $columnName between $startDay and $endDay
         | and f_content_type !=0 group by
         |a.f_terminal,a.f_content_type,
         |a.f_province_name,a.f_city_name,a.f_province_id,a.f_city_id,a.f_region_id,
         |a.f_region_name
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(finaldf, tableName)
  }


}
