package cn.ipanel.homed.repots

import cn.ipanel.common.SparkSession
import cn.ipanel.utils.{DBUtils, DateUtils, RegionUtils}
import org.apache.spark.sql.hive.HiveContext
/**
推荐场景分析
  */
object RecommedColumn {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("RecommedColumn")
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    val regionCode = RegionUtils.getRootRegion
    val time = args(0)
    val parnum = args(1)
    val month = time.substring(0, 6)
    //月
    val year = time.substring(0, 4)
    //季度
    val quarterDate = DateUtils.getFirstDateOfQuarter(time)
    val quarter = quarterDate.substring(0, 6)
    //周
    val week = DateUtils.getFirstDateOfWeek(time)
    val beforeWeek = DateUtils.getDateByDays(time, 6)
    val beforeMonth = DateUtils.getDateByDays(time, 29)
    //专栏首页点播数据
    getRecommendColumn(sqlContext, time, time, "day", cn.ipanel.common.Tables.t_recommend_column_by_day) //日
    getRecommendColumn(sqlContext, month, month, "month", cn.ipanel.common.Tables.t_recommend_column_by_month) //月
    getRecommendColumn(sqlContext, week, time, "day", cn.ipanel.common.Tables.t_recommend_column_by_week) //周
    getRecomendHistory(sqlContext, beforeWeek, time, 3, cn.ipanel.common.Tables.t_recommend_column_by_history)
    getRecomendHistory(sqlContext, beforeMonth, time, 2, cn.ipanel.common.Tables.t_recommend_column_by_history)
   println("专栏数据统计结束")
    sc.stop()
  }

  def getRecomendHistory(sqlContext: HiveContext, f_start_time: String, f_end_time: String, f_type: Int, tableName: String) = {
    sqlContext.sql("use userprofile")
    val finaldf = sqlContext.sql(
      s"""
         |select  '$f_start_time' as f_start_date,'$f_end_time' as f_end_date,
         |devicetype as f_terminal,
         |regionid as f_region_id,regionname as f_region_name,
         |cityid as f_city_id,cityname as f_city_name,provinceid as f_province_id,
         |provincename as f_province_name,
         |exts['scene'] as f_scene,
         |count(*) as f_count,
         |count(distinct(userid)) as f_user_count,
         | $f_type as f_type
         |from
         |orc_sence_upload
         |where exts['method'] like '%recommend%'
         |and day between '$f_start_time' and '$f_end_time'
         |and (exts['scene'] ='homepage' or exts['scene'] ='detailpage')
         |group by
         |devicetype,regionid,regionname,cityid,cityname,provinceid,
         |provincename,exts['scene']
       """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(finaldf, tableName)
  }

  def getRecommendColumn(sqlContext: HiveContext, f_start_time: String, f_end_time: String, columnName: String, tableName: String) = {
    sqlContext.sql("use userprofile")
    val finaldf = sqlContext.sql(
      s"""
         |select   '$f_start_time' as f_date,
         |devicetype as f_terminal,
         |regionid as f_region_id,regionname as f_region_name,
         |cityid as f_city_id,cityname as f_city_name,provinceid as f_province_id,
         |provincename as f_province_name,
         |exts['scene'] as f_scene,
         |count(*) as f_count,
         |count(distinct(userid)) as f_user_count
         |from
         |orc_sence_upload
         |where exts['method'] like '%recommend%'
         | and $columnName between '$f_start_time' and '$f_end_time'
         |and ( exts['scene'] ='homepage' or exts['scene'] ='detailpage')
         |group by
         |devicetype,regionid ,regionname,cityid,cityname,provinceid,
         |provincename,exts['scene']
       """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(finaldf, tableName)
  }
}
