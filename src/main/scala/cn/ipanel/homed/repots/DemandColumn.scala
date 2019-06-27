package cn.ipanel.homed.repots

import cn.ipanel.common.SparkSession
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.SQLContext
/**
  * 点播栏目
  */
object DemandColumn {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("DemandColumn")
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    //每个栏目下总共使用时长以及次数
    val time = if (args.length == 1) args(0) else DateUtils.getYesterday()
    getVideoColumn(sqlContext, time)
    println("点播栏目次数时长统计结束")
    //每个栏目下用户使用时长和次数
    getUserColumn(sqlContext,time)
    println("点播栏目用户类型分布统计结束")
    sc.stop()
  }

  //根据日志做关联
  def getVideoColumn(sqlContext: SQLContext, date: String) = {
    sqlContext.sql(" use userprofile")
    val finalDF = sqlContext.sql(
      s"""
        select '$date' as f_date,
         |cast(f_terminal as string) as f_terminal,
         |cast(f_region_id as string) as f_region_id,
         |f_region_name,f_province_id,f_province_name,
         |f_city_id,f_city_name,
         |cast(f_column_id as bigint) as f_column_id,
         |f_column_name,
         |f_parent_column_id,f_parent_column_name,f_parent_parent_column_id,f_parent_parent_column_name,
         |sum(videoPlay) as f_count,sum(f_play_time) as f_play_time
         |from t_demand_video_basic where day='$date' and f_column_id !=0
         |and f_column_id  is not null
         |group by f_terminal,f_region_id,f_province_id,f_province_name,
         |f_city_id,f_city_name,f_region_name,
         |f_column_id,f_column_name,
         |f_parent_column_id,f_parent_column_name,f_parent_parent_column_id, f_parent_parent_column_name
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(finalDF, cn.ipanel.common.Tables.t_column_demand_count)
  }

  def getUserColumn(sqlContext: SQLContext, date: String) = {
    sqlContext.sql("use userprofile")
    val finalDF = sqlContext.sql(
      s"""
          select '$date' as f_date,
         |cast(a.f_terminal as string) as f_terminal,
         |cast(a.f_region_id as string) as f_region_id,
         |a.f_region_name,a.f_province_id,a.f_province_name,
         |a.f_city_id,a.f_city_name,
         |cast(a.f_column_id as bigint) as f_column_id,
         |a.f_column_name,
         |a.f_parent_column_id,a.f_parent_column_name,a.f_parent_parent_column_id,a.f_parent_parent_column_name,
         |a.f_user_type,count(*) as f_user_count
         |from (
         |select
         |f_terminal,f_region_id,f_region_name,f_province_id,f_province_name,
         |f_city_id,f_city_name,f_column_id,f_column_name,
         |f_parent_column_id,f_parent_column_name,f_parent_parent_column_id,f_parent_parent_column_name,
         |f_user_id,
         |sum(videoPlay) as f_count,sum(f_play_time) as f_play_time,
         |(case  when  sum(f_play_time)<=600 then 1
         |when (sum(f_play_time)>600 and sum(f_play_time)<=3600) then 2
         |else 3 end) as f_user_type
         |from t_demand_video_basic  where day='$date' and f_column_id !=0
         |and f_column_id  is not null
         |group by f_terminal,f_region_id,f_province_id,f_province_name,
         |f_city_id , f_city_name, f_region_id,f_region_name,
         |f_column_id,f_column_name,
         |f_parent_column_id,f_parent_column_name,f_parent_parent_column_id,
         | f_parent_parent_column_name,f_user_id) a
         | group by
         | a.f_terminal,a.f_region_id,a.f_region_name,a.f_province_id,a.f_province_name,
         |a.f_city_id,a.f_city_name,a.f_column_id,a.f_column_name,
         |a.f_parent_column_id,a.f_parent_column_name,a.f_parent_parent_column_id,a.f_parent_parent_column_name,
         |a.f_user_type
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(finalDF, cn.ipanel.common.Tables.t_column_user_count)
  }


}
