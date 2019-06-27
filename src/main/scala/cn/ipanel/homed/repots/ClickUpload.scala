package cn.ipanel.homed.repots

import cn.ipanel.common.{DBProperties, SparkSession}
import cn.ipanel.utils.{DBUtils, DateUtils, RegionUtils}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * 搜索推荐次数
  */
object ClickUpload {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("ClickUpload")
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    val regionCode = RegionUtils.getRootRegion
    val time = args(0)
    val parnum = args(1).toInt
    val month = time.substring(0, 6)
    //月
    val year = time.substring(0, 4) //年
    //基础数据
    getBasicBusinessOperation(sqlContext, time, regionCode, parnum, month, year)
    println("搜索推荐基础数据统计结束")
    //点播 推荐 等使用次数
    getBusinessOperationCount(sqlContext, time)
    println("专栏使用率，专栏点播次数统计结束")
    sc.stop()
  }

  def getBusinessOperationCount(sqlContext: HiveContext, time: String) = {
    sqlContext.sql("use userprofile")
    val finaldf1 = sqlContext.sql(
      s"""
         |select  '$time' as f_date,
         |a.f_terminal,
         |a.f_region_id,
         |a.f_region_name,
         |cast(a.f_city_id as string) as f_city_id,
         |a.f_city_name,
         |cast(a.f_province_id as string) as f_province_id,
         |a.f_province_name,
         |sum(a.Recommend) as f_recommend,sum(a.Search) as f_search,sum(a.Rank) as f_rank,
         |sum(a.up_recommend) as f_up_recommend,sum(a.up_search) as f_up_search,sum(a.up_rank) as f_up_rank
         |from
         |(select
         |devicetype as f_terminal,
         |regionid as f_region_id,regionname as f_region_name,
         |cityid as f_city_id,cityname as f_city_name,provinceid as f_province_id,
         |provincename as f_province_name,
         |if((reportType ='Recommend'),1,0) as Recommend,
         |if((reportType ='Search'),1,0) as Search,
         |if((reportType ='TopList' or reportType ='RankList'),1,0) as Rank,
         |if((reportType ='ClickSceneUpload' and exts['method'] like '%recommend%'),1,0) as up_recommend,
         |if((reportType ='ClickSceneUpload' and exts['method'] like '%search%'),1,0) as up_search,
         |if((reportType ='ClickSceneUpload' and (exts['method']='rank_list' or  exts['method']='top_list')),1,0) as up_rank
         |from orc_sence_upload where day ='$time' ) a
         |group by a.f_terminal,a.f_region_id,a.f_region_name,a.f_city_id,a.f_city_name,
         |a.f_province_id,a.f_province_name
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(finaldf1, cn.ipanel.common.Tables.t_click_upload_count)
  }


  def getBasicBusinessOperation(sqlContext: HiveContext, date: String, regionCode: String, parnum: Int, month: String, year: String) = {
    val sql =
      s"""
         |(select cast(a.area_id as char) as regionid,a.area_name as regionname,
         |cast(a.city_id as char) as cityid,c.city_name as cityname,
         |cast(c.province_id as char) as provinceid,p.province_name as provincename
         |from (area a left join city c on a.city_id=c.city_id)
         |left join province p on c.province_id=p.province_id
         |where a.city_id=${regionCode} or c.province_id=${regionCode}
         |) as region
        """.stripMargin
    val regionDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    sqlContext.sql("use bigdata")
    val finalProgramDF = sqlContext.sql(
      s"""
         |select * from
         |orc_user_behavior
         |where day='$date' and (
         |(reportType ='ClickSceneUpload' and exts['method'] is not null) or reportType ='Recommend' or reportType ='Search' or reportType ='TopList' or reportType ='RankList')
        """.stripMargin)
    val finalInfo = regionDF.join(finalProgramDF, Seq("regionid")).repartition(parnum)
    finalInfo.registerTempTable("finalInfo")
    val sql1 =
      s"""
         |insert overwrite table orc_sence_upload partition(day='$date',month='$month',year='$year')
         |select *
         |from finalInfo
         """.stripMargin
    writeToHive(sql1, sqlContext, cn.ipanel.common.Tables.orc_sence_upload, date)
  }

  /**
    * 写入Hive表方法
    */
  def writeToHive(sql: String, sqlContext: SQLContext, tableName: String, day: String) = {
    sqlContext.sql("use userprofile")
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlContext.sql(s"alter table $tableName drop IF EXISTS PARTITION(day='$day')")
    sqlContext.sql(sql)
  }
}
