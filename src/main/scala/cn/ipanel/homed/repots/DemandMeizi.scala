package cn.ipanel.homed.repots

import cn.ipanel.common.{DBProperties, SparkSession}
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ListBuffer

/**
  * 在库媒资统计
  * 在库点播媒资统计
  * 在库回看媒资统计
  */

case class CpContentType(daytime: String, f_cp_id: String, f_content_type: String)

object DemandMeizi {
  def main(args: Array[String]): Unit = {
    val sparkSession
    = new SparkSession("DemandMeizi")
    val hiveContext = sparkSession.sqlContext
    //媒资统计数据（表格）
    //日期，节目名,节目ID,内容类型,提供商,版权终端,总集数,时长,入库时间,入库人,发布时间,发布人,媒资状态
    val time = if (args.length == 1) args(0) else DateUtils.getYesterday()
    val nowtime = DateUtils.getAfterDay(time)
    //清除表
    delMysql1(time, nowtime, cn.ipanel.common.Tables.meizi_statisc_report)
    delMysql2(time, nowtime, cn.ipanel.common.Tables.t_meizi_lookback_report)
    delMysql(time, cn.ipanel.common.Tables.meizi_content_chart)
    delMysql(time, cn.ipanel.common.Tables.t_cp_up_under_count)
    println("删除成功")
    //点播报表
    getMeiZiStatistic(time, nowtime, hiveContext)
    println("点播报表统计结束")
    // 回看报表
    getLookBackMeiZiStatistic(time, nowtime, hiveContext)
    println("回看报表统计结束")
    // 得到媒资基本信息
    val basicMeizi = getMeiZiBasic(nowtime, hiveContext)
    val lookBackMeizi = getLookBackMeiZiBasic(nowtime, hiveContext)
    //按内容类型分  回看
    getMeiZiContent(time, hiveContext, lookBackMeizi)
    println("回看图表统计结束")
    //供应商 内容类型 点播
    getMeiZiContentCp(time, hiveContext, basicMeizi)
    println("点播图表统计结束")
    //新增 下架
    getDemandUpDone(hiveContext, time, nowtime)
    println("每一天发布下架统计结束")
    //点播 一级+二级
    getVideoContextSub(hiveContext, basicMeizi, time)
    println("点播按内容类型分统计结束")
    //回看 一级+二级
    getEventContextSub(hiveContext, lookBackMeizi, time)
    println("回看按内容类型分统计结束")
    //发布下架
    getMeiZiStatusCount(time, nowtime, hiveContext)
    println("发布下架总数")
    sparkSession.stop()
  }


  def getMeiZiBasic(nowtime: String, sqlContext: HiveContext): DataFrame = {
    val sql =
      s"""(
         select
         |cast(a.video_id as char) as video_id,cast(a.f_series_id as char) as f_series_id,
         |b.f_sub_type,
         |cast(b.f_content_type as char) as f_content_type,a.f_cp_id,
         |time_to_sec(a.video_time) as video_time
         |from video_info a
         |left join video_series b
         |on a.f_series_id = b.series_id
         |where a.f_edit_time<'$nowtime' and  a.video_time !=''
         | ) as vod_info
          """.stripMargin
    val meiZiInfoDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    meiZiInfoDF
  }


  def getLookBackMeiZiBasic(nowtime: String, sqlContext: HiveContext): DataFrame = {
    val sql =
      s"""(
         |SELECT cast(a.event_id as char) as event_id,cast(a.f_series_id as char) as f_series_id,
         |cast(b.f_content_type as char) as f_content_type,b.f_sub_type,
         |time_to_sec(a.duration) as video_time
         |FROM homed_eit_schedule_history a
         |join event_series b
         |on a.f_series_id = b.series_id
         |where a.start_time < $nowtime and a.duration is not null
         |and  b.f_content_type != '' and b.f_content_type !=0 and a.f_series_id >0
         ) as look_back_info
          """.stripMargin
    val meiZiInfoDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    meiZiInfoDF
  }

  def getMeiZiContent(time: String, sqlContext: HiveContext, lookBackMeizi: DataFrame) = {
    lookBackMeizi.registerTempTable("t_look_back_Meizi")
    val contentType = sqlContext.sql(
      s"""
         |SELECT  $time as f_date,
         |f_content_type,count(*) as f_count,
         |sum(video_time) as f_video_time,count(distinct(f_series_id)) as f_series_count
         |FROM t_look_back_Meizi
         |group by f_content_type,$time
          """.stripMargin)
    DBUtils.saveToHomedData_2(contentType, cn.ipanel.common.Tables.meizi_content_chart)
  }


  def getMeiZiContentCp(time: String, sqlContext: HiveContext, basicMeizi: DataFrame) = {
    basicMeizi.registerTempTable("t_basicMeizi")
    val video = sqlContext.sql(
      s"""
         | SELECT  $time as f_date,
         | f_cp_id,f_content_type,1 as f_status,count(*) as f_count,
         | sum(video_time) as f_sum_time,1 as f_type
         | FROM t_basicMeizi
         | group by f_cp_id,f_content_type
          """.stripMargin)
    val series = sqlContext.sql(
      s"""
         | SELECT  $time as f_date,
         | f_cp_id,f_content_type,1 as f_status,
         | count(distinct(f_series_id)) as f_count,0 as f_sum_time,2 as f_type
         | FROM t_basicMeizi
         | group by f_cp_id,f_content_type
          """.stripMargin)
    DBUtils.saveToHomedData_2(video, cn.ipanel.common.Tables.t_cp_up_under_count)
    DBUtils.saveToHomedData_2(series, cn.ipanel.common.Tables.t_cp_up_under_count)
    //DBUtils.saveToHomedData_2(contentType, cn.ipanel.common.Tables.meizi_cp_content_chart)
  }

  def getMeiZiStatistic(time: String, nowtime: String, sqlContext: HiveContext) = {
    //从数据库读取媒资信息 点播
    val aa = "999,919,929,939,949,1199,1299,1399,1499,1599,1699"
    val sql =
      s"""(
         |SELECT
         | a.video_name ,a.video_id ,a.f_copyright,
         | a.f_content_type ,a.f_sub_type,a.f_cp_id,
         |if((b.f_platform = ''),'6',b.f_platform) as f_terminal,
         |b.series_num,
         |time_to_sec(a.video_time) as f_video_time,
         |a.register_date as f_upload_complete_time,
         |a.user_id,a.f_release_time,a.f_release_user_id,
         |(case when a.status <900 then 1
         |when (a.status =999  or a.status =919 or a.status =929 or a.status =939 or a.status =949 or a.status =1199 or a.status =1299 or a.status =1399 or a.status =1499 or a.status =1599 or a.status =1699) then 3
         |when (a.status >=3000 and a.status<9000) then 4
         |when (a.status =9000 or a.status=10000) then 5
         |when (a.status =9999) then 6
         |else 2 end ) as status,
         |a.f_series_id,b.series_name
         |FROM video_info a
         |join video_series b
         |on a.f_series_id = b.series_id
         |where  a.f_edit_time >'$time' and a.f_edit_time <'$nowtime'
         ) as vod_info
          """.stripMargin
    val meiZiInfoDF1 = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    meiZiInfoDF1.registerTempTable("meiZiInfoDF")
    val meiZiInfoDF = sqlContext.sql(
      """
        |select *,f_platform
        |from meiZiInfoDF
        |lateral view explode(split(f_terminal, ';')) myTable as f_platform
      """.stripMargin).filter("f_platform !=''")
    //得到操作者id 操作者名字
    val sql1 =
      s"""(
         |SELECT operator_id,operator_name from operator_info
          ) as vod_info
          """.stripMargin
    val operateDF = DBUtils.loadMysql(sqlContext, sql1, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    meiZiInfoDF.registerTempTable("t_demand_meizi")
    operateDF.registerTempTable("operate")
    //查询媒资库消息
    val basic_df = sqlContext.sql(
      """
        |select
        |a.video_name as f_video_name,a.video_id as f_video_id,a.f_content_type,
        |a.f_sub_type,a.f_cp_id,
        |a.f_platform as f_terminal,a.f_copyright,
        |a.series_num as f_series_num,
        |a.f_video_time,a.f_upload_complete_time,
        |a.user_id as f_user_id,b.operator_name as f_user_name,
        |a.f_release_time,a.f_release_user_id,a.status as f_status,
        |a.f_series_id,a.series_name as f_series_name
        |from t_demand_meizi a
        |left join operate b on a.user_id = b.operator_id
      """.stripMargin).registerTempTable("t_demand_meizi1")
    val basic_df1 = sqlContext.sql(
      s"""
         |select
         |a.f_video_name,a.f_video_id,a.f_content_type,a.f_cp_id,a.f_copyright,
         |a.f_terminal,
         |a.f_series_num,a.f_video_time,a.f_upload_complete_time,
         |a.f_user_id,a.f_user_name,
         |a.f_release_time,a.f_release_user_id,b.operator_name as f_release_user_name,
         |a.f_status,a.f_series_id,a.f_series_name,'$time' as f_date,a.f_sub_type
         |from t_demand_meizi1 a
         |left join operate b on a.f_release_user_id = b.operator_id
      """.stripMargin)
    DBUtils.saveToHomedData_2(basic_df1, cn.ipanel.common.Tables.meizi_statisc_report)
  }


  def getLookBackMeiZiStatistic(time: String, nowtime: String, sqlContext: HiveContext) = {
    val sql =
      s"""(
         |SELECT
         |a.start_time as f_start_time,
         |a.homed_service_id as f_channel_id,
         |a.event_id as f_event_id,
         |a.event_name as f_event_name,
         |time_to_sec(a.duration) as f_event_time,
         |a.f_content_type,a.f_series_id,
         |b.series_num as f_series_num
         |FROM homed_eit_schedule_history a
         |left join event_series b
         |on a.f_series_id = b.series_id
         |where a.start_time < $nowtime and  a.duration is not null and a.start_time > $time
         |and  a.f_content_type != ''
         ) as look_back_info
          """.stripMargin
    val lookBackMeiZiInfoDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    lookBackMeiZiInfoDF.registerTempTable("look_back")
    val sql1 =
      s"""(
         |SELECT channel_id as f_channel_id,
         |chinese_name as f_channel_name
         |from channel_store
         ) as look_back_info
          """.stripMargin
    val channelInfoDF = DBUtils.loadMysql(sqlContext, sql1, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    channelInfoDF.registerTempTable("channel_info")
    val finalInfo = sqlContext.sql(
      """
        |select a.f_start_time,a.f_channel_id,b.f_channel_name,a.f_event_id,a.f_event_name,a.f_event_time,a.f_content_type,
        |a.f_series_id,a.f_series_num
        | from look_back a left join channel_info b
        | on a.f_channel_id = b.f_channel_id
      """.stripMargin)
    val map = Map("f_series_num" -> "0")
    val finall = finalInfo.na.fill(map)
    DBUtils.saveToHomedData_2(finall, cn.ipanel.common.Tables.t_meizi_lookback_report)
  }

  /**
    * 防止重跑导致重复数据
    **/
  def delMysql1(day: String, nowday: String, table: String): Unit = {
    val time = DateUtils.transformDateStr(day)
    val nowtime = DateUtils.transformDateStr(nowday)
    val del_sql = s"delete from $table where f_upload_complete_time>'$time' and f_upload_complete_time <'$nowtime' "
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, del_sql)
  }

  /**
    * 防止重跑导致重复数据
    **/
  def delMysql2(day: String, nowday: String, table: String): Unit = {
    val time = DateUtils.transformDateStr(day)
    val nowtime = DateUtils.transformDateStr(nowday)
    val del_sql = s"delete from $table where f_start_time>'$time' and f_start_time <'$nowtime' "
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, del_sql)
  }

  /**
    * 防止重跑导致重复数据
    **/
  def delMysql(day: String, table: String): Unit = {
    val del_sql = s"delete from $table where f_date='$day'"
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, del_sql)
  }

  def getDemandUpDone(sqlContext: HiveContext, time: String, nowtime: String) = {
    val sql =
      s"""(
         |select '$time' as f_date,f_cp_id,f_content_type,f_status,
         |count(*) as f_count,sum(f_video_time) as f_sum_time,
         |1 as f_type
         |from
         |t_meizi_statisc_report
         |where (f_status =5 or f_status = 6) and f_date  = '$time'
         |group by f_cp_id,f_content_type,f_status
         |) a
          """.stripMargin
    val video = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)
    val sql1 =
      s"""
         |(select  '$time' as f_date,f_cp_id,f_content_type,status,count(*) as f_count, 0 as f_sum_time,
         |2 as f_type
         |from
         |video_series
         |where (status = 5 or status = 6) and
         |f_edit_time between '$time' and '$nowtime'
         |group by
         |f_cp_id,f_content_type,status) a
        """.stripMargin
    val series = DBUtils.loadMysql(sqlContext, sql1, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    video.unionAll(series)
    DBUtils.saveToHomedData_2(video.unionAll(series), cn.ipanel.common.Tables.t_cp_up_under_count)
  }

  def getEventContextSub(sqlContext: HiveContext, basicMedia: DataFrame, time: String) = {
    basicMedia.registerTempTable("basicMedia")
    import sqlContext.implicits._
    val listRdd = basicMedia.filter("f_sub_type !=''")
      .map(x => {
        val f_content_type = x.getAs[String]("f_content_type")
        val f_sub_type = x.getAs[String]("f_sub_type")
        val f_video_id = x.getAs[String]("event_id")
        val f_series_id = x.getAs[String]("f_series_id")
        val f_video_time = x.getAs[Long]("video_time")
        (f_content_type, f_sub_type, f_video_id, f_series_id, f_video_time)
      }).map(x => {
      val listBuffer = ListBuffer[(String, String, String, String, Long)]()
      if (x._2.contains("|")) {
        for (aa <- x._2.split("\\|")) {
          listBuffer += ((x._1, aa, x._3, x._4, x._5))
        }
      } else {
        listBuffer += ((x._1, x._2, x._3, x._4, x._5))
      }
      listBuffer.toList
    })
    listRdd.flatMap(x => x).map(x => {
      aa(x._1, x._2, x._3, x._4, x._5)
    }).toDF().registerTempTable("listRdd")
    val contentType = sqlContext.sql(
      s"""
         |select '$time' as f_date,f_content_type,f_sub_type,count(*) as f_count,
         |sum(video_time) as f_video_time,
         |count(distinct(f_series_id)) as f_series_count
         |from listRdd
         |group by f_content_type,f_sub_type
        """.stripMargin)
    DBUtils.saveToHomedData_2(contentType, cn.ipanel.common.Tables.t_media_event_content_sub)

  }

  case class aa(f_content_type: String, f_sub_type: String, video_id: String, f_series_id: String, video_time: Long)

  def getVideoContextSub(sqlContext: HiveContext, basicMedia: DataFrame, time: String) = {
    import sqlContext.implicits._
    val listRdd = basicMedia
      .filter("f_sub_type !=''")
      .map(x => {
        val f_content_type = x.getAs[String]("f_content_type")
        val f_sub_type = x.getAs[String]("f_sub_type")
        val f_video_id = x.getAs[String]("video_id")
        val f_series_id = x.getAs[String]("f_series_id")
        val f_video_time = x.getAs[Long]("video_time")
        (f_content_type, f_sub_type, f_video_id, f_series_id, f_video_time)
      }).map(x => {
      val listBuffer = ListBuffer[(String, String, String, String, Long)]()
      if (x._2.contains("|")) {
        for (aa <- x._2.split("\\|")) {
          listBuffer += ((x._1, aa, x._3, x._4, x._5))
        }
      } else {
        listBuffer += ((x._1, x._2, x._3, x._4, x._5))
      }
      listBuffer.toList
    })
    listRdd.flatMap(x => x).map(x => {
      aa(x._1, x._2, x._3, x._4, x._5)
    }).toDF().registerTempTable("listRdd")
    val contentType = sqlContext.sql(
      s"""
         |select '$time' as f_date,f_content_type,f_sub_type,count(*) as f_count,
         |sum(video_time) as f_video_time,
         |count(distinct(f_series_id)) as f_series_count
         |from listRdd
         |group by f_content_type,f_sub_type
        """.stripMargin)
    DBUtils.saveToHomedData_2(contentType, cn.ipanel.common.Tables.t_media_video_content_sub)
  }

  def getMeiZiStatusCount(time: String, nowtime: String, sqlContext: HiveContext) = {
    val sql =
      s"""(
         |select  '$time' as f_date,f_cp_id,f_content_type,f_status,
         |count(*) as f_count,sum(f_video_time) as f_sum_time,
         |1 as f_type
         |from
         |(select
         |cast(a.video_id as char) as video_id,
         |cast(b.f_content_type as char) as f_content_type,a.f_cp_id,
         |if((a.status= 10000 or a.status = 9000),4,2) as f_status,
         |time_to_sec(a.video_time) as f_video_time
         |from video_info a
         |left join video_series b
         |on a.f_series_id = b.series_id
         |where (a.status= 10000 or a.status = 9000 or a.status =9999) and a.f_edit_time <$nowtime
         |and a.video_time !=''
         | ) a
         | group by a.f_cp_id,a.f_content_type,f_status) as vod_info
          """.stripMargin
    val video = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val sql1 =
      s"""
         |(select  '$time' as f_date,f_cp_id,f_content_type,
         |if((status =5),4,2) as f_status,
         |count(*) as f_count, 0 as f_sum_time,
         |2 as f_type
         |from
         |video_series
         |where (status = 5 or status = 6) and
         |f_edit_time < '$nowtime'
         |group by
         |f_cp_id,f_content_type,status) a
        """.stripMargin
    val series = DBUtils.loadMysql(sqlContext, sql1, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    DBUtils.saveToHomedData_2(video, cn.ipanel.common.Tables.t_cp_up_under_count)
    DBUtils.saveToHomedData_2(series, cn.ipanel.common.Tables.t_cp_up_under_count)
  }
}

