package cn.ipanel.homed.repots

import java.sql.DriverManager
import java.util

import cn.ipanel.common._
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ListBuffer


case class columnRdd(f_column_level: Long, f_column_id: Long, f_column_name: String, f_parent_column_id: Long,
                     f_parent_column_name: String, f_parent_parent_column_id: Long, f_parent_parent_column_name: String)

case class terminalRDD(terminal: String, f_terminal: String)

case class column(f_column_level: Int, f_column_id: Long, f_column_name: String, f_parent_column_id: Long, f_parent_column_name: String, f_parent_parent_column_id: Long, f_parent_parent_column_name: String)
@deprecated
/**
  * 点播观看
  */
object DemandWatch {

  /**
    * 从数据库栏目所需信息
    */
  def getColumnInfo(hiveContext: HiveContext) = {
    val map = new java.util.HashMap[Long, String]()
    val connection = DriverManager.getConnection(DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val statement = connection.createStatement
    val queryRet = statement.executeQuery(
      "SELECT f_column_id,f_parent_id,f_column_name FROM	`t_column_info` WHERE f_column_status = 1")
    while (queryRet.next) {
      val f_column_id = queryRet.getLong("f_column_id")
      val f_column_name = queryRet.getString("f_column_name")
      val f_parent_id = queryRet.getLong("f_parent_id")
      map.put(f_column_id, f_parent_id + "," + f_column_name)
    }
    connection.close
    map
  }

  //栏目
  def getColumn(hiveContext: HiveContext): DataFrame = {
    val sql =
      """
        |(SELECT distinct(f_column_id),f_column_name
        |FROM	`t_column_info` WHERE f_column_status = 1) as aa
      """.stripMargin
    val columnDF = DBUtils.loadMysql(hiveContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    columnDF
  }

  /**
    * 从数据库得到点播媒资需要信息
    */
  def getDemandMeiZi(hiveContext: HiveContext): DataFrame = {

    val sql =
      """(
           |SELECT distinct(a.video_id) as f_video_id,a.video_name as f_video_name,
           |b.f_content_type,b.f_sub_type,
           |a.f_cp_id,a.f_copyright,
           |time_to_sec(a.video_time) as duration,a.f_series_id,b.series_name,
           | b.series_num,a.f_upload_complete_time
           |FROM video_info a join video_series b
           |on a.f_series_id = b.series_id
            ) as vod_info
      """.stripMargin
    val vodMeiZiInfoDF = DBUtils.loadMysql(hiveContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    vodMeiZiInfoDF
  }

  /**
    * 从hive得到点播数据信息(基础表)
    */
  def getBasicDF(time: String, hiveContext: HiveContext,
                 demandMeiZiInfoDF: DataFrame,
                 user_df_basic1: DataFrame, columnMap: java.util.HashMap[Long, String]) = {
    import hiveContext.implicits._
   //user_df_basic1.registerTempTable("user_df_basic1")
    val basic_df1 = hiveContext.sql(
      s"""
         |select '$time' as f_date,
         |f_hour, videoPlay,
         |f_timerange,
         |f_user_id,f_region_name,
         |f_city_id,f_city_name,
         |f_province_id,f_province_name,
         |f_region_id,f_terminal,
         |f_program_id as f_video_id,f_column_id,
         |f_play_time as f_video_time
         |from user_df_basic1
         |where f_play_type = '${GatherType.DEMAND_NEW}' and f_city_name is not null
             """.stripMargin)
    val columnInfo = basic_df1.map(x => {
      val f_column_id = x.getAs[String]("f_column_id")
      val f_column_info = getColumnIdName((getColumnRoot(x.getAs[String]("f_column_id").toInt, columnMap)))
      //(level,columnId,columnName,columnParId,columnParName,columnParPId,columnParPName)
      val level = f_column_info._1
      val columnId = f_column_info._2
      val columnName = f_column_info._3
      val columnParId = f_column_info._4
      val columnParName = f_column_info._5
      val columnParPId = f_column_info._6
      val columnParPName = f_column_info._7
      column(level, columnId, columnName, columnParId, columnParName, columnParPId, columnParPName)
    }).toDF().distinct()
    val demandInfo = demandMeiZiInfoDF.join(basic_df1, Seq("f_video_id"), "right")
    val demandInfo1 = columnInfo.join(demandInfo, Seq("f_column_id"), "right")
    demandInfo1
  }


  /**
    * 得到本级，上一级，上两级栏目id  栏目名字
    */

  def getColumnIdName(comInfo: (ListBuffer[(Long, String)])) = {
    var columnId = 0L
    var columnName = ""
    var columnParId = 0L
    var columnParName = ""
    var columnParPId = 0L
    var columnParPName = ""
    val level = if (comInfo.length - 1 > 0) comInfo.length - 1 else 0
    var j = 0
    for (tuple <- comInfo) {
      if (j == 0) {
        columnId = tuple._1
        columnName = tuple._2
      } else if (j == 1) {
        columnParId = tuple._1
        columnParName = tuple._2
      } else if (j == 2) {
        columnParPId = tuple._1
        columnParPName = tuple._2
      }
      j += 1
    }
    (level, columnId, columnName, columnParId, columnParName, columnParPId, columnParPName)
  }

  /**
    * 得到所有栏目信息
    */
  def getColumnRoot(colId: Long, columnInfoMap: util.HashMap[Long, String]) = {
    var mapId = colId
    var find = false
    val listBuffer = ListBuffer[(Long, String)]()
    //    val tmplist = List("100","961","962","5034","12071",
    //     "12549","12891", "37845","37922")
    val tmplist = Constant.COLUMN_ROOT
    while (!find && columnInfoMap.keySet().contains(mapId)) {
      val keyInfo = columnInfoMap.get(mapId).split(",")
      val keyId = keyInfo(0)
      var keyName = keyInfo(1)
      if (tmplist.contains(keyId)) {
        find = true
        keyName = keyInfo(1)
      }
      listBuffer += ((mapId, keyName))
      mapId = columnInfoMap.get(mapId).split(",")(0).toLong
    }
    listBuffer
  }

  //节目名,节目ID,内容类型,提供商,版权商,饱和度,人均时长,观看总时长
  //平均观看时长,总点击次数,总分享次数,多屏互动点击量,节目观看人数（独立用户数）
  //节目观看人数（独立用户数）
  def getDemandWatchBasic(basicDemand: DataFrame, hiveContext: HiveContext, userBehaviour: DataFrame, multiScreen: DataFrame) = {
    basicDemand.registerTempTable("basicDemand")
    val demandWatchdf = hiveContext.sql(
      s"""
         |select f_date,f_hour,f_timerange,
         |concat_ws(',', collect_set(f_user_id)) as f_user_ids,
         |f_video_id,f_video_name,
         |f_province_id,f_province_name,f_city_id,f_city_name,
         |f_region_id,f_region_name,
         |f_terminal,f_content_type,f_cp_id,f_copyright,
         |f_column_level,f_column_id,f_column_name,
         |f_parent_column_id,f_parent_column_name,
         |f_parent_parent_column_id,f_parent_parent_column_name,
         |if(sum(f_video_time) =0,1,sum(f_video_time)) as f_video_time_sum,
         |duration as f_duration,
         |sum(videoPlay) as f_click_num,
         |series_num as f_series_num,
         |f_series_id,series_name as f_series_name
         |from basicDemand
         |where f_video_name is not null
         |group by f_date,f_hour,f_timerange,f_video_name,f_video_id,f_terminal,f_content_type,f_cp_id,f_copyright,
         |f_province_name,f_city_name,f_province_id,f_city_id,f_region_id,
         |f_region_name,duration,
         |series_num,f_column_level,f_column_id,f_column_name,
         |f_parent_column_id,f_parent_column_name,f_series_id,series_name,
         |f_parent_parent_column_id,f_parent_parent_column_name
             """.stripMargin)
    demandWatchdf.registerTempTable("demandWatchdf")
    // userBehaviour.registerTempTable("userBehaviour")
    //  multiScreen.registerTempTable("multiScreen")
    val demandWatchFinal = hiveContext.sql(
      """
        |select  a.f_date,
        |if((a.f_hour>9),cast(a.f_hour as STRING),concat('0',cast(a.f_hour as STRING))) as f_hour,
        |a.f_timerange,
        |a.f_user_ids,
        |a.f_video_id,a.f_video_name,
        |a.f_province_id,a.f_province_name,a.f_city_id,a.f_city_name,
        |a.f_region_id,a.f_region_name,
        |a.f_terminal,
        |cast(a.f_content_type as STRING) as f_content_type,
        |a.f_cp_id,a.f_copyright,
        |f_column_level,f_column_id,f_column_name,
        |f_parent_column_id,f_parent_column_name,
        |f_parent_parent_column_id,f_parent_parent_column_name,
        |a.f_video_time_sum,
        |a.f_duration,
        |a.f_click_num,
        |if((c.f_screen is not null),c.f_screen,0) as f_screen,
        |if((b.f_share is not null),b.f_share,0) as f_share,
        |a.f_series_num,a.f_series_id,a.f_series_name
        |from demandWatchdf a left join userBehaviour b
        |on (a.f_hour = b.f_hour and a.f_timerange= b.f_timerange and
        | a.f_video_id = b.f_video_id and a.f_region_id = b.f_region_id
        | and a.f_terminal= b.f_terminal)
        | left join multiScreen c
        | on (a.f_hour = c.f_hour and a.f_timerange= c.f_timerange and
        | a.f_video_id = c.f_program_id and a.f_region_id = c.f_region_id
        | and a.f_terminal= c.f_terminal)
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(demandWatchFinal, cn.ipanel.common.Tables.t_demand_watch_statistics_report)
    // DBUtils.saveToHomedData_2(demandWatchFinal, cn.ipanel.common.Tables.t_demand_watch_statistics_report)
  }

  def getTerminal(deviceType: String) = deviceType match {
    case "机顶盒" => "1"
    case "stb" => "1"
    case "CA" => "2"
    case "mobile" => "3"
    case "pad" => "4"
    case "pc" => "5"
    case _ => "0"
  }

  def getDemandUser(basicDemand: DataFrame, hiveContext: HiveContext) = {
    val demandWatchdf = hiveContext.sql(
      s"""
         |select f_date,
         |if((f_hour>9),f_hour,concat('0',cast(f_hour as STRING))) as f_hour,
         |f_timerange,f_terminal,
         |f_province_id,f_province_name,f_city_id,f_city_name,
         |f_region_id,f_region_name,
         |f_content_type,
         |concat_ws(',', collect_set(f_user_id)) as f_user_ids,
         |count(distinct(f_user_id)) as f_count
         |from basicDemand where f_content_type is not null
         |group by f_date,f_hour,f_timerange,
         |f_terminal,f_content_type,f_province_name,
         |f_province_id,f_city_name,f_city_id,
         |f_region_name,f_region_id
             """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(demandWatchdf, cn.ipanel.common.Tables.t_demand_user_by_halfhour)
    // DBUtils.saveToHomedData_2(demandWatchdf, cn.ipanel.common.Tables.t_demand_online_report)
  }
  

}



