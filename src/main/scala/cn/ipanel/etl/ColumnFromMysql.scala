package cn.ipanel.etl

import java.sql.DriverManager
import java.util

import cn.ipanel.common.{Constant, DBProperties, SparkSession}
import cn.ipanel.utils.DBUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext


case class col(f_terminal: String, f_series_id: Long, f_column_level: Int, f_column_id: Long)

/**
  * 点播回看栏目关键数据库 得到最末级栏目
  */

object ColumnFromMysql {
  def getColumnFromMysql(time: String, partNum: Int, hiveContext: HiveContext) = {
    //回看 剧集
    val eventseries = getEventSeries(hiveContext)
    //剧集+栏目+tree
    val seriesCloumn = getSeriesColumn(hiveContext, time)
    //tree+终端
    val treeColumn = gettreeColumn(hiveContext)
    //剧集+栏目+终端
    val finalColumndf = treeColumn.join(seriesCloumn, Seq("f_tree_id"))
    val columnMap = getColumnPar(hiveContext)
    //栏目id+栏目名称
    val columndf = getColumn(hiveContext: HiveContext)
    import hiveContext.implicits._
    val columnInfo = finalColumndf.map(x => {
      val f_series_id = x.getAs[Long]("f_series_id")
      val f_column_id = x.getAs[Long]("f_column_id")
      val level = getColumnLeave(x.getAs[Long]("f_column_id"), columnMap)
      val f_terminal = x.getAs[Long]("f_terminal").toString
      col(f_terminal, f_series_id, level, f_column_id)
    }).toDF()
      .join(eventseries, Seq("f_series_id"), "left")
      .repartition(partNum).registerTempTable("df")
    hiveContext.sql("use userprofile")
    hiveContext.sql(
      """
        |insert overwrite table t_column
        |select a.f_series_id,a.f_terminal,a.f_column_id,a.f_column_level,f_event_series_id
        |from
        |(select f_series_id,f_terminal,f_column_id,f_column_level,f_event_series_id,
        | rank() over (partition by f_series_id,f_terminal order by f_column_level desc) rank
        |from
        |df) a
        |where a.rank=1
      """.stripMargin)
  }


  def getEventSeries(hiveContext: HiveContext) = {
    val sql1 =
      """(
           |SELECT f_program_id as f_event_series_id,f_duplicate_id as f_series_id
           |FROM t_duplicate_program
           |where f_program_id between   400000000 and 499999999
            ) as vod_info
      """.stripMargin
    DBUtils.loadMysql(hiveContext, sql1, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
  }


  def getSeries(hiveContext: HiveContext) = {
    val sql =
      """(
           |SELECT distinct(video_id) as f_video_id,f_series_id
           |FROM video_info
            ) as vod_info
      """.stripMargin
    val video = DBUtils.loadMysql(hiveContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val sql1 =
      """(
           |SELECT distinct(event_id) as f_video_id,f_series_id
           |FROM homed_eit_schedule_history
            ) as vod_info
      """.stripMargin
    val event = DBUtils.loadMysql(hiveContext, sql1, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    video.unionAll(event)
  }

  def getBasicInfo(hiveContext: HiveContext, time: String) = {
    hiveContext.sql("use bigdata")
    val user_basic = hiveContext.sql(
      s"""
         |select serviceId as f_video_id,deviceType as f_terminal
         |from  orc_video_play_tmp  where (playtype ='demand'  or playtype ='lookback')
         |and day='$time' group by serviceId,deviceType
            """.stripMargin)
    user_basic
  }

  def getSeriesColumn(sqlContext: HiveContext, time: String) = {
    val sql =
      s"""
         |(SELECT f_column_id,f_program_id as f_series_id,f_tree_id
         |FROM	`t_column_program` WHERE f_program_status = 1 and f_is_hide = 0
         | and f_program_id !=0
         |) as aa
      """.stripMargin
    val columnDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    columnDF
  }

  def gettreeColumn(sqlContext: HiveContext) = {
    val sql =
      """
        |(SELECT f_tree_id,f_device_type as f_terminal
        |FROM	`t_column_device`
        | ) as aa
      """.stripMargin
    val treecolumnDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    treecolumnDF
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
    * 从数据库栏目所需信息
    */
  def getColumnPar(hiveContext: HiveContext) = {
    val map = new java.util.HashMap[Long, Long]()
    val connection = DriverManager.getConnection(DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val statement = connection.createStatement
    val queryRet = statement.executeQuery(
      "SELECT distinct(f_column_id),f_parent_id FROM	`t_column_info` WHERE f_column_status = 1 and f_is_hide = 0 ")
    while (queryRet.next) {
      val f_column_id = queryRet.getLong("f_column_id")
      val f_parent_id = queryRet.getLong("f_parent_id")
      map.put(f_column_id, f_parent_id)
    }
    connection.close
    map
  }


  /**
    * 得到所有栏目信息
    */
  def getColumnLeave(colId: Long, columnInfoMap: util.HashMap[Long, Long]) = {
    var mapId = colId
    var i = 0
    var find = false
    //    val tmplist = List("100","961","962","5034","12071",
    //     "12549","12891", "37845","37922")
    val tmplist = Constant.COLUMN_ROOT
    while (!find && columnInfoMap.keySet().contains(mapId)) {
      val keyId = columnInfoMap.get(mapId)
      if (tmplist.contains(keyId)) {
        find = true
      }
      i = i + 1
      mapId = columnInfoMap.get(mapId)
    }
    i
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("ColumnFromMysql")
    val sc = sparkSession.sparkContext
    val hiveContext = sparkSession.sqlContext
    val time = args(0)
    val partNum = args(1).toInt
    getColumnFromMysql(time: String, partNum: Int, hiveContext: HiveContext)
  }
}
