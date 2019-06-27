package cn.ipanel.homed.repots

import java.sql.DriverManager
import java.util

import cn.ipanel.common.{SparkSession, _}
import cn.ipanel.utils.LogUtils.Stopwatch
import cn.ipanel.utils.{DBUtils, DateUtils, MOEDateUtils, RegionUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
  * 栏目点击次数报表统计
  * 入参为日期和日志来源 如 20190412 false  其中日志来源标志中 false 代表从用户上报读取数据，true 代表从nginx日志读取数据
  *
  **/
case class newLog(f_column_id: Long = 0, f_column_level: Integer = 0, f_column_name: String = "", f_parent_column_id: Long = 0, f_parent_column_name: String = "",
                  f_parent_parent_column_id: Long = 0, f_parent_parent_column_name: String = "", f_region_id: String = "", f_city_id: String = "", f_province_id: String = "",
                  f_date: String = "", f_hour: String = "", f_timerange: String = "", f_userid: String = "", f_terminal: String = "", f_pv: Integer = 0, f_province_name: String = "", f_city_name: String = "", f_region_name: String = "")

case class newLog1(f_column_id: Long = 0, f_column_level: Integer = 0, f_column_name: String = "", f_parent_column_id: Long = 0, f_parent_column_name: String = "",
                   f_parent_parent_column_id: Long = 0, f_parent_parent_column_name: String = "", f_region_id: String = "",
                   f_date: String = "", f_hour: String = "", f_timerange: String = "", f_userid: String = "", f_terminal: String = "", f_pv: Integer = 0, f_province_id: String = "", f_province_name: String = "", f_city_id: String = "", f_city_name: String = "", f_region_name: String = "")

object ColumnDetailNew {

  def main(args: Array[String]): Unit = {
    val time = new Stopwatch("栏目点击次数报表统计 ColumnDetailNew ")
    var day = DateUtils.getYesterday()
    if (args.length != 2) {
      println("请输入参数 如 20180327 false")
      System.exit(-1)
    }

    val sparkSession = SparkSession("ColumnDetailNew")
    val hiveContext = sparkSession.sqlContext

    //日期
    val begin = args(0)
    val version = args(1).toBoolean //true nginx 日志 | false 用户上报
    val year = MOEDateUtils.getYear(begin).toString
    val columnInfoMap = toParent(hiveContext)
    //通过hiveContext读取json日志，分析log，提取出accesstoken和栏目id
    getDataFrame(version, hiveContext, "day", begin, year, columnInfoMap)
    columnDetail(sparkSession, columnInfoMap, begin)

    println(time)

  }

  case class Column(f_date: String, f_hour: String, f_timerange: String, f_userid: String, f_terminal: String,
                    f_region_id: String, label: String, f_pv: Int)

  //key_word like '%ad/get_list%' or
  //  key_word like '%search/search_by_keyword%'
  def getDataFrame(version: Boolean, hiveContext: HiveContext, types: String, begin: String, year: String, column: util.HashMap[Long, String]): Unit = {
    hiveContext.sql(s"use ${Constant.HIVE_DB}")
    import hiveContext.implicits._
    var lines = hiveContext.emptyDataFrame
    if (version) { //使用NGINX日志 , 提取出accesstoken和栏目id
      val lines_base = hiveContext.sql(
        s"""
           |select report_time,params['accesstoken'] as accesstoken,
           |params['label'] as labels,
           |day
           |from orc_nginx_log
           |where day=$begin and params is not null and key_word like '%program/get_list%'
        """.stripMargin).withColumn("label", explode(split(col("labels"), "\\|")))
      lines = lines_base.map(x => {
        val report_time = x.getAs[String]("report_time") //2018-05-04 08:51:26
        val dateTime = DateUtils.dateStrToDateTime(report_time)
        val date = x.getAs[String]("day")
        val hour = dateTime.getHourOfDay
        val minute = dateTime.getMinuteOfHour
        val f_timerange = getTimeRangeByMinute(minute)
        val accesstoken = x.getAs[String]("accesstoken")
        val keyarr = TokenParser.parserAsUserNew(accesstoken)
        //val arr = Token.parser(accesstoken)
        val f_userid = keyarr.DA.toString
        val deviceId = keyarr.device_id
        val f_terminal = keyarr.device_type
        val f_province_id = keyarr.province_id
        val f_city_id = keyarr.city_id
        val f_region_id = keyarr.region_id
        val label = x.getAs[String]("label")
        val key = date + "," + hour + "," + f_timerange + "," + f_userid + "," + f_terminal + "," + f_province_id + "," + f_city_id + "," + f_region_id + "," + label
        (key, 1)
      }).filter(x => {
        !"null".equals(x._1.split(",")(4)) && !"0".equals(x._1.split(",")(4)) && !"".equals(x._1.split(",")(4))
      }).reduceByKey(_ + _).map(x => {
        val arr = x._1.split(",")
        val f_date = arr(0)
        val f_hour = arr(1)
        val f_timerange = arr(2)
        val f_userid = arr(3)
        val f_terminal = arr(4)
        val f_province_id = arr(5)
        val f_city_id = arr(6)
        val f_region_id = arr(7)
        val label = arr(8)
        val f_pv = x._2
        Column(f_date, f_hour, f_timerange, f_userid, f_terminal, f_region_id, label, f_pv)
      }).filter(x => x.f_userid != "0").toDF()
    } else {
      hiveContext.sql(s"use ${Constant.HIVE_DB}")
      lines = hiveContext.sql(
        s"""
           |select '$begin' as f_date,
           |t.f_hour,t.f_timerange,t.f_userid,t.f_region_id,
           |t.f_terminal,t.label,cast(count(*) as int) as f_pv
           |from
           |(select
           |hour(reporttime) as f_hour,
           |(case when minute(reporttime)>30 then '60' else '30' end) as f_timerange,
           |userid as f_userid,regionid as f_region_id,
           |devicetype as f_terminal,exts['ID'] as label
           |from
           |orc_report_behavior
           |where day  = '$begin'
           |and reporttype = 131
           |and exts['ID'] is not null
           |and exts['A']=26) t
           |group by t.f_hour,
           |t.f_timerange,
           |t.f_region_id,t.f_terminal,
           |t.f_userid,t.f_terminal,t.label
   """.stripMargin)
    }
    val columnsql = "( select distinct f_column_id ,f_parent_id, f_column_name  from t_column_info ) as column_info"
    //    val provincesql = "(SELECT province_id as f_province_id,province_name as f_province_name from province ) as aa"
    //    val citysql = s"(SELECT city_id as f_city_id,city_name as f_city_name from city ) as aa"
    //    val regionsql = "(SELECT area_id as f_region_id,area_name as f_region_name from area ) as aa "
    //    val provinceDF = DBUtils.loadMysql(hiveContext, provincesql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()
    //    val cityDF = DBUtils.loadMysql(hiveContext, citysql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()
    //    val regionDF = DBUtils.loadMysql(hiveContext, regionsql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()
    val columnInfoDF = DBUtils.loadMysql(hiveContext, columnsql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    val regionCode = RegionUtils.getRootRegion
    val region = getRegionInfo(hiveContext: HiveContext, regionCode: String)
    val result = columnInfoDF.join(lines, lines("label") === columnInfoDF("f_column_id"))
      .join(region, Seq("f_region_id"))
      .drop("label")
      .drop("f_parent_id")
      .drop("f_column_name")
    var maxColumnLevel = 0
    val column_info = result.map(x => {
      val column_id = x.getAs[Long]("f_column_id")
      val column_info = getColumnRoot(column_id, column) //将栏目信息构成listBuffer
      var f_column_id: Long = 0L //解析listBuffer  将栏目id,栏目名字及父栏目信息读出
      var f_column_name: String = ""
      var f_parent_column_id: Long = 0L
      var f_parent_column_name: String = ""
      var f_parent_parent_column_id: Long = 0L
      var f_parent_parent_column_name: String = ""
      val f_column_level = column_info.length - 1 //得出栏目的层级
      if (f_column_level > maxColumnLevel) {
        maxColumnLevel = f_column_level
      }
      var j = 0
      for (tuple <- column_info) {
        if (j == 0) {
          f_column_id = tuple._1
          f_column_name = tuple._2
        } else if (j == 1) {
          f_parent_column_id = tuple._1
          f_parent_column_name = tuple._2
        } else if (j == 2) {
          f_parent_parent_column_id = tuple._1
          f_parent_parent_column_name = tuple._2
        }
        j += 1
      }
      Column_info(f_column_level, f_column_id, f_column_name, f_parent_column_id, f_parent_column_name,
        f_parent_parent_column_id, f_parent_parent_column_name) //返回栏目的层级信息
    }).toDF().distinct()

    val column_count_info = column_info.join(result, Seq("f_column_id"))
    var base_df1 = column_count_info
    //按天统计
    var i = 4
    while (i > 1) {
      base_df1 = getAllData(base_df1, column, hiveContext, i)
      i -= 1
    }
    //按半小时统计
    groupByHalfHour(base_df1, hiveContext)
    val base_df = base_df1.groupBy(col("f_column_id"), col("f_column_level"), col("f_column_name"),
      col("f_parent_column_id"), col("f_parent_column_name"), col("f_parent_parent_column_id"),
      col("f_parent_parent_column_name"), col("f_region_id"), col("f_city_id"), col("f_province_id"),
      col("f_date"), col("f_userid"), col("f_terminal"), col("f_province_name"), col("f_city_name"),
      col("f_region_name")).sum("f_pv")
      .selectExpr("f_column_id", "f_column_level", "f_column_name",
        "f_parent_column_id", "f_parent_column_name", "f_parent_parent_column_id",
        "f_parent_parent_column_name", "f_region_id", "f_city_id", "f_province_id", "f_date", "f_userid", "f_terminal",
        "f_province_name", "f_city_name", "f_region_name", "`sum(f_pv)` as f_pv")

    DBUtils.saveDataFrameToPhoenixNew(base_df, "t_user_column_base") //将数据保存到hbase

  }

  def getAllData(base_df: DataFrame, column: util.HashMap[Long, String], hiveContext: HiveContext, i: Int): DataFrame = {
    import hiveContext.implicits._
    //    var arr: util.HashSet[Long] = new util.HashSet[Long]()
    //    base_df.select("f_column_id").distinct().collect().map(x => {
    //      val column_id = x.getAs[Long]("f_column_id")
    //      arr.add(column_id)
    //    })
    val base_df3 = base_df.filter(col("f_column_level") === i).map(x => {
      val f_region_id = x.getAs[String]("f_region_id")
      val f_city_id = x.getAs[String]("f_city_id")
      val f_province_id = x.getAs[String]("f_province_id")
      val f_date = x.getAs[String]("f_date")
      val f_hour = x.getAs[String]("f_hour")
      val f_timerange = x.getAs[String]("f_timerange")
      val f_userid = x.getAs[String]("f_userid")
      val f_terminal = x.getAs[String]("f_terminal")
      val f_pv = x.getAs[Integer]("f_pv")
      val f_province_name = x.getAs[String]("f_province_name")
      val f_city_name = x.getAs[String]("f_city_name")
      val f_region_name = x.getAs[String]("f_region_name")
      val column_id = x.getAs[Long]("f_parent_column_id")
      //      if (!arr.contains(column_id)) {
      //        arr.add(column_id)
      val column_info = getColumnRoot(column_id, column)
      var f_column_id: Long = 0L //解析listBuffer  将栏目id,栏目名字及父栏目信息读出
      var f_column_name: String = ""
      var f_parent_column_id: Long = 0L
      var f_parent_column_name: String = ""
      var f_parent_parent_column_id: Long = 0L
      var f_parent_parent_column_name: String = ""
      val f_column_level = column_info.length - 1 //得出栏目的层级
      var j = 0
      for (tuple <- column_info) {
        if (j == 0) {
          f_column_id = tuple._1
          f_column_name = tuple._2
        } else if (j == 1) {
          f_parent_column_id = tuple._1
          f_parent_column_name = tuple._2
        } else if (j == 2) {
          f_parent_parent_column_id = tuple._1
          f_parent_parent_column_name = tuple._2
        }
        j += 1
      }
      newLog1(f_column_id, f_column_level, f_column_name, f_parent_column_id, f_parent_column_name, f_parent_parent_column_id, f_parent_parent_column_name, f_region_id,
        f_date, f_hour, f_timerange, f_userid, f_terminal, f_pv, f_province_id, f_province_name, f_city_id, f_city_name, f_region_name)
      //      } else {
      //        newLog1()
      //      }
    }
    ).filter(x => x.f_column_id != 0L).toDF().unionAll(base_df)
    base_df3
  }

  /** *
    * 按天
    *
    * @param column_count_info
    * @param types day
    */
  def groupByDay(begin: String, base_df: DataFrame, hiveContext: HiveContext) = {
    base_df.filter(col("f_date") === begin).registerTempTable("temp_day")
    val df = hiveContext.sql(
      s"""
         |select
         |f_date,
         |cast(f_terminal as tinyint) as f_terminal,
         |f_province_id,f_province_name,
         |f_city_id,f_city_name,
         |f_region_id,f_region_name,
         |f_column_id,f_column_name,
         |f_parent_column_id,f_parent_column_name,
         |f_parent_parent_column_id,f_parent_parent_column_name,
         |f_column_level,
         |sum(f_pv) as f_click_count,
         |count(distinct f_userid) as f_user_count,
         |sum(if(f_pv>=2,1,0)) as f_active_user_count
         |from temp_day
         |group by
         |f_column_id,f_column_level,f_column_name,f_parent_column_id,f_parent_column_name,f_parent_parent_column_id,f_parent_parent_column_name,
         |f_region_id,f_region_name,f_city_id,f_city_name,f_province_id,f_province_name,f_date,f_terminal
      """.stripMargin).filter(col("f_click_count").isNotNull).registerTempTable("temp_day_new")

    val df_new = hiveContext.sql(
      """
        |select
        |f_date,
        |cast(f_terminal as tinyint) as f_terminal,
        |f_province_id,f_province_name,
        |f_city_id,f_city_name,
        |f_region_id,f_region_name,
        |f_column_id,f_column_name,
        |f_parent_column_id,f_parent_column_name,
        |f_parent_parent_column_id,f_parent_parent_column_name,
        |f_column_level,
        |f_click_count,
        |f_user_count,
        |if(f_active_user_count > f_user_count , f_user_count , f_active_user_count ) as f_active_user_count
        |from temp_day_new
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(df_new, "t_column_report_by_day")
  }

  /** *
    * 按半小时
    *
    * @param column_count_info
    * @param types day
    */
  def groupByHalfHour(column_count_info: DataFrame, hiveContext: HiveContext) = {
    column_count_info.registerTempTable("temp_half")
    //按半小时维度
    val halfHour_df = hiveContext.sql(
      """
        |select
        |f_date,
        |f_hour,
        |cast(f_timerange as tinyint) as f_timerange,
        |cast(f_terminal as tinyint) as f_terminal,
        |f_province_id,f_province_name,
        |f_city_id,f_city_name,
        |f_region_id,f_region_name,
        |f_column_id,f_column_name,
        |f_parent_column_id,f_parent_column_name,
        |f_parent_parent_column_id,f_parent_parent_column_name,
        |f_column_level,
        |sum(f_pv) as f_click_count
        |from temp_half
        |group by
        |f_date,f_hour,f_timerange,f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name,f_terminal,
        |f_column_id,f_column_name,f_parent_column_id,f_parent_column_name,f_parent_parent_column_id,f_parent_parent_column_name,f_column_level
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(halfHour_df, "t_column_report_by_halfhour")
  }

  /** *
    * 读取dtvs数据库，通过栏目id找到parent_id,column_index,label_name字段信息
    * 读取iusm数据库，通过access_token去找DA,再通过DA去找到用户的基础信息
    *
    * @param detailsDF
    * @param sparkSession
    * @param column
    */
  def columnDetail(sparkSession: SparkSession, column: util.HashMap[Long, String], begin: String): Unit = {
    val sqlContext = sparkSession.sqlContext
    //周
    val week = MOEDateUtils.getWeekOfYear(begin).toString
    //月
    val month = MOEDateUtils.getMonth(begin).toString
    //季度
    val quarter = MOEDateUtils.getSeason(begin).toString
    //年
    val year = MOEDateUtils.getYear(begin).toString
    val weekStartTime = MOEDateUtils.getFirstOfWeek(begin)
    val monthStartTime = MOEDateUtils.getFirstDayOfMonth(year.toInt, begin)
    val latestDay = DateUtils.getNDaysBefore(6, begin)
    val arr = Array(weekStartTime, monthStartTime, latestDay, begin).sorted
    val smallDay = arr(0)
    val sql = s"(select * from t_user_column_base where f_date>='$smallDay' and f_date<= '$begin') as a"

    val base_df = DBUtils.loadDataFromPhoenix2(sqlContext, sql)
    base_df.persist(StorageLevel.DISK_ONLY)
    //    groupByCategory(begin,base_df, "year", sparkSession, begin, year, "t_column_report_by_year")
    //    groupByCategory(begin, base_df, "quarter", sparkSession, quarter, "t_column_report_by_quarter")
    groupByCategory(begin, base_df, "month", sparkSession, month, year, "t_column_report_by_month")
    groupByCategory(begin, base_df, "week", sparkSession, week, year, "t_column_report_by_week")
    groupByDay(begin, base_df, sqlContext)
    groupBylatestDay(base_df, sparkSession, begin)
    base_df.unpersist()
  }

  /** *
    * 按近七天
    *
    * @param column_count_info
    * @param types day
    */
  def groupBylatestDay(base_df: DataFrame, sparkSession: SparkSession, begin: String) = {
    val spark = sparkSession.sqlContext
    val latestDay = DateUtils.getNDaysBefore(6, begin)
    base_df.filter(col("f_date") >= latestDay and col("f_date") <= begin).registerTempTable("temp_latest")
    //近七天
    val latest_day_df = spark.sql(
      s"""
         |select
         |cast($latestDay as string) as f_start_date,
         |cast($begin as string) as f_end_date,
         |cast(f_terminal as tinyint) as f_terminal,
         |f_province_id,f_province_name,
         |f_city_id,f_city_name,
         |f_region_id,f_region_name,
         |f_column_id,f_column_name,
         |f_parent_column_id,f_parent_column_name,
         |f_parent_parent_column_id,f_parent_parent_column_name,
         |f_column_level,
         |sum(f_pv) as f_click_count,
         |count(distinct f_userid) as f_user_count,
         |sum(if(f_pv>=2,1,0)) as f_active_user_count
         |from(
         |select
         |sum(f_pv) as f_pv,
         |f_column_id,f_column_level,f_column_name,f_parent_column_id,f_parent_column_name,f_parent_parent_column_id,f_parent_parent_column_name,
         |f_region_id,f_region_name,f_city_id,f_city_name,f_province_id,f_province_name,
         |f_terminal,f_userid
         |from temp_latest
         |group by
         |f_column_id,f_column_level,f_column_name,f_parent_column_id,f_parent_column_name,f_parent_parent_column_id,f_parent_parent_column_name,
         |f_region_id,f_region_name,f_city_id,f_city_name,f_province_id,f_province_name,
         |f_terminal,f_userid
         |)t
         |group by
         |f_column_id,f_column_level,f_column_name,f_parent_column_id,f_parent_column_name,f_parent_parent_column_id,f_parent_parent_column_name,
         |f_region_id,f_region_name,f_city_id,f_city_name,f_province_id,f_province_name,
         |f_terminal
      """.stripMargin).filter(col("f_click_count").isNotNull).registerTempTable("temp_latest_new")

    val latest_day_df_new = spark.sql(
      """
        |select
        |f_start_date,
        |f_end_date,
        |cast(f_terminal as tinyint) as f_terminal,
        |f_province_id,f_province_name,
        |f_city_id,f_city_name,
        |f_region_id,f_region_name,
        |f_column_id,f_column_name,
        |f_parent_column_id,f_parent_column_name,
        |f_parent_parent_column_id,f_parent_parent_column_name,
        |f_column_level,
        |f_click_count,
        |f_user_count,
        |if(f_active_user_count > f_user_count , f_user_count , f_active_user_count ) as f_active_user_count
        |from temp_latest_new
      """.stripMargin)
    //    latest_day_df.printSchema()
    DBUtils.saveDataFrameToPhoenixNew(latest_day_df_new, "t_column_report_by_history")
  }

  /** *
    * 根据类别进行统计
    *
    * @param column_count_info
    * @param types 【day,week,month,quarter,year】
    * @param sparkSession
    */
  def groupByCategory(date: String, base_df: DataFrame, types: String, sparkSession: SparkSession, begin: String, year: String, tableName: String) = {
    val spark = sparkSession.sqlContext
    var starttime = ""
    var endtime = ""
    var f_date = ""
    if (types.equals("week")) {
      starttime = MOEDateUtils.getFirstOfWeek(date)
      endtime = MOEDateUtils.getLastOfWeek(date)
      f_date = starttime
    } else if (types.equals("month")) {
      starttime = MOEDateUtils.getFirstDayOfMonth(year.toInt, date)
      endtime = MOEDateUtils.getLastDayOfMonth(year.toInt, date)
      f_date = starttime.substring(0, 6)
    } else if (types.equals("year")) {
      starttime = MOEDateUtils.getFirstDayOfYear(year.toInt)
      endtime = MOEDateUtils.getLastDayOfYear(year.toInt)
      f_date = year
    }
    base_df.filter(col("f_date") >= starttime and col("f_date") <= endtime).registerTempTable("temp")

    val df = spark.sql(
      s"""
         |select
         |cast($f_date as string) as f_date,
         |cast(f_terminal as tinyint) as f_terminal,
         |f_province_id,f_province_name,
         |f_city_id,f_city_name,
         |f_region_id,f_region_name,
         |f_column_id,f_column_name,
         |f_parent_column_id,f_parent_column_name,
         |f_parent_parent_column_id,f_parent_parent_column_name,
         |f_column_level,
         |sum(f_pv) as f_click_count,
         |count(distinct f_userid) as f_user_count,
         |sum(if(f_pv>=2,1,0)) as f_active_user_count
         |from(
         |select
         |sum(f_pv) as f_pv,
         |f_column_id,f_column_level,f_column_name,f_parent_column_id,f_parent_column_name,f_parent_parent_column_id,f_parent_parent_column_name,
         |f_region_id,f_region_name,f_city_id,f_city_name,f_province_id,f_province_name,
         |f_terminal,f_userid
         |from temp
         |group by
         |f_column_id,f_column_level,f_column_name,f_parent_column_id,f_parent_column_name,f_parent_parent_column_id,f_parent_parent_column_name,
         |f_region_id,f_region_name,f_city_id,f_city_name,f_province_id,f_province_name,
         |f_terminal,f_userid
         |)t
         |group by
         |f_column_id,f_column_level,f_column_name,f_parent_column_id,f_parent_column_name,f_parent_parent_column_id,f_parent_parent_column_name,
         |f_region_id,f_region_name,f_city_id,f_city_name,f_province_id,f_province_name,
         |f_terminal
          """.stripMargin).filter(col("f_click_count").isNotNull).registerTempTable("temp_new")

    val df_new_categroy = spark.sql(
      """
        |select
        |f_date,
        |cast(f_terminal as tinyint) as f_terminal,
        |f_province_id,f_province_name,
        |f_city_id,f_city_name,
        |f_region_id,f_region_name,
        |f_column_id,f_column_name,
        |f_parent_column_id,f_parent_column_name,
        |f_parent_parent_column_id,f_parent_parent_column_name,
        |f_column_level,
        |f_click_count,
        |f_user_count,
        |if(f_active_user_count > f_user_count , f_user_count , f_active_user_count ) as f_active_user_count
        |from temp_new
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(df_new_categroy, tableName)
  }

  /** *
    * 将一个小时分成2块
    * 0-30分钟 为30
    * 30-60分钟为60
    *
    * @param minute
    * @return
    */
  def getTimeRangeByMinute(minute: Int): String = {
    var timeRange = "00-30"
    minute match {
      case _ if (minute >= 30) => timeRange = "60"
      case _ => timeRange = "30"
    }
    timeRange
  }

  /** *
    * 读取栏目信息表column_info
    * 将column_id  column_name和parent_id构成一个map结构
    *
    * @param hiveContext
    * @return
    */
  def toParent(hiveContext: HiveContext): util.HashMap[Long, String] = {
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

  /** *
    * 根据一个column_id确定是第几级栏目，找到该栏目的父栏目信息
    *
    * @param colId
    * @param columnInfoMap
    * @return
    */
  def getColumnRoot(colId: Long, columnInfoMap: util.HashMap[Long, String]) = {
    var mapId = colId
    var find = false
    val listBuffer = ListBuffer[(Long, String)]()
    val tmplist = Constant.COLUMN_ROOT
    while (!find && columnInfoMap.keySet().contains(mapId)) {
      val keyInfo = columnInfoMap.get(mapId).split(",")
      val keyId = keyInfo(0) //f_parent_id
      var keyName = keyInfo(1) //f_column_name
      if (tmplist.contains(keyId)) {
        find = true
        keyName = keyInfo(1)
      }
      listBuffer += ((mapId, keyName))
      mapId = columnInfoMap.get(mapId).split(",")(0).toLong
    }
    listBuffer
  }

  def getRegionInfo(hiveContext: HiveContext, regionCode: String) = {
    val sql1 =
      s"""
         |(SELECT province_id as f_province_id,province_name as f_province_name
         |from province
          ) as aa
     """.stripMargin
    val provinceInfoDF = DBUtils.loadMysql(hiveContext, sql1, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val sql2 =
      s"""
         |(SELECT province_id,city_id as f_city_id,city_name as f_city_name
         |from city where province_id='$regionCode' or city_id='$regionCode'
          ) as aa
     """.stripMargin
    val cityInfoDF2 = DBUtils.loadMysql(hiveContext, sql2, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val sql3 =
      s"""
         |(SELECT city_id,area_id as f_region_id,area_name as f_region_name
         |from area
          ) as aa
     """.stripMargin
    val areaInfoDF3 = DBUtils.loadMysql(hiveContext, sql3, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    //addressInfoDF.registerTempTable("address")
    provinceInfoDF.registerTempTable("province")
    cityInfoDF2.registerTempTable("city")
    areaInfoDF3.registerTempTable("region")
    val basic_df1 = hiveContext.sql(
      s"""
         |select cast(a.f_province_id as string) as f_province_id,a.f_province_name,
         |cast(b.f_city_id as string) as f_city_id,b.f_city_name,c.f_region_id,c.f_region_name
         |from province a
         |join city b on a.f_province_id = b.province_id
         |join region c on b.f_city_id = c.city_id
          """.stripMargin)
    val basic_df = basic_df1.distinct()
    basic_df
  }
}
