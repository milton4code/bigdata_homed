package cn.ipanel.rank.reports

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}
import java.text.SimpleDateFormat

import cn.ipanel.common.{CluserProperties, DBProperties, SparkSession}
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable
import scala.collection.mutable.HashMap

object LabelToMqsql {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("LabelToMqsql")
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    val date = args(0)
    val startDate = DateUtils.getDateByDays(date, 7)

    //用户观影时长
    val usermap1 = new mutable.HashMap[String, Long]()
    val usermap2 = new mutable.HashMap[String, Long]()
    //一天在线时长
    val userDurationMap = getDuration(usermap1, date)
    //历史在线时长
    val userHistoryDurationMap = getHistoryDuration(usermap2)

    val useDurationMap = userDurationMap ++ userHistoryDurationMap
      .map(t => t._1 -> (t._2 + userDurationMap.getOrElse(t._1, 0L)))

    usermap1.clear()
    usermap2.clear()

    //历史开机次数  一天观看次数
    val userTimeMap = getOneDayTimes(usermap1, date)
    //历史观看次数
    val userHistoryTimeMap = getHistoryTimes(usermap2)

    val useTimesMap = userTimeMap ++ userHistoryTimeMap
      .map(t => t._1 -> (t._2 + userTimeMap.getOrElse(t._1, 0L)))

    usermap1.clear()
    usermap2.clear()

    //先删表
    delMysql("t_user_profile_label")
    getDurationToMysql(useDurationMap)
    updateTimeMapToMysql(useTimesMap, "times", "t_user_profile_label")
    val map = new mutable.HashMap[String, String]()
    //最活跃时段
    //val userTimerange = getTimeRange(map, startDate, date)
    // updateMapToMysql(userTimerange, "behavioractivesection", "t_user_profile_label")
    // map.clear()
    //节目
    val userProgram = getSeries(map, startDate, date)
     updateMapToMysql(userProgram, "mediainteresseries", "t_user_profile_label")
     map.clear()
    //栏目
      val userColumn = getColumn(map, startDate, date)
      updateMapToMysql(userColumn, "mediainteresprogramtype", "t_user_profile_label")
      map.clear()
    //消费分群
    //val billingDate = DateUtils.getDateByDays(date, -1)
    // val billingMap = getBilling(map, startDate, billingDate)
    // updateMapToMysql(billingMap, "behaviortenant_group", "t_user_profile_label")
    // map.clear()
    //最喜欢媒资类型
      val mediaMap = getMediaMap()
     val mediauser = getContentSub(sqlContext, startDate, date, mediaMap)
     updateDFToMysql(mediauser, "media", "t_user_profile_label")
    //活跃终端类型
    // val terminalMap = getTerminal(map, startDate, date)
    // updateMapToMysql(terminalMap, "behaviorfavorite_terminal", "t_user_profile_label")
    //  map.clear()

    //国家
    // val countryDf = getCountry(sqlContext: HiveContext, startDate: String, date: String)
    // updateDFToMysql(countryDf, "meidainterestcountry", "t_user_profile_label")
    //按月
    //    val firstMonthDay = DateUtils.getFirstDateOfMonth(date: String)
    //    val endMonthDay = DateUtils.getMonthEnd(date: String)
    //    val pattern = "yyyyMMdd"
    //    val firstMonthDayOfDate = new SimpleDateFormat(pattern).parse(firstMonthDay)
    //    val endMonthDayOfDate = new SimpleDateFormat(pattern).parse(endMonthDay)
    //    val between = endMonthDayOfDate.getTime - firstMonthDayOfDate.getTime
    //    //    时间差
    //    val month = date.substring(0, 6)
    //    if (date == endMonthDay) {
    //      val month = date.substring(0, 6)
    //      // 在线时长
    //      delMysql("t_user_profile_label_by_month")
    //      val userOntime = getMonthOnTime(month, map, firstMonthDay, endMonthDay)
    //      getOnTimeToMysql(month, userOntime, between)
    //      map.clear()
    //      //      // 内容类型  点播次数占比
    //      val contentMonth = getContentMonth(sqlContext: HiveContext, firstMonthDay: String, date: String)
    //      updateDFToMysql(contentMonth, "contenttype_ratio", "t_user_profile_label_by_month")
    //      //消费总金额 消费总次数
    //      val billingMonthDate = DateUtils.getDateByDays(endMonthDay, -1)
    //      val billingMapMonth = getBillingMonth(map, firstMonthDay, billingMonthDate)
    //      updateMapToMysql(billingMapMonth, "monetary", "t_user_profile_label_by_month")
    //      map.clear()
    //      val billingMapCount = getBillingCount(map, firstMonthDay, billingMonthDate)
    //      updateMapToMysql(billingMapCount, "frenquency", "t_user_profile_label_by_month")
    //      map.clear()
    //    }
  }

  def updateTimeMapToMysql(map: mutable.Map[String, Long], columnName: String, tableName: String) = {
    for ((k, v) <- map) {
      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection(DBProperties.JDBC_URL_PROFILE, DBProperties.USER_PROFILE, DBProperties.PASSWORD_PROFILE)
      val ps = connection.prepareStatement(s"update $tableName set $columnName='$v' where f_user_id = '$k' ;")
      ps.execute()
    }
  }


  def getOneDayTimes(map: mutable.HashMap[String, Long], date: String) = {
    var conn: Connection = null
    var st: Statement = null
    var rs: ResultSet = null
    try {
      val url = CluserProperties.PHOENIX_ZKURL
      conn = DriverManager.getConnection(url)
      st = conn.createStatement()
      rs = st.executeQuery(
        s"""
           | select f_user_id
           | from t_user_watch_detail
           | where length(f_user_id)=8
           | and f_date = '$date'
           |  group by f_user_id
        """.stripMargin)
      while (rs.next()) {
        val k = rs.getString("f_user_id")
        val v = 1L
        map.put(k, v)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      rs.close
      st.close
      conn.close()
    }
    map
  }


  import cn.ipanel.common.DBProperties

  def getHistoryTimes(historyMap: mutable.HashMap[String, Long]) = {
    val map = HashMap[String, Long]()
    val sql = "select f_user_id,duration,times from `t_user_profile_label`"
    var conn: Connection = null
    var st: Statement = null
    var rs: ResultSet = null
    try {
      conn = getConn(
        DBProperties.JDBC_URL_PROFILE,
        DBProperties.USER_PROFILE,
        DBProperties.PASSWORD_PROFILE)
      st = conn.createStatement()
      rs = st.executeQuery(sql)
      while (rs.next()) {
        val f_user_id = rs.getString("f_user_id")
        val duration = rs.getLong("duration")
        val times = rs.getLong("times")
        //println(package_id+"-->"+package_name)
        map.put(f_user_id, times)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      this.close(conn, rs, st, null)
    }
    map
  }

  def getHistoryDuration(historyMap: mutable.HashMap[String, Long]) = {
    val map = HashMap[String, Long]()
    val sql = "select f_user_id,duration,times from `t_user_profile_label`"
    var conn: Connection = null
    var st: Statement = null
    var rs: ResultSet = null
    try {
      conn = getConn(
        DBProperties.JDBC_URL_PROFILE,
        DBProperties.USER_PROFILE,
        DBProperties.PASSWORD_PROFILE)
      st = conn.createStatement()
      rs = st.executeQuery(sql)
      while (rs.next()) {
        val f_user_id = rs.getString("f_user_id")
        val duration = rs.getLong("duration")
        val times = rs.getLong("times")
        //println(package_id+"-->"+package_name)
        map.put(f_user_id, duration)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      this.close(conn, rs, st, null)
    }
    map
  }


  def getMediaTag(sqlContext: HiveContext) = {
    val sql =
      """
        |(select f_tag_id,f_tag_name
        |from t_media_tag_info) as aa
      """.stripMargin
    DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
  }


  def getMediaMap() = {
    val map = HashMap[Long, String]()
    val sql = "select f_tag_id ,f_tag_name from t_media_tag_info"
    var conn: Connection = null
    var st: Statement = null
    var rs: ResultSet = null
    try {
      conn = getConn(DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
      st = conn.createStatement()
      rs = st.executeQuery(sql)
      while (rs.next()) {
        val f_tag_id = rs.getLong("f_tag_id")
        val f_tag_name = rs.getString("f_tag_name")
        map.put(f_tag_id, f_tag_name)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      close(conn, rs, st, null)
    }
    map
  }

  /**
    * 根据参数获取数据库的连接
    *
    * @param url
    * @param user
    * @param pass
    * @return
    */
  def getConn(url: String, user: String, pass: String): Connection = {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    DriverManager.getConnection(url, user, pass)
  }

  /**
    * 关闭数据库资源
    *
    * @param conn
    * @param resultSet
    * @param statement
    * @param preparedStatement
    */
  def close(conn: Connection, resultSet: ResultSet, statement: Statement, preparedStatement: PreparedStatement) = {
    try {
      if (resultSet != null && !resultSet.isClosed) {
        resultSet.close()
      }
      if (statement != null && !statement.isClosed) {
        statement.close()
      }
      if (preparedStatement != null && !preparedStatement.isClosed) {
        preparedStatement.close()
      }
      if (conn != null && !conn.isClosed) {
        conn.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def getBillingMonth(map: mutable.HashMap[String, String], firstMonthDay: String, billingMonthDate: String) = {
    val dateNew = DateUtils.transformDateStr(billingMonthDate)
    val dateStartNew = DateUtils.transformDateStr(firstMonthDay)
    val sql =
      s"""
         |select f_user_id as k,sum(f_money)/100  as v
         |from t_billing
         |where f_time between '$dateStartNew' and '$dateNew'
         |and length(f_user_id)=8 and f_money !=0
         |group by f_user_id
        """.stripMargin
    dateFromPhoniex(map: mutable.HashMap[String, String], sql)

  }


  def getBillingCount(map: mutable.HashMap[String, String], firstMonthDay: String, billingMonthDate: String) = {
    val dateNew = DateUtils.transformDateStr(billingMonthDate)
    val dateStartNew = DateUtils.transformDateStr(firstMonthDay)
    val sql =
      s"""
         |select f_user_id as k,count(*)  as v
         |from t_billing
         |where f_time between '$dateStartNew' and '$dateNew'
         |and length(f_user_id)=8 and f_money !=0
         |group by f_user_id
        """.stripMargin
    dateFromPhoniex(map: mutable.HashMap[String, String], sql)

  }


  def getCountry(sqlContext: HiveContext, startDate: String, date: String) = {
    val sql =
      s"""
         |(select f_user_id,f_country,f_play_time from t_country_terminal_time
         | where f_date between '$startDate' and '$date' and length(f_user_id)=8
         | and f_play_time !=0 )
         | as aa
      """.stripMargin
    DBUtils.loadDataFromPhoenix2(sqlContext, sql).registerTempTable("country")
    val countryDf = sqlContext.sql(
      """
        |select f_user_id as k,f_country as v,f_play_time,rank from
        |(select a.*,ROW_NUMBER() OVER (PARTITION BY a.f_user_id ORDER BY a.f_play_time DESC ) as rank
        |from
        |(select f_user_id,f_country,sum(f_play_time) as f_play_time
        |from country
        |group by f_user_id,f_country) a) t
        |where t.rank<=1
      """.stripMargin)
    countryDf
  }

  def getTerminal(map: mutable.HashMap[String, String], startDate: String, date: String) = {
    val terminalsql =
      s"""
         |select a.f_user_id as k,a.f_terminal as v,a.f_play_time_new from
         |(select f_user_id,f_terminal,sum(f_play_time) as f_play_time_new from t_user_time
         | where f_date between '$startDate' and '$date' and length(f_user_id)=8
         | group by f_user_id,f_terminal) a
         |where
         |a.f_play_time_new =
         |(select max(b.f_play_time1)
         |from
         |(select f_user_id,f_terminal,sum(f_play_time) as f_play_time1 from t_user_time
         | where f_date between '$startDate' and '$date' and length(f_user_id)=8
         | group by f_user_id,f_terminal) b
         |where b.f_user_id = a.f_user_id )
            """.stripMargin
    dateFromPhoniex(map: mutable.HashMap[String, String], terminalsql: String)
  }

  def getColumn(map: mutable.HashMap[String, String], startDate: String, date: String) = {
    var conn: Connection = null
    val sql =
      s"""
         |select a.f_user_id as k,a.f_column_id as v,a.f_play_time_new from
         |(select f_user_id,f_column_id,sum(f_play_time) as f_play_time_new from t_column_info
         | where f_date between '$startDate' and '$date' and length(f_user_id)=8
         | group by f_user_id,f_column_id) a
         |where
         |a.f_play_time_new =
         |(select max(b.f_play_time1)
         |from
         |(select f_user_id,f_column_id,sum(f_play_time) as f_play_time1 from t_column_info
         | where f_date between '$startDate' and '$date' and length(f_user_id)=8
         | group by f_user_id,f_column_id) b
         |where b.f_user_id = a.f_user_id )
            """.stripMargin
    dateFromPhoniex(map: mutable.HashMap[String, String], sql)
  }

  //    def getSeries1(sqlContext: HiveContext, startDate: String, date: String) = {
  //      val sql =
  //        s"""
  //           |(select f_user_id,f_program_id,f_play_time from t_user_watch_video_detail
  //           | where f_date between '$startDate' and '$date' and length(f_user_id)=8
  //           | and f_play_time !=0 )
  //           | as aa
  //        """.stripMargin
  //      DBUtils.loadDataFromPhoenix2(sqlContext, sql).registerTempTable("series")
  //      val series = sqlContext.sql(
  //        """
  //          |select t.f_user_id as k,concat_ws('|',collect_list(t.f_program_id)) as v
  //          |from
  //          |(
  //          |select a.*,ROW_NUMBER() OVER (PARTITION BY a.f_user_id ORDER BY a.f_play_time DESC ) as rank
  //          |from
  //          |(select f_user_id,f_program_id,sum(f_play_time) as f_play_time
  //          |from series
  //          |group by f_user_id,f_program_id) a) t
  //          |where t.rank<=3
  //          |group by t.f_user_id
  //        """.stripMargin)
  //      series
  //    }


  def getSeries(map: mutable.HashMap[String, String], startDate: String, date: String) = {
    val sql =
      s"""
         |select a.f_user_id as k,a.f_program_id as v,a.f_play_time_new
         | from
         | (select f_user_id,f_program_id,sum(f_play_time) as f_play_time_new from t_user_watch_video_detail
         | where f_date between '$startDate' and '$date' and length(f_user_id)=8
         |  group by f_user_id,f_program_id )a
         | where
         | a.f_play_time_new =
         | (select max(b.f_play_time)
         | from
         | (select f_user_id,f_program_id,sum(f_play_time) as f_play_time from t_user_watch_video_detail
         | where f_date between '$startDate' and '$date' and length(f_user_id)=8
         |  group by f_user_id,f_program_id) b
         | where b.f_user_id = a.f_user_id )
        """.stripMargin
    dateFromPhoniex(map: mutable.HashMap[String, String], sql)
  }


  def getBilling(map: mutable.HashMap[String, String], startDate: String, date: String) = {
    val dateNew = DateUtils.transformDateStr(date)
    val dateStartNew = DateUtils.transformDateStr(startDate)
    val sql =
      s"""
         |select a.f_user_id as k ,a.f_type as v
         |from
         |(select f_user_id,
         |(case  when  count(*)>2 then '11003001'
         |when (count(*)=1 or count(*)=2) then '11003002'
         |else '11003003' end) as f_type
         |from t_billing
         |where f_time between '$dateStartNew' and '$dateNew'
         |and length(f_user_id)=8 and f_money !=0
         |group by f_user_id) a
        """.stripMargin
    dateFromPhoniex(map: mutable.HashMap[String, String], sql)
  }


  //最喜欢媒资类型
  def getContentMonth(sqlContext: HiveContext, startDate: String, date: String) = {
    val sql =
      s"""
         |(select f_user_id,f_content_type,f_play_time from t_content_type_time
         | where f_date between '$startDate' and '$date' and length(f_user_id)=8
         | and f_play_time !=0 )
         | as aa
      """.stripMargin
    DBUtils.loadDataFromPhoenix2(sqlContext, sql).registerTempTable("media")
    sqlContext.sql(
      """
        |select a.f_user_id as k,concat_ws(',',collect_list(a.f_content)) as v
        |from
        |(select a.f_user_id,
        |concat('{"statistics.contenttype":"',a.f_content_type,
        |'","statistics.time":"',a.f_play_time,
        |'","statistics.ratio":"',round(a.f_play_time/b.f_count_time,6),
        |  '"}' )as f_content
        |from
        |(select f_user_id,f_content_type,sum(f_play_time) as f_play_time
        |from media group by f_user_id,f_content_type) a
        |join
        |(select f_user_id,sum(f_play_time) as f_count_time  from
        |media  group by f_user_id) b
        |on a.f_user_id = b.f_user_id) a
        |group by a.f_user_id
      """.stripMargin)
  }


  def getDuration(map: mutable.HashMap[String, Long], date: String) = {
    var conn: Connection = null
    var st: Statement = null
    var rs: ResultSet = null
    try {
      val url = CluserProperties.PHOENIX_ZKURL
      conn = DriverManager.getConnection(url)
      st = conn.createStatement()
      rs = st.executeQuery(
        s"""
           | select f_user_id,sum(f_play_time)/3600 as f_play_time
           | from t_user_watch_detail
           | where length(f_user_id)=8
           | and f_date = '$date'
           |  group by f_user_id
        """.stripMargin)
      while (rs.next()) {
        val k = rs.getString("f_user_id")
        val v = rs.getLong("f_play_time").toLong
        map.put(k, v)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      rs.close
      st.close
      conn.close()
    }
    map
  }


  def getMonthOnTime(month: String, map: mutable.HashMap[String, String], firstMonthDay: String, endMonthDay: String) = {
    var conn: Connection = null
    var st: Statement = null
    var rs: ResultSet = null
    try {
      val url = CluserProperties.PHOENIX_ZKURL
      conn = DriverManager.getConnection(url)
      st = conn.createStatement()
      rs = st.executeQuery(
        s"""
           | select f_user_id,count(distinct(f_date)) as f_active_count,sum(f_play_time) as f_play_time
           | from t_user_watch_detail
           | where (f_date between '$firstMonthDay' and '$endMonthDay') and length(f_user_id)=8
           |  group by f_user_id
        """.stripMargin)
      while (rs.next()) {
        val k = rs.getString("f_user_id")
        val v = rs.getString("f_active_count") + ":" + rs.getString("f_play_time")
        map.put(k, v)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      rs.close
      st.close
      conn.close()
    }
    map
  }


  //最喜欢媒资类型
  def getContentSub(sqlContext: HiveContext, startDate: String, date: String, mediaMap: HashMap[Long, String]) = {
    val sql =
      s"""
         |(select f_user_id,f_content_type,f_sub_type,f_play_time from t_sub_type_time
         | where f_date between '$startDate' and '$date' and length(f_user_id)=8
         | and f_play_time !=0 )
         | as aa
      """.stripMargin
    import sqlContext.implicits._
    val df = DBUtils.loadDataFromPhoenix2(sqlContext, sql)
      .map(x => {
        val f_user_id = x.getAs[String]("F_USER_ID")
        val f_content_type = x.getAs[String]("F_CONTENT_TYPE")
        val f_content_name = getTypeName(x.getAs[String]("F_CONTENT_TYPE"), mediaMap)
        val f_sub_type = x.getAs[String]("F_SUB_TYPE")
        val f_sub_name = getTypeName(x.getAs[String]("F_SUB_TYPE"), mediaMap)
        val f_play_time = x.getAs[Long]("F_PLAY_TIME")
        (f_user_id, f_content_type, f_content_name, f_sub_type, f_sub_name, f_play_time)
      }).toDF("f_user_id", "f_content_type", "f_content_name", "f_sub_type", "f_sub_name", "f_play_time")
      .registerTempTable("media")
    val mediauser = sqlContext.sql(
      """
        |select ss.f_user_id as k,
        |concat_ws(',',collect_list(ss.f_media)) as v
        |from
        |(select t.f_user_id,
        |concat('{"contenttype":',t.f_content_type,
        |',"contenttype_name":"',t.f_content_name,
        |'","interest":[{"typeid":2,
        |"value":"',
        |concat_ws('|',collect_list(t.f_sub_type)),
        |'","value_name":"',concat_ws('|',collect_list(t.f_sub_name)),
        | '"},{"typeid":3,"value":"',
        | '',
        |  '"},{"typeid":4,"value":"',
        |  '',
        |    '"},{"typeid":5,"value":"',
        |  '',
        |    '"},{"typeid":6,"value":"',
        |     '',
        |            '"},{"typeid":7,"value":"',
        |       '',
        |            '"},{"typeid":8,"value":"',
        |      '"}]}'    ) as f_media
        |from
        |(select a.*,ROW_NUMBER() OVER (PARTITION BY a.f_user_id,a.f_content_type,a.f_content_name ORDER BY a.f_play_time DESC ) as rank
        |from
        |(select f_user_id,f_content_type,f_content_name,f_sub_type,f_sub_name,sum(f_play_time) as f_play_time
        |from media
        |where f_content_name != '' and f_sub_name !=''
        |group by f_user_id,f_content_type,
        |f_content_name,f_sub_type,f_sub_name) a) t
        |where t.rank<=3
        |group  by t.f_user_id,t.f_content_type,t.f_content_name) ss
        |group by ss.f_user_id
      """.stripMargin)
    //    val mediauser = sqlContext.sql(
    //      """
    //        |select ss.f_user_id as k,
    //        |concat_ws(',',collect_list(ss.f_media)) as v
    //        |from
    //        |(select t.f_user_id,
    //        |concat('\{\"contenttype\"\:',t.f_content_type,
    //        |'\,\"interest\"\:\[\{\"typeid\"\:2\,\"value\"\:\"',
    //        |concat_ws('|',collect_list(t.f_sub_type)),
    //        | '\"\}\,\{\"typeid\"\:3\,\"value\"\:\"',
    //        | '',
    //        |  '\"\}\,\{\"typeid\"\:4\,\"value\"\:\"',
    //        |  '',
    //        |    '\"\}\,\{\"typeid\"\:5\,\"value\"\:\"',
    //        |  '',
    //        |    '\"\}\,\{\"typeid\"\:6\,\"value\"\:\"',
    //        |     '',
    //        |            '\"\}\,\{\"typeid\"\:7\,\"value\"\:\"',
    //        |       '',
    //        |            '\"\}\,\{\"typeid\"\:8\,\"value\"\:\"',
    //        |      '\"\}\]\}'    ) as f_media
    //        |from
    //        |(
    //        |select a.*,ROW_NUMBER() OVER (PARTITION BY a.f_user_id,a.f_content_type ORDER BY a.f_play_time DESC ) as rank
    //        |from
    //        |(select f_user_id,f_content_type,f_sub_type,sum(f_play_time) as f_play_time
    //        |from media
    //        |group by f_user_id,f_content_type,f_sub_type) a)t
    //        |where t.rank<=3
    //        |group by t.f_user_id,t.f_content_type) ss
    //        |group by ss.f_user_id
    //      """.stripMargin)
    mediauser
  }

  def getTypeName(f_content_type: String, mediaMap: HashMap[Long, String]) = {
    var name = ""
    name = mediaMap.get(f_content_type.toLong).getOrElse("")
    name
  }

  def getTimeRange(map: mutable.HashMap[String, String], startDate: String, date: String) = {
    val sql =
      s"""
         |select a.f_user_id as k,a.f_hour as v,a.f_play_time_new
         | from
         | (select f_user_id,f_hour,sum(f_play_time) as f_play_time_new from t_timerange_time
         | where f_date between '$startDate' and '$date' and length(f_user_id)=8
         |  group by f_user_id,f_hour )a
         | where
         | a.f_play_time_new =
         | (select max(b.f_play_time)
         | from
         | (select f_user_id,f_hour,sum(f_play_time) as f_play_time from t_timerange_time
         | where f_date between '$startDate' and '$date' and length(f_user_id)=8
         |  group by f_user_id,f_hour) b
         | where b.f_user_id = a.f_user_id )
        """.stripMargin
    dateFromPhoniex(map: mutable.HashMap[String, String], sql: String)
  }

  /**
    * 防止重跑导致重复数据
    **/
  def delMysql(table: String): Unit = {
    val del_sql = s"delete from $table"
    DBUtils.executeSql(DBProperties.JDBC_URL_PROFILE, DBProperties.USER_PROFILE, DBProperties.PASSWORD_PROFILE, del_sql)
  }

  def getDurationToMysql(userTimerange: mutable.Map[String, Long]) = {
    for ((k, v) <- userTimerange) {
      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection(DBProperties.JDBC_URL_PROFILE, DBProperties.USER_PROFILE, DBProperties.PASSWORD_PROFILE)
      val ps = connection.prepareStatement(s"INSERT INTO t_user_profile_label (f_user_id,duration)  VALUES('$k','$v');")
      ps.execute()
    }
  }

  def getOnTimeToMysql(firstMonth: String, userTimerange: mutable.HashMap[String, String], between: Long) = {
    for ((k, v) <- userTimerange) {
      val value = v.split(":")
      val active = value(0)
      val time = format("%.6f", (value(1).toDouble / 3600))
      val avgtime = format("%.6f", value(1).toDouble / 3600 / between)
      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection(DBProperties.JDBC_URL_PROFILE, DBProperties.USER_PROFILE, DBProperties.PASSWORD_PROFILE)
      val ps = connection.prepareStatement(
        s"""
           |INSERT INTO t_user_profile_label_by_month (f_date,f_user_id,active_days,online_duration,average_duration)  VALUES('$firstMonth','$k','$active','$time','$avgtime');
         """.stripMargin)
      ps.execute()
    }
  }

  def getMediaToMysql(mediauser: DataFrame) = {
    mediauser.collect().map(x => {
      val f_user_id = x.getAs[String]("f_user_id")
      val f_media = x.getAs[String]("f_media")
      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection(DBProperties.JDBC_URL_PROFILE, DBProperties.USER_PROFILE, DBProperties.PASSWORD_PROFILE)
      val ps = connection.prepareStatement(s"update t_user_profile_label set media='$f_media' where f_user_id = '$f_user_id' ;")
      ps.execute()
    })
  }


  def getBillingToMysql(billingMap: mutable.HashMap[String, String]) = {
    for ((k, v) <- billingMap) {
      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection(DBProperties.JDBC_URL_PROFILE, DBProperties.USER_PROFILE, DBProperties.PASSWORD_PROFILE)
      val ps = connection.prepareStatement(s"update t_user_profile_label set behaviortenant_group='$v' where f_user_id = '$k' ;")
      ps.execute()
    }
  }

  def dateFromPhoniex(map: mutable.HashMap[String, String], sql: String) = {
    var conn: Connection = null
    var st: Statement = null
    var rs: ResultSet = null
    try {
      val url = CluserProperties.PHOENIX_ZKURL
      conn = DriverManager.getConnection(url)
      st = conn.createStatement()
      rs = st.executeQuery(sql)
      while (rs.next()) {
        val k = rs.getString("k")
        val v = rs.getString("v")
        map.put(k, v)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      rs.close
      st.close
      conn.close()
    }
    map
  }

  def updateMapToMysql(map: mutable.HashMap[String, String], columnName: String, tableName: String) = {
    for ((k, v) <- map) {
      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection(DBProperties.JDBC_URL_PROFILE, DBProperties.USER_PROFILE, DBProperties.PASSWORD_PROFILE)
      val ps = connection.prepareStatement(s"update $tableName set $columnName='$v' where f_user_id = '$k' ;")
      ps.execute()
    }
  }

  def updateDFToMysql(df: DataFrame, columnName: String, tableName: String) = {
    df.collect().map(x => {
      val k = x.getAs[String]("k")
      val v = x.getAs[String]("v")
      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection(DBProperties.JDBC_URL_PROFILE, DBProperties.USER_PROFILE, DBProperties.PASSWORD_PROFILE)
      val ps = connection.prepareStatement(s"update $tableName set $columnName='$v' where f_user_id = '$k' ;")
      ps.execute()
    })

  }
}
