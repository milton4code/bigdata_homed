package cn.ipanel.homed.repots

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}

import cn.ipanel.common.{DBProperties, SparkSession, UserActionType}
import cn.ipanel.etl.LogConstant
import cn.ipanel.etl.LogParser.getRegionCode
import cn.ipanel.homed.repots.RevenueReport.getAreaId
import cn.ipanel.utils.DateUtils.YYYYMMDD
import cn.ipanel.utils.{DBUtils, DateUtils, MultilistUtils, RegionUtils}
import jodd.util.StringUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.util.control.Breaks


/**
  * 订购触发媒资
  * ocn演示项目 现在要做成公共需求
  */
object BusinessProgram {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("BusinessProgram")
    val sc = sparkSession.sparkContext

    if (args.length != 3) {
      println(
        """
          |请输入参数 日期,分区数, 日志版本
          |true  推流
          |flase 上报
        """.stripMargin)
      sys.exit(-1)
    }
    val day = args(0)
    val date = DateUtils.transformDateStr(day)
    val num = args(1).toInt
    val version = args(2).toBoolean
    val sqlContext = sparkSession.sqlContext
    val regionCode = getRegionCode
    //默认区域码
    // 1
    //点播单集 100000000L-199999999L
    val demandVideoMap = sc.broadcast(getDemandMap)
    // 3
    //回看单集 200000000L-299999999L
    val lookbackVideoMap = sc.broadcast(getLookbackMap)
    // 5
    //homed应用 1000000000L-1099999999L
    val appMap = sc.broadcast(getHomedAppMap)
    // 6
    //音乐 500000000L-549999999L
    val musicMap = sc.broadcast(getMusicMap)
    // 10
    //专题 1300000000L-1399999999L
    val subjectMap = sc.broadcast(getSubjectMap)
    // 12
    //频道相关 4200000000L-4201999999L
    val channelMap = sc.broadcast(getChannelMap)

    val regionDF = getUserRegion(regionCode, sqlContext)
    val regionCodeBr: Broadcast[String] = sc.broadcast(regionCode)
    val regionBr: Broadcast[DataFrame] = sc.broadcast(regionDF)
    regionBr.value.registerTempTable("t_region")
    val packageInfo = getPackage(sqlContext)

    val orderProgramDF = processLog(sc, sqlContext, date, packageInfo, num, regionCodeBr.value, day,
      demandVideoMap, lookbackVideoMap, appMap, musicMap, subjectMap, channelMap, version)
    val regionInfo = getRegionInfo(sqlContext, RegionUtils.getRootRegion)
    val finalDF = orderProgramDF.join(regionInfo, "f_region_id")
      .repartition(num)
    DBUtils.saveDataFrameToPhoenixNew(finalDF, "t_program_package")
    regionBr.unpersist()
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

  def getPackage(sqlContext: HiveContext) = {
    val sql =
      s"""
         |(select package_id,f_order_type,package_name from program_package
         |where
         |status=${UserActionType.BUSINESS_PACKAGE_EFFECTIVE.toInt}
         |) as aa
      """.stripMargin
    val packageTypeDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    packageTypeDF
  }

  def getBusinessRecord(time: String, nowtime: String, sqlContext: HiveContext) = {
    import sqlContext.implicits._
    val data = MultilistUtils.getMultilistData("homed_iusm", "t_business_record", sqlContext)
    data.cache().registerTempTable("t_business_record")
    data.count()
    val sql =
      s"""
         |SELECT f_order_id,f_payment_mode as f_paymode
         | FROM t_business_record
         | WHERE f_order_time >'$time' and f_order_time<'$nowtime'
         | and f_order_type=${UserActionType.BUSINESS_ORDER_TYPE_CONSUME.toInt}
     """.stripMargin
    val businessRecordDF = sqlContext.sql(sql)
    data.unpersist()
    businessRecordDF
  }


  def processLog(sc: SparkContext, sqlContext: HiveContext, date: String,
                 packageInfo: DataFrame, num: Int, regionCode: String, day: String,
                 demandVideoMap: Broadcast[mutable.HashMap[String, String]],
                 lookbackVideoMap: Broadcast[mutable.HashMap[String, String]],
                 appMap: Broadcast[mutable.HashMap[String, String]],
                 musicMap: Broadcast[mutable.HashMap[String, String]],
                 subjectMap: Broadcast[mutable.HashMap[String, String]],
                 channelMap: Broadcast[mutable.HashMap[String, String]], version: Boolean) = {
    import sqlContext.implicits._
    var logDF = sqlContext.emptyDataFrame
    if (version) {
      val path = LogConstant.IUSM_LOG_PATH + date + "/*"
      //val path = "file:///C:\\Users\\Administrator\\Desktop\\BB"
      val billRdd = sc.textFile(path)
      val billDF = billRdd.filter(x => x.contains("BillingSuccess")).filter(x => StringUtil.isNotEmpty(x))
        .map(x => {
          val userId = if (getKeywords(x, "Operator") != "") getKeywords(x, "Operator")
          else getKeywords(x, "DA")
          val time = getLogTimeRegex(x)
          val package_id = getKeywords(x, "ComboId")
          val programId = getKeywords(x, "SeriesId")
          val ProgramName = getKeywords(x, "ProgramName")
          val DeviceId = getKeywords(x, "DeviceId")
          val deviceType = deviceIdMapToDeviceType(DeviceId)
          val PayMode = getKeywords(x, "PayMode")
          val Money = getKeywords(x, "Money")
          (userId, time, package_id, programId, ProgramName, DeviceId, deviceType, PayMode, Money)
        }).toDF("userId", "time", "package_id", "programId", "ProgramName", "DeviceId", "deviceType", "PayMode", "Money").filter("programId != 0")
      billDF.join(packageInfo, "package_id")
        //      +----------+--------+-------------------+---------+-----------+----------+----------+-------+-----+------------+------------+
        //      |package_id|  userId|               time|programId|ProgramName|  DeviceId|deviceType|PayMode|Money|f_order_type|package_name|
        //      +----------+--------+-------------------+---------+-----------+----------+----------+-------+-----+------------+------------+
        //      |       529|55313240|2019-04-04 09:07:40|300249911|       幕后玩家|1005359975|         1|      1|  500|           1|      5元单片点播|
        //      +----------+--------+-------------------+---------+-----------+----------+----------+-------+-----+------------+------------+
        .repartition(num).registerTempTable("log")
      logDF = sqlContext.sql(//获取区域字段
        s"""
           |select '$day' as f_date,a.deviceType as f_terminal,
           |a.regionId as f_region_id,a.package_id as f_package_id,
           |cast(a.PayMode as int) as f_paymode,
           |a.f_order_type as f_package_type,
           |a.ProgramName as f_program_name,
           |a.programId as f_program_id,
           |a.package_name as f_package_name,
           |cast(sum(a.Money) as bigint) as f_money,count(*) as f_count
           |from
           |(select t1.userId,if(t2.regionId is null,$regionCode,t2.regionId) as regionId,
           |t1.package_id,t1.programId,t1.package_name,
           |t1.ProgramName,t1.deviceType,t1.PayMode,t1.Money,t1.f_order_type
           |from log t1
           |left join t_region t2
           |on t1.userId=t2.userId) a
           |group by
           |a.regionId,a.package_id,a.deviceType,a.PayMode,a.f_order_type,
           |a.deviceType,a.ProgramName,a.programId,a.package_name
              """.stripMargin)
        .repartition(num)
    } else {
      //      +--------+----------+-----------+------------+---------+--------------+--------------+------------+--------------+-------+-------+
      //      |  f_date|f_terminal|f_region_id|f_package_id|f_paymode|f_package_type|f_program_name|f_program_id|f_package_name|f_money|f_count|
      //      +--------+----------+-----------+------------+---------+--------------+--------------+------------+--------------+-------+-------+
      //      |20170926|         1|     440301|         529|        1|             1|    宝塔镇河妖之诡墓龙棺|   300250013|        5元单片点播|    500|      1|
      //        +--------+----------+-----------+------------+---------+--------------+--------------+------------+--------------+-------+-------+
      sqlContext.sql("use bigdata")
      val df = sqlContext.sql(
        s"""
           | select devicetype,regionid,exts['PID'] as programId ,exts['PI'] as package_id,
           | exts['PM'] as money,exts['PT'] as f_package_type,exts['OI'] as f_order_id,
           | exts['PN'] as package_name
           | from orc_user_behavior where reporttype  = 144 and day  = '$day'
           | and exts['R']=1
           | and exts['PID'] is not null and exts['PI'] is not null
           | and exts['PM'] is not null
           | and exts['PN'] is not null
        """.stripMargin)
      val nowtime = DateUtils.getNDaysBefore(-1, day)
      val nowdate = DateUtils.transformDateStr(nowtime)
      val paymodedf = getBusinessRecord(date, nowdate: String, sqlContext: HiveContext)
      df.join(paymodedf, Seq("f_order_id")).repartition(num)
        .registerTempTable("df")
      logDF = sqlContext.sql(
        s"""
           | select '$day' as f_date,devicetype,regionid,programId , package_id,
           | sum(money) as money, f_package_type, f_paymode,
           | package_name,count(*) as f_count
           | from df
           | group by
           | devicetype,regionid,devicetype,regionid,programId , package_id,
           |  f_package_type, f_paymode,
           | package_name
            """.stripMargin).map(x => {
        val f_date = x.getAs[String]("f_date")
        val f_count = x.getAs[Long]("f_count")
        val f_terminal = x.getAs[String]("devicetype").toInt
        val f_program_id = x.getAs[String]("programId")
        val f_package_id = x.getAs[String]("package_id")
        val f_program_name = getName(x.getAs[String]("programId"),
          demandVideoMap.value, lookbackVideoMap.value, appMap.value,
          musicMap.value, subjectMap.value, channelMap.value)
        val f_money = x.getAs[Double]("money").toLong
        val f_package_type = x.getAs[String]("f_package_type").toInt
        val f_paymode = x.getAs[Int]("f_paymode")
        val f_package_name = x.getAs[String]("package_name")
        val f_region_id = x.getAs[String]("regionid")
        (f_date, f_terminal, f_region_id, f_package_id, f_paymode, f_package_type, f_program_name, f_program_id,
          f_package_name, f_money, f_count)
      }).toDF("f_date", "f_terminal", "f_region_id", "f_package_id", "f_paymode", "f_package_type", "f_program_name", "f_program_id",
        "f_package_name", "f_money", "f_count")
        .filter("f_program_name != ''")
    }

    logDF
  }

  def getName(programId: String,
              demandVideoMap: mutable.HashMap[String, String],
              lookbackVideoMap: mutable.HashMap[String, String],
              appMap: mutable.HashMap[String, String],
              musicMap: mutable.HashMap[String, String],
              subjectMap: mutable.HashMap[String, String],
              channelMap: mutable.HashMap[String, String]) = {
    val num = getNumByResultId(programId.toLong)
    var content = ""
    if (num == 1) {
      content = demandVideoMap.get(programId).getOrElse("")
    }
    else if (num == 3) {
      content = lookbackVideoMap.get(programId).getOrElse("")
    }
    else if (num == 5) {
      content = appMap.get(programId).getOrElse("")
    }
    else if (num == 6) {
      content = musicMap.get(programId).getOrElse("")
    }
    else if (num == 10) {
      content = subjectMap.get(programId).getOrElse("")
    }
    else if (num == 12) {
      content = channelMap.get(programId).getOrElse("")
    }
    content
  }

  def getDemandMap = {
    val map = HashMap[String, String]()
    val sql =
      """
        |select distinct(a.video_id) as result_id,a.video_name as program_name
        |from video_info a
      """.stripMargin
    var conn: Connection = null
    var st: Statement = null
    var rs: ResultSet = null
    try {
      conn = getConn(DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
      st = conn.createStatement()
      rs = st.executeQuery(sql)
      while (rs.next()) {
        val result_id = rs.getLong("result_id").toString
        val program_name = rs.getString("program_name")
        map.put(result_id, program_name)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      close(conn, rs, st, null)
    }
    map
  }

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

  def getConn(url: String, user: String, pass: String): Connection = {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    DriverManager.getConnection(url, user, pass)
  }


  /**
    * 获取区域信息
    */
  def getUserRegion(regionCode: String, sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    // 分开查询
    val accountInfoSql = "(select  DA as f_user_id,home_id as f_home_id from account_info) as account"
    val addressInfoSql =
      s"""
         |(select home_id,
         |case when  length(cast(region_id as char)) >= 6  then cast(region_id as char)
         |     when length(cast(region_id as char)) = 6  and SUBSTR(region_id,5) ='00'then  region_id+1
         |    else region_id end f_region_id
         |from address_info ) as address
                    """.stripMargin
    val accountDF = DBUtils.loadMysql(sqlContext, accountInfoSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val addressDF = DBUtils.loadMysql(sqlContext, addressInfoSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)

    var _regionCode = RegionUtils.getRootRegion
    var filter_region = ""
    if (_regionCode.endsWith("0000")) {
      filter_region = _regionCode.substring(0, 2)
    } else {
      filter_region = _regionCode.substring(0, 4)
    }
    val region = accountDF.join(addressDF, accountDF("f_home_id") === addressDF("home_id"))
      .selectExpr("f_user_id as userId", "f_region_id as regionId")
      .filter($"regionId".startsWith(filter_region))
    region
  }

  def deviceIdMapToDeviceType(deviceId: String): Int = {
    //    var deviceType = "unknown"
    //终端（0 其他, 1stb,2 CA卡,3mobile,4 pad, 5pc）
    var deviceType = 0
    try {
      val device = deviceId.toLong
      if (device >= 1000000000l && device <= 1199999999l) {
        deviceType = 1
      } else if (device >= 1400000000l && device <= 1599999999l) {
        deviceType = 2
      } else if (device >= 1800000000l && device < 1899999999l) {
        deviceType = 4
      } else if (device >= 2000000000l && device <= 2999999999l) {
        deviceType = 3
      } else if (device >= 3000000000l && device < 3999999999l) {
        deviceType = 5
      }
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
    deviceType
  }

  /**
    * 根据正则抽取关键字段
    *
    * @param line 不允许为空
    * @return
    */
  def getKeywords(line: String, keyword: String): String = {
    val loop = new Breaks
    var result = ""
    val fields = line.split(",")
    loop.breakable(
      for (x <- fields) {
        if (x.startsWith(keyword)) {
          result = x.replace(keyword, "").trim
          loop.break()
        }
      }
    )
    result
  }

  def getLogTimeRegex(line: String): String = {
    val pattern = "(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})".r
    val timeStr = pattern findFirstIn line
    timeStr.getOrElse("")
  }

  def getLookbackMap = {
    val map = HashMap[String, String]()
    val sql =
      """
        |select distinct(a.event_id) as result_id,a.event_name as program_name
        |from homed_eit_schedule_history a
      """.stripMargin
    var conn: Connection = null
    var st: Statement = null
    var rs: ResultSet = null
    try {
      conn = getConn(DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
      st = conn.createStatement()
      rs = st.executeQuery(sql)
      while (rs.next()) {
        val result_id = rs.getLong("result_id").toString
        val program_name = rs.getString("program_name")
        map.put(result_id, program_name)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      close(conn, rs, st, null)
    }
    map
  }

  // 5
  //homed应用 1000000000L-1099999999L
  def getHomedAppMap = {
    val map = HashMap[String, String]()
    val sql =
      """
        |select distinct(id) as result_id,id as program_id,chinese_name as program_name,
        |id as series_id,f_content_type as contenttype,
        |f_sub_type as subtype,
        |chinese_name as series_name
        |from app_store
      """.stripMargin
    var conn: Connection = null
    var st: Statement = null
    var rs: ResultSet = null
    try {
      conn = getConn(DBProperties.JDBC_URL_ICORE, DBProperties.USER_ICORE, DBProperties.PASSWORD_ICORE)
      st = conn.createStatement()
      rs = st.executeQuery(sql)
      while (rs.next()) {
        val result_id = rs.getLong("result_id").toString
        val program_name = rs.getString("program_name")
        map.put(result_id, program_name)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      close(conn, rs, st, null)
    }
    map
  }

  // 6
  //音乐 500000000L-549999999L
  def getMusicMap = {
    val map = HashMap[String, String]()
    val sql =
      """
        |select distinct(a.music_id) as result_id,a.music_name as program_name
        |from music_info a
      """.stripMargin
    var conn: Connection = null
    var st: Statement = null
    var rs: ResultSet = null
    try {
      conn = getConn(DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
      st = conn.createStatement()
      rs = st.executeQuery(sql)
      while (rs.next()) {
        val result_id = rs.getLong("result_id").toString
        val program_name = rs.getString("program_name")
        map.put(result_id, program_name)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      close(conn, rs, st, null)
    }
    map
  }


  // 10
  //专题 1300000000L-1399999999L
  def getSubjectMap = {
    val map = HashMap[String, String]()
    val sql =
      """
        |select distinct(f_subject_id) as result_id,
        |f_subject_name as program_name
        |from t_subject_info
      """.stripMargin
    var conn: Connection = null
    var st: Statement = null
    var rs: ResultSet = null
    try {
      conn = getConn(DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
      st = conn.createStatement()
      rs = st.executeQuery(sql)
      while (rs.next()) {
        val result_id = rs.getLong("result_id").toString
        val program_name = rs.getString("program_name")
        map.put(result_id, program_name)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      close(conn, rs, st, null)
    }
    map
  }

  // 12
  //频道相关 4200000000L-4201999999L
  def getChannelMap = {
    val map = HashMap[String, String]()
    val sql =
      """
        |select distinct(channel_id) as result_id,channel_id as program_id ,
        |chinese_name as program_name
        |from channel_store
      """.stripMargin
    var conn: Connection = null
    var st: Statement = null
    var rs: ResultSet = null
    try {
      conn = getConn(DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
      st = conn.createStatement()
      rs = st.executeQuery(sql)
      while (rs.next()) {
        val result_id = rs.getLong("result_id").toString
        val program_name = rs.getString("program_name")
        map.put(result_id, program_name)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      close(conn, rs, st, null)
    }
    map
  }

  def getNumByResultId(e: Long): Int = {
    var num = 0
    if (e > 100000000L && e < 199999999L) {
      num = 1
    }
    else if (e > 200000000L && e < 299999999L) {
      num = 3
    }
    else if (e > 1000000000L && e < 1099999999L) {
      num = 5
    }
    else if (e > 500000000L && e < 549999999L) {
      num = 6
    }
    else if (e > 1300000000L && e < 1399999999L) {
      num = 10
    }
    else if (e > 4200000000L && e < 4201999999L) {
      num = 12
    }
    num
  }
}
