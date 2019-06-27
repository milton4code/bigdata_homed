package cn.ipanel.homed.repots

import java.sql.DriverManager
import java.util

import cn.ipanel.common.{Constant, DBProperties, Tables}
import cn.ipanel.utils.{DBUtils, DateUtils}
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.control.Breaks

case class Column_info(f_column_level: Int, f_column_id: Long, f_column_name: String, f_parent_column_id: Long,
                       f_parent_column_name: String, f_parent_parent_column_id: Long, f_parent_parent_column_name: String)

/** *
  * 栏目下节目报表
  * 建表语句
  * create table if not exists t_column_video_report(
  * f_date VARCHAR not null,
  * f_hour VARCHAR not null,
  * f_terminal INTEGER not null,
  * f_program_id VARCHAR not null,
  * f_video_name VARCHAR,
  * f_user_id VARCHAR not null,
  * f_terminal_id VARCHAR not null,
  * f_TVstation VARCHAR,
  * f_community VARCHAR,
  * f_doorplate VARCHAR,
  * f_column_level INTEGER,
  * f_column_id BIGINT not null,
  * f_column_name  VARCHAR,
  * f_parent_column_id BIGINT,
  * f_parent_column_name VARCHAR,
  * f_parent_parent_column_id BIGINT,
  * f_parent_parent_column_name VARCHAR,
  * f_user_pv INTEGER,
  * f_phone_model VARCHAR,
  * f_app_version VARCHAR
  * CONSTRAINT PK PRIMARY KEY(f_date,f_hour,f_terminal,f_program_id,f_user_id,f_terminal_id,f_column_id)
  * );
  */
object ColumnAndVideoReport {
  var save_path = ""

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("请输入日期和csv文件保存路径 如20190217 、/ColumnAndVideoReport/day=20190227")
      System.exit(-1)
    }
    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.codegen", "true")
    conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
    conf.set("spark.sql.autoBroadcastJoinThreshold", "209715200")
    conf.setAppName("ColumnAndVideoReport")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val day = args(0) // 时间
    save_path = args(1) // csv文件保存路径
    //文件路径
    //    val path = "/user/hive/warehouse/userdata_1/day=" + day
    //    val lines = sc.textFile(path).filter(x => x.startsWith("101") || x.startsWith("103") || x.startsWith("104")).map(_.split(","))
    //      .filter(x => {
    //        val maps = parseMaps(x(8))
    //        if (x.length == 9 && !x(5).contains("guest") && maps.contains("A") && maps("A") == "1") true else false
    //      })

    val column = getColumnInfo(sqlContext)
    //    deleteData(day)
    val lines = getData(sqlContext, day)
    lines.persist(StorageLevel.MEMORY_AND_DISK)
    dealData(lines, sqlContext, day, column)
    lines.unpersist()
  }

  def getData(sqlContext: SQLContext, day: String): DataFrame = {
    val sql =
      s"""
         |select * from bigdata.orc_user_report
         |where day='$day' and (servicetype in ('101','103','104') and paras is not null and paras['A']='1' or servicetype='301')
      """.stripMargin
    sqlContext.sql(sql)
  }

  //删除数据
  def deleteData(day: String): Unit = {
    val table = Tables.F_COLUMN_VIDEO_REPORT
    val sql = s"delete from $table where f_date='$day'"
    DBUtils.excuteSqlPhoenix(sql)
  }

  //处理数据
  def dealData(lines: DataFrame, sqlContext: SQLContext, day: String, column: util.HashMap[Long, String]): Unit = {
    import sqlContext.implicits._
    getChannelEventData(sqlContext).registerTempTable("channel_event_final_table")

    //101 103
    val live_lookback_df = lines.filter(col("servicetype") === "101" || col("servicetype") === "103").map(x => {
      val servicetype = x.getAs[String]("servicetype")
      val maps = x.getAs[scala.collection.immutable.HashMap[String, String]]("paras")
      val reportTime = x.getAs[String]("starttime")
      val date = DateUtils.dateStrToDateTime(reportTime)
      val day1 = day
      val hour = date.getHourOfDay().toString
      val terminal_type = x.getAs[String]("devicetype")
      var channel_id = "0"
      val column_id = maps.getOrElse("CL", "null")
      val user_id = x.getAs[String]("userid")
      val deviceid = x.getAs[Long]("deviceid").toString
      var upTime = "0"
      if (servicetype == "101") {
        upTime = reportTime
        channel_id = maps.getOrElse("ID", "0")
      } else if (servicetype == "103") {
        var stime = maps.getOrElse("ST", "0").trim
        channel_id = maps.getOrElse("CI", "0")
        if (stime.contains(":")) {
          upTime = (stime.substring(0, 10) + " " + stime.substring(10, stime.length).trim).trim
        } else if (stime.length == 10) {
          upTime = DateUtils.date_Format(stime)
        }
      }
      (upTime, day1, hour, terminal_type, channel_id, column_id, user_id, deviceid)
    }).toDF("f_uptime", "f_date", "f_hour", "f_terminal", "f_channel_id", "f_column_id", "f_user_id", "f_device_id")
    live_lookback_df.registerTempTable("live_lookback_table")
    //    println("live_lookback_df:" + live_lookback_df.count())
    val df1 = sqlContext.sql(
      """
        |select f_date,f_hour,f_terminal,f_column_id,program_id as f_program_id,video_name as f_video_name ,f_user_id,f_series_id,f_device_id
        |from live_lookback_table a join channel_event_final_table b on a.f_channel_id=b.channel_id and a.f_uptime>=b.start_time and a.f_uptime<=b.end_time
      """.stripMargin)
    //    println("df1:" + df1.count())
    //    df1.show(100, false)
    //关联column_id
    val seriesAndColumnMap = getChnldColumn()

    //104
    val demand_df = lines.filter(col("servicetype") === "104").map(x => {
      val maps = x.getAs[scala.collection.immutable.HashMap[String, String]]("paras")
      val reportTime = x.getAs[String]("starttime")
      val date = DateUtils.dateStrToDateTime(reportTime)
      val day1 = day
      val hour = date.getHourOfDay().toString
      val terminal_type = x.getAs[String]("devicetype")
      val program_id = maps.getOrElse("ID", "0")
      val column_id = maps.getOrElse("CL", "null")
      val user_id = x.getAs[String]("userid")
      val deviceid = x.getAs[Long]("deviceid").toString
      (day1, hour, terminal_type, program_id, column_id, user_id, deviceid)
    }).toDF("f_date", "f_hour", "f_terminal", "f_program_id", "f_column_id", "f_user_id", "f_device_id")
    demand_df.registerTempTable("demand_table")
    //    println("demand_df:"+demand_df.count())
    getVideoInfo(sqlContext).registerTempTable("video_info_table")
    val df2 = sqlContext.sql(
      """
        |select f_date,f_hour,f_terminal,f_column_id,f_program_id,video_name as f_video_name,f_user_id,f_series_id,f_device_id
        |from demand_table a join video_info_table b on a.f_program_id=b.video_id
      """.stripMargin)
    //    println("df2:"+df2.count())
    //获取column_id
    val df_12 = df1.unionAll(df2).persist(StorageLevel.MEMORY_AND_DISK)
    //    df_12.printSchema()
    //    df_12.filter(col("f_series_id").isNull).show(false)
    val df_1 = df_12.rdd.filter(x => x.getAs[String]("f_column_id") == "null").map(x => {
      val f_date = x.getAs[String]("f_date")
      val f_hour = x.getAs[String]("f_hour")
      val f_terminal = x.getAs[String]("f_terminal")
      var f_column_id = x.getAs[String]("f_column_id")
      val f_program_id = x.getAs[String]("f_program_id")
      val f_video_name = x.getAs[String]("f_video_name")
      val f_user_id = x.getAs[String]("f_user_id")
      val f_series_id = x.getAs[Long]("f_series_id").toString
      val f_device_id = x.getAs[String]("f_device_id")
      f_column_id = weigthList(f_series_id, seriesAndColumnMap)
      (f_date, f_hour, f_terminal, f_column_id, f_program_id, f_video_name, f_user_id, f_series_id, f_device_id)
    }).toDF("f_date", "f_hour", "f_terminal", "f_column_id", "f_program_id", "f_video_name", "f_user_id", "f_series_id", "f_device_id")
    val df_2 = df_12.rdd.filter(x => x.getAs[String]("f_column_id") != "null").map(x => {
      val f_date = x.getAs[String]("f_date")
      val f_hour = x.getAs[String]("f_hour")
      val f_terminal = x.getAs[String]("f_terminal")
      var f_column_id = x.getAs[String]("f_column_id")
      val f_program_id = x.getAs[String]("f_program_id")
      val f_video_name = x.getAs[String]("f_video_name")
      val f_user_id = x.getAs[String]("f_user_id")
      val f_series_id = x.getAs[Long]("f_series_id").toString
      val f_device_id = x.getAs[String]("f_device_id")
      if (f_column_id == "0") getStr()
      (f_date, f_hour, f_terminal, f_column_id, f_program_id, f_video_name, f_user_id, f_series_id, f_device_id)
    }).toDF("f_date", "f_hour", "f_terminal", "f_column_id", "f_program_id", "f_video_name", "f_user_id", "f_series_id", "f_device_id")
    val union_df = df_1.unionAll(df_2)
    df_12.unpersist()
    val user_address_device_df = getUserAdressAndDeviceID(sqlContext)

    //获取用户设备信息
    val user_device_info_sql =
      """(select f_user_id ,f_device_id ,f_phone_model,f_app_version from t_user_device_info) as a"""
    val user_device_info_df = DBUtils.loadDataFromPhoenix2(sqlContext, user_device_info_sql)
    val final_df = union_df.join(user_address_device_df, Seq("f_user_id", "f_device_id"))
      .join(user_device_info_df, Seq("f_user_id", "f_device_id"), "left").na.fill("")

    val base_df = final_df.map(x => {
      val f_date = x.getAs[String]("f_date")
      val f_hour = x.getAs[String]("f_hour")
      val f_terminal = x.getAs[String]("f_terminal").toInt
      val f_column_id = x.getAs[String]("f_column_id").toLong
      val f_program_id = x.getAs[String]("f_program_id")
      val f_video_name = x.getAs[String]("f_video_name")
      val f_user_id = x.getAs[String]("f_user_id")
      val f_phone_model = x.getAs[String]("F_PHONE_MODEL")
      val f_app_version = x.getAs[String]("F_APP_VERSION")
      val address_name = x.getAs[String]("address_name")
      val f_terminal_id = x.getAs[String]("f_terminal_id")
      var detail = ""
      var f_TVstation = ""
      var f_community = ""
      var f_doorplate = ""
      if (address_name.contains("{") && address_name.contains(":")) {
        //        val json = JSON.parseObject(address_name)
        val json = scala.util.parsing.json.JSON.parseFull(address_name).get.asInstanceOf[scala.collection.immutable.Map[String, String]]
        //        detail = json.get("detail").toString.trim
        detail = json("detail").toString.trim
      } else {
        detail = address_name.trim
      }
      val detail_arr = detail.split(" ")
      if (detail_arr.length == 5) {
        f_TVstation = detail_arr(1)
        f_community = detail_arr(2)
        val code = detail_arr(4).replaceAll("\\D", "")
        if (code.length > 1) {
          f_doorplate = detail_arr(3) + "-" + code
        } else {
          f_doorplate = detail_arr(3)
        }
        //        f_doorplate = detail_arr(3) + "-" + code
      } else if (detail_arr.length == 4) {
        f_TVstation = detail_arr(0)
        f_community = detail_arr(1)
        val code = detail_arr(3).replaceAll("\\D", "")
        if (code.length > 1) {
          f_doorplate = detail_arr(2) + "-" + code
        } else {
          f_doorplate = detail_arr(2)
        }

      }
      (f_date, f_hour, f_terminal, f_column_id, f_program_id, f_video_name, f_user_id, f_phone_model, f_app_version, f_terminal_id, f_TVstation, f_community, f_doorplate)
    }).toDF("f_date", "f_hour", "f_terminal", "f_column_id", "f_program_id", "f_video_name", "f_user_id", "f_phone_model", "f_app_version", "f_terminal_id", "f_TVstation", "f_community", "f_doorplate")
      .filter(col("f_column_id").isNotNull && col("f_program_id").isNotNull)
      .groupBy("f_date", "f_hour", "f_terminal", "f_column_id", "f_program_id", "f_video_name", "f_user_id", "f_phone_model", "f_app_version", "f_terminal_id", "f_TVstation", "f_community", "f_doorplate")
      .count()
      .withColumnRenamed("count", "f_user_pv")
    //      .selectExpr("f_date", "f_hour", "f_terminal","f_column_id",  "f_program_id", "f_video_name", "f_user_id", "f_terminal_id", "f_TVstation", "f_community", "f_doorplate", "`count` as f_user_pv")

    val column_info = base_df.map(x => {
      val column_id = x.getAs[Long]("f_column_id")
      val column_info = getColumnRoot(column_id, column) //将栏目信息构成listBuffer
      var f_column_id: Long = column_id //解析listBuffer  将栏目id,栏目名字及父栏目信息读出
      var f_column_name: String = ""
      var f_parent_column_id: Long = 0L
      var f_parent_column_name: String = ""
      var f_parent_parent_column_id: Long = 0L
      var f_parent_parent_column_name: String = ""
      val f_column_level = column_info.length - 1
      //得出栏目的层级
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

    val base1 = column_info.join(base_df, "f_column_id").persist(StorageLevel.MEMORY_AND_DISK)
    //保存数据到hbase
    DBUtils.saveDataFrameToPhoenixNew(base1, Tables.F_COLUMN_VIDEO_REPORT)
    //以csv格式保存数据到hdfs
    import com.databricks.spark.csv._
    base1.coalesce(1).saveAsCsvFile(s"$save_path", Map("header" -> "true"))
    base1.unpersist()
  }

  //处理301开机数据得到用户和app版本信息及设备相关信息
  def dealOpenDetail(lines: DataFrame, day: String, hiveContext: SQLContext) {
    lines.filter(col("servicetype") === "301").registerTempTable("deviceInfo_table")
    val df = hiveContext.sql(
      s"""
         |select
         |if((a.f_phone_model is not null),a.f_phone_model,'${Constant.UNKOWN}') as f_phone_model,
         |if((a.f_manufacturer is not null),a.f_manufacturer,'${Constant.UNKOWN}') as f_manufacturer,
         |if((a.f_app_version is not null),a.f_app_version,'${Constant.UNKOWN}') as f_app_version,
         |if(a.f_device_hardware_version is not null,a.f_device_hardware_version,'${Constant.UNKOWN}') as f_device_hardware_version,
         |if(a.f_device_software_version is not null,a.f_device_software_version,'${Constant.UNKOWN}') as f_device_software_version,
         |cast(a.deviceid as string) as f_device_id,
         |a.userid as f_user_id
         |from
         |(select userid,devicetype,paras['M'] as f_phone_model,
         |paras['MA'] as f_manufacturer,paras['A'] as f_app_version,
         |paras['H'] as f_device_hardware_version,
         |paras['S'] as f_device_software_version,
         |regionid, deviceid,starttime,
         |row_number() over(partition by userid ,deviceid order by starttime desc ) as num
         |from deviceInfo_table
         |) a
         |where num <2
        """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(df, "t_user_device_info")
  }

  //获取video_info
  def getVideoInfo(sqlContext: SQLContext): DataFrame = {
    val sql = """(select video_id ,video_name ,f_series_id from video_info ) as a"""
    val video_df = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    video_df
  }

  //获取不同时间段不同频道下节目信息
  def getChannelEventData(sqlContext: SQLContext): DataFrame = {
    sqlContext.udf.register("get_end_times", (start_time: String, duration: String) => DateUtils.get_end_time(start_time, duration))
    val sql = """(select homed_service_id as channel_id ,event_id as program_id,f_series_id ,event_name as video_name ,cast(start_time as char) as start_time ,cast(duration as char) as duration from homed_eit_schedule_history) as a"""
    val channel_event_df = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    channel_event_df.registerTempTable("channel_event_table")
    val channel_event_final_df = sqlContext.sql("select channel_id,program_id,video_name,f_series_id,start_time,get_end_times(start_time,duration) as end_time from channel_event_table")
    channel_event_final_df

  }

  //通过用户id 获取用户地址和机顶盒号
  def getUserAdressAndDeviceID(sqlContext: SQLContext): DataFrame = {
    //user_id-home_id
    val uh_sql =
      """(select DA as user_id,home_id from account_info where status=1 or status=2) as a"""
    val uh_df = DBUtils.loadMysql(sqlContext, uh_sql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()
    //home_id-address_id-address_name
    val ha_sql =
      """(select home_id,address_id,address_name from address_info where status=1) as b"""
    val ha_df = DBUtils.loadMysql(sqlContext, ha_sql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()
    //address_id-device_id
    val ad_sql =
      """(select address_id,device_id from address_device) as c"""
    val ad_df = DBUtils.loadMysql(sqlContext, ad_sql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()
    //device_id-terminal_type-terminal_id
    val dc_sql =
      """( select f_create_time,device_id,device_type as terminal_type,case device_type when 1 then stb_id when 2 then cai_id when 3 then mobile_id when 4 then pad_id when 5 then mac_address end as f_terminal_id
        |from homed_iusm.device_info where status=1 ) as d""".stripMargin
    val dc_df = DBUtils.loadMysql(sqlContext, dc_sql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()

    val final_df = uh_df.join(ha_df, "home_id")
      .join(ad_df, "address_id")
      .join(dc_df, "device_id")
      .selectExpr("user_id as f_user_id", "address_name", "device_id as f_device_id", "f_terminal_id").distinct()
    final_df

  }

  //获取栏目信息
  def getColumnInfo(sqlContext: SQLContext): util.HashMap[Long, String] = {
    val map = new java.util.HashMap[Long, String]()
    val connection = DriverManager.getConnection(DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val statement = connection.createStatement
    val queryRet = statement.executeQuery(
      "SELECT f_column_id,f_parent_id,f_column_name FROM	`t_column_info`")
    while (queryRet.next) {
      val f_column_id = queryRet.getLong("f_column_id")
      val f_column_name = queryRet.getString("f_column_name")
      val f_parent_id = queryRet.getLong("f_parent_id")
      map.put(f_column_id, f_parent_id + "," + f_column_name)
    }
    connection.close
    map
  }

  def parseMaps(paras: String): scala.collection.mutable.Map[String, String] = {
    import scala.collection.mutable.Map
    val map = Map[String, String]()
    val arr = paras.split("&")
    for (i <- arr) {
      val kv = Array(i.substring(0, i.indexOf(":")), i.substring(i.indexOf(":") + 1))
      if (kv.length == 2) {
        map += (kv(0) -> kv(1))
      } else {
      }
    }
    map
  }

  //pc
  //机顶盒
  //mobile
  def getTerminal(str: String): Int = {
    var strs = 0
    str.toLowerCase match {
      case "机顶盒" => strs = 1
      case "stb" => strs = 1
      case "ca" => strs = 2
      case "mobile" => strs = 3
      case "pad" => strs = 4
      case "pc" => strs = 5
    }
    strs
  }

  val COLUMN_ROOT = "0"

  def getColumnRoot(colId: Long, columnInfoMap: util.HashMap[Long, String]) = {
    var mapId = colId
    var find = false
    val listBuffer = ListBuffer[(Long, String)]()
    val tmplist = COLUMN_ROOT
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

  def getChnldColumn(): java.util.HashMap[String, ListBuffer[Int]] = {
    val sql =
      """
        |select t.f_column_id,f_program_id ,f_column_name from ( select f_column_id ,f_program_id from t_column_program where f_program_status = 1 and f_is_hide = 0 )t
        |join (select f_column_id ,f_column_name from t_column_info where f_parent_id !=0)tt on t.f_column_id=tt.f_column_id order by f_program_id
      """.stripMargin
    //    DBUtils.loadMysql(hiveContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val map = new java.util.HashMap[String, ListBuffer[Int]]()
    //    val list = ListBuffer[String]()
    var list_temp = new ListBuffer[Int]()
    val names = new ListBuffer[String]()
    val connection = DriverManager.getConnection(DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val statement = connection.createStatement
    val queryRet = statement.executeQuery(sql)
    var f_programid = ""
    while (queryRet.next) {
      val f_column_id = queryRet.getString("f_column_id").toInt
      val f_program_id = queryRet.getString("f_program_id")
      val f_column_name = queryRet.getString("f_column_name")
      if (!f_programid.equals(f_program_id)) {
        names.clear()
        if (list_temp.length == 0) {
          f_programid = f_program_id
          list_temp += f_column_id
          names += f_column_name
        } else {
          map.put(f_programid, list_temp)
          list_temp = new ListBuffer[Int]()
          f_programid = f_program_id
          list_temp += f_column_id
          names += f_column_name
        }
      } else {
        if (!list_temp.contains(f_column_id) && !names.contains(f_column_name)) {
          list_temp += f_column_id
          names += f_column_name
        }
      }
      map.put(f_programid, list_temp)
    }
    connection.close
    map
  }

  def getStr(): String = {
    val arr = Array("101", "116", "103", "109")
    var result = arr(Random.nextInt(4))
    result
  }

  def weigthList(chnld: String, map: java.util.HashMap[String, ListBuffer[Int]]): String = {
    var result = getStr()
    var loop = new Breaks
    loop.breakable(
      if (map.get(chnld) != null && map.get(chnld).length >= 12) {
        val value = map.get(chnld).toList.sorted
        //将数组分成四分（权重分别为50、150、350、850）
        val weigth = Array(50, 150, 350, 850)
        val splitNum = Math.round(value.length.toDouble / 4).toInt
        val len = value.length - splitNum * 3
        var sum = 0
        for (i <- 0 until weigth.length) {
          sum += weigth(i)
        }
        var randNum = Random.nextInt(sum)
        for (i <- 0 until weigth.length) {
          if (randNum <= weigth(i)) {
            i match {
              case 0 => result = value(Random.nextInt(splitNum)).toString
              case 1 => result = value(splitNum + Random.nextInt(splitNum)).toString
              case 2 => result = value(2 * splitNum + Random.nextInt(splitNum)).toString
              case 3 => result = value(3 * splitNum + Random.nextInt(len)).toString
            }
            loop.break()
          } else {
            randNum -= weigth(i)
          }
        }
      } else if (map.get(chnld) != null && map.get(chnld).length < 12 && map.get(chnld).length > 0) {
        val value = map.get(chnld).toList.sorted
        val len = value.length
        var weigth = new Array[Int](len)
        for (i <- 0 until (len)) {
          weigth(i) = (i + 1) * (i + 1) * 10
        }
        var sum = 0
        for (i <- 0 until (len)) {
          sum += weigth(i)
        }
        if (sum == 0) sum = 1
        var randNum = Random.nextInt(sum)
        for (i <- 0 until weigth.length) {
          if (randNum <= weigth(i)) {
            result = value(i).toString
            weigth = null
            loop.break()
          } else {
            randNum -= weigth(i)
          }
        }

      }
    )
    loop = null
    result
  }

}

