package cn.ipanel.homed.general

import cn.ipanel.common.{CluserProperties, DBProperties, GatherType, SparkSession, Tables}
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.ListBuffer
import scala.util.Sorting

/**
  *
  * 开机用户及时长
  * Author :liujjy  
  * Date : 2019-03-19 0019 10:58
  * version: 1.0
  * Describle: 
  **/
private case class UserOnLineIacs(f_user_id: String = "", f_terminal: String = "", f_onLineTime: Long = 0L)

object OnlineUsersAndTime {
  //1,day 2,week,3 month,4 year
  private val DAY = "1"
  private val WEEK = "2"
  private val MONTH = "3"
  private val YEAR = "4"


  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("参数不正确,请输入日期 [20180528]")
      sys.exit(-1)
    }
    val sparkSession = SparkSession("OnlineTest")
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    val day = args(0)
    process(day, sqlContext)
  }

  def process(day: String, sqlContext: HiveContext): Unit = {
    delete(day)
    statisticsByDay(day, DAY, sqlContext)
    statisticsByCountType(day, WEEK, sqlContext)
    statisticsByCountType(day, MONTH, sqlContext)
    statisticsByCountType(day, YEAR, sqlContext)
  }

  /**
    * 根据统计类型 来统计开机用户数和播放时长
    * WEEK MONTH YEAR
    */
  private def statisticsByCountType(day: String, count_type: String, sqlContext: HiveContext) = {
    val regionCode = CluserProperties.REGION_CODE
    val userCountDF = getOnlineUserByType(day, count_type, sqlContext)
    val playTimeDF = getPlayTimeByCountType(day, count_type, sqlContext)
    val resultDF = userCountDF.join(playTimeDF, Seq("f_terminal", "f_region_id"), "left")
      .selectExpr(s"'$day' as f_date", s"'$count_type' f_type", "f_terminal", "f_province_id", "f_province_name",
        "f_city_id", "f_city_name", "f_region_id", "f_region_name", "f_user_count",
        "nvl(f_play_time,0,f_play_time) f_play_time")
    if(regionCode.endsWith("0000")){ //省
      val _resultDF = resultDF.filter(s"f_province_id = '$regionCode'  ")
//      _resultDF.show()
      DBUtils.saveToHomedData_2(_resultDF, Tables.T_ONLINE_USER_TIME)
    }else { //市网
      val _resultDF = resultDF.filter(s"f_city_id = '$regionCode' ")
//      _resultDF.show()
      DBUtils.saveToHomedData_2(_resultDF, Tables.T_ONLINE_USER_TIME)
    }
  }

  private def getPlayTimeByCountType(day: String, count_type: String, sqlContext: HiveContext) = {
    val startDay = getStartDay(day, count_type)
    val querySql =
      s"""
         |(SELECT sum(f_play_time) f_play_time ,f_terminal,f_region_id
         |from t_online_user_time
         |WHERE f_date>="$startDay" and f_date <= "$day" and f_type='$DAY'
         |GROUP BY f_terminal,f_region_id,f_city_id) t
      """.stripMargin

    DBUtils.loadMysql(sqlContext, querySql, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)

  }

  /**
    * 按天 来计算在线人数 和播放时长
    * 1.按天统计维度,在线人数和播放是时长一起计算
    */
  def statisticsByDay(day: String, count_type: String, sqlContext: HiveContext): Unit = {
    val regionCode = CluserProperties.REGION_CODE
    sqlContext.sql("use bigdata")
    //人数
    val userCountDF = getOnlineUserByType(day, count_type, sqlContext)
    //    val playTimeDF = getPlayTimeByDay(day, sqlContext)
    val playTimeDF = countOnLineTimeNew(day, count_type, sqlContext) // 统计用户在线时长
    val resultDF = userCountDF.join(playTimeDF, Seq("f_terminal", "f_region_id"), "left")
      .selectExpr(s"'$day' as f_date", "f_type", "f_terminal", "f_province_id", "f_province_name",
        "f_city_id", "f_city_name", "f_region_id", "f_region_name", "f_user_count",
        "nvl(f_play_time,0,f_play_time) f_play_time")

    if(regionCode.endsWith("0000")){ //省
      val _resultDF = resultDF.filter(s"f_province_id = '$regionCode'  ")
      _resultDF.show()
      DBUtils.saveToHomedData_2(_resultDF, Tables.T_ONLINE_USER_TIME)
    }else { //市网
      val _resultDF = resultDF.filter(s"f_city_id = '$regionCode' ")
      _resultDF.show()
      DBUtils.saveToHomedData_2(_resultDF, Tables.T_ONLINE_USER_TIME)
    }


  }

  /**
    * 按天统计四种业务播放时长
    */
  private def getPlayTimeByDay(day: String, sqlContext: HiveContext) = {
    //播放时长
    val playTimesDF = sqlContext.sql(
      s"""
         |select sum(playtime) f_play_time ,devicetype as f_terminal,regionid as f_region_id
         |from orc_video_play
         |where playtype in ( '${GatherType.LIVE_NEW}','${GatherType.TIME_SHIFT_NEW}','${GatherType.DEMAND_NEW}','${GatherType.LOOK_BACK_NEW}')
         |and day=$day
         |group by devicetype,regionid
      """.stripMargin)
    playTimesDF
  }

  /**
    * 根据统计类型获取在线(开机)人数
    */
  private def getOnlineUserByType(day: String, count_type: String, sqlContext: HiveContext) = {
    val startDay = getStartDay(day, count_type)
    sqlContext.sql(
      s"""
         |select count(distinct(user_id)) f_user_count, '$count_type' as f_type,
         |terminal f_terminal,province_id f_province_id,first(province_name) f_province_name,
         |city_id f_city_id,first(city_name) f_city_name,region_id f_region_id, first(region_name)  f_region_name
         |from bigdata.orc_login_user
         |where day >= $startDay and day <= $day
         |group by terminal,province_id,city_id,region_id
        """.stripMargin)
  }

  private def getStartDay(day: String, count_type: String): String = {
    val dateTime = DateUtils.dateStrToDateTime(day, DateUtils.YYYYMMDD)
    val startDay = count_type match {
      case DAY => day
      case WEEK => dateTime.plusDays(-dateTime.getDayOfWeek).toString(DateUtils.YYYYMMDD)
      case MONTH => dateTime.plusDays(-dateTime.getDayOfMonth).toString(DateUtils.YYYYMMDD)
      case YEAR => dateTime.plusDays(-dateTime.getDayOfYear).toString(DateUtils.YYYYMMDD)
    }
    startDay
  }

  private def delete(day: String): Unit = {
    val sql = s"delete from ${Tables.T_ONLINE_USER_TIME} where f_date='$day'"
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, sql)

  }

  def getOnlineTime(loginTime_time: String, logoutTime: String, newDay: String): Long = {
    val f_login_time = if (loginTime_time < newDay) DateUtils.dateToUnixtime(newDay) else DateUtils.dateToUnixtime(loginTime_time)
    val f_logout_time = DateUtils.dateToUnixtime(logoutTime)
    val onLineTime = f_logout_time - f_login_time
    Math.ceil(onLineTime).toLong
  }

  /**
    * 用户日均开机时长f
    * val savedDF = onlineUserDF.unionAll(offlineUsefrDF)
    * .selectExpr(" f_user_id", " f_device_id","f_device_type"," f_home_id","f_srvtype_id",
    * "f_login_time"," f_logout_time"," f_ipaddress"," f_port"," f_region_id"
    * ,s"'$day' day ")
    */

  def countOnLineTimeNew(day: String, count_type: String, hiveContext: SQLContext): DataFrame = {
    hiveContext.udf.register("get_online_times", (loginTime_time: String, logoutTime: String, newDay: String) => getOnlineTime(loginTime_time, logoutTime, newDay))
    val newDay = DateUtils.getStartTimeOfDay(day)
    val days = DateUtils.transformDateStr(day)
    val startDay = getStartDay(day, count_type)
    val onlineUserDF = hiveContext.sql(
      s"""
         |select f_device_type as f_terminal,f_region_id,sum(f_play_time) as f_play_time
         |from
         |(
         |select f_user_id ,f_device_id,f_device_type,get_online_times(f_login_time,f_logout_time,'$newDay') as f_play_time ,
         |substr(ltrim(cast(f_region_id as string)),1,6) as f_region_id
         |from bigdata.t_user_online_history
         |where day = '$day'
         |)t
         |group by f_device_type,f_region_id
        """.stripMargin)
    onlineUserDF
  }

  def countOnLineTime(day: String, count_type: String, hiveContext: SQLContext): DataFrame = {
    import hiveContext.implicits._
    val newDay = DateUtils.getStartTimeOfDay(day)
    val days = DateUtils.transformDateStr(day)
    //.select("user_id","province_id","province_name","city_id","city_name","region_id","region_name","login_time","terminal","day")
    val startDay = getStartDay(day, count_type)
    val onlineUserDF = hiveContext.sql(
      s"""
         |select user_id as f_user_id,
         |terminal f_terminal,
         |region_id as f_region_id
         |from bigdata.orc_login_user
         |where day >= $startDay and day <= $day
        """.stripMargin)
    val dataDF = hiveContext.sql(
      s"""
         |SELECT report_time,key_word, exts['UserID'] f_user_id, exts['DeviceID'] f_device_id,exts['DeviceType'] f_device_type,
         |exts['HomeID'] f_home_id, exts['SrvTypeID'] f_srvtype_id, exts['LoginTime'] f_login_time,
         |if(exts['LogoutTime'] is null or exts['LogoutTime']='null','$days 23:59:59',exts['LogoutTime'])  f_logout_time,exts['IPAddr'] f_ipaddress,
         |exts['Port'] f_port,substr(exts['RegionID'],1,6) f_region_id
         |FROM bigdata.orc_iacs  where day= $day and (key_word='UserOffline' or key_word='UserOnline')
          """.stripMargin)

    val userOnLineTimeDF = dataDF.mapPartitions(partition => {
      val list = new ListBuffer[(String, Array[String])]
      partition.foreach(x => {
        val report_time = x.getAs[String]("report_time")
        val key_word = x.getAs[String]("key_word")
        val f_user_id = x.getAs[String]("f_user_id")
        val f_device_id = x.getAs[String]("f_device_id")
        val f_device_type = x.getAs[String]("f_device_type")
        val f_login_time = x.getAs[String]("f_login_time")
        val f_logout_time = x.getAs[String]("f_logout_time")
        val key = f_user_id + "," + f_device_id + "," + f_device_type
        val value = report_time + "-->" + key_word + "-->" + f_login_time + "-->" + f_logout_time
        list += ((key, Array(value)))
      })
      list.iterator
    }).reduceByKey((a, b) => sort(a, b))
      .mapPartitions(partition => {
        val lists = new ListBuffer[UserOnLineIacs]
        partition.foreach(x => {
          val keyArr = x._1.split(",")
          val f_user_id = keyArr(0)
          val f_device_id = keyArr(1)
          val f_device_type = keyArr(2)
          val arr = x._2
          var loginTime = 0L
          var i = 0
          for (e <- arr) {
            i += 1
            val valueArr = e.split("-->")
            val key_word = valueArr(1)
            val f_login_time = if (valueArr(2) < newDay) DateUtils.dateToUnixtime(newDay) else DateUtils.dateToUnixtime(valueArr(2))
            val f_logout_time = DateUtils.dateToUnixtime(valueArr(3))
            if (key_word.equals("UserOffline")) {
              loginTime += f_logout_time - f_login_time
            }
            if (i == arr.length && key_word.equals("UserOnline")) {
              loginTime += f_logout_time - f_login_time
            }
          }
          lists += UserOnLineIacs(f_user_id, f_device_type, loginTime)
        })
        lists.iterator
      }).toDF()

    onlineUserDF.join(userOnLineTimeDF, Seq("f_user_id", "f_terminal"), "left")
      .selectExpr("f_user_id", "f_terminal", "f_region_id", "nvl(f_onLineTime,86400) as f_onLineTime")
      .groupBy("f_terminal", "f_region_id").sum("f_onLineTime")
      .withColumnRenamed("sum(f_onLineTime)", "f_play_time")
  }

  def sort(arr1: Array[String], arr2: Array[String]): Array[String] = {
    val buffer = arr1.toBuffer
    buffer ++= arr2
    val result = buffer.toArray
    Sorting.quickSort(result)
    result
  }

}
