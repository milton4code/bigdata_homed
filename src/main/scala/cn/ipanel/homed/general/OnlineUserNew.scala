package cn.ipanel.homed.general

import cn.ipanel.common.{CluserProperties, DBProperties, SparkSession, StatisticsType, Tables}
import cn.ipanel.utils.{DBUtils, DateUtils, LogUtils, UserUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * 开机用户半小时刻度明细统计
  * create  liujjy
  * time    2019-05-03 0004 21:47
  */
object OnlineUserNew {

  def main(args: Array[String]): Unit = {
    val time = new LogUtils.Stopwatch("OnlineUserNew 开机 半小时功能, ")
    if (args.length != 1) {
      System.err.println("请输入有效日期  例如[20180401]   ")
      System.exit(-1)
    }

    val day = args(0)
    val session = SparkSession("OnlineUserNew")
    val hiveContext = session.sqlContext

    processChart(day, hiveContext)
    //    OnlineUsersAndTimeNew.process(day, hiveContext, homedUserDeviceDF)
    println(time)
  }

  //处理半小时统计图表数据
  private def processChart(day: String, hiveContext: HiveContext): Unit = {

    val nextDay = DateUtils.getAfterDay(day, targetFormat = DateUtils.YYYY_MM_DD)
    val homedUserDeviceDF = UserUtils.getHomedUserDevice(hiveContext, nextDay)

//    homedUserDeviceDF.show(30, false)
    homedUserDeviceDF.persist().registerTempTable("t_users")

    val onlineDF = hiveContext.sql(
      s"""
         |select t1.province_id , t1.province_name, t1.city_id ,t1.city_name,t1.region_id,t1.region_name,
         |t1.device_type,t1.hour,t1.range,t1.user_id,t1.device_id
         |from bigdata.orc_user_details t1
         |where t1.day='$day'
      """.stripMargin)
    onlineDF.registerTempTable("t_user_details")

//    onlineDF.show(16, false)

    val df: DataFrame = CluserProperties.STATISTICS_TYPE match {
      case StatisticsType.STB => statisticsByDevice(hiveContext, day)
      case StatisticsType.MOBILE => statisticsByMoblie(hiveContext, day)
      case StatisticsType.ALL => statisticsAll(hiveContext, day)
    }

    val result = df.selectExpr(s"'$day' as f_date", "f_province_id", "f_city_id", "f_region_id",
      "f_province_name", "f_city_name", "f_region_name", "f_terminal", "f_hour", "f_timerange", "f_user_count")

    try {
      delete(day, Tables.T_ONLINE_DEV_HALFHOUR)
      //保存按半小时维度统计开机用户
      DBUtils.saveToMysql(result, Tables.T_ONLINE_DEV_HALFHOUR, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)

    } catch {
      case ex: Exception =>  ex.printStackTrace()
    }
  }


  //按移动设备统计
  private def statisticsAll(hiveContext: HiveContext, day: String): DataFrame = {
    val df2 = statisticsByMoblie(hiveContext, day).persist()
    if (df2.count() > 0) {
      statisticsByDevice(hiveContext, day).unionAll(df2)
    } else {
      statisticsByDevice(hiveContext, day)
    }
  }

  //按移动设备统计
  private def statisticsByMoblie(hiveContext: HiveContext, day: String): DataFrame = {
    //半小时的数据源依赖于bigdata.orc_user_details 表
    hiveContext.sql(
      s"""
         |select
         | province_id as f_province_id,first(province_name) as f_province_name,
         | city_id as f_city_id, first(city_name) as f_city_name,
         | region_id as f_region_id,first(region_name) as f_region_name,
         | device_type as f_terminal,hour as f_hour,range as f_timerange,
         |COUNT(DISTINCT user_id) as f_user_count
         |from t_user_details
         |where  device_type  not in ("1","2")
         |group by device_type,hour,range,province_id,city_id,region_id
      """.stripMargin)
  }

  //按机顶盒统计
  private def statisticsByDevice(hiveContext: HiveContext, day: String): DataFrame = {
    //半小时的数据源依赖于bigdata.orc_user_details 表
    hiveContext.sql(
      s"""
         |select
         |  province_id as f_province_id,first(province_name) as f_province_name,
         |  city_id as f_city_id, first(city_name) as f_city_name,
         |  region_id as f_region_id,first(region_name) as f_region_name,
         |  device_type as f_terminal,hour as f_hour,range as f_timerange,
         |  COUNT(DISTINCT device_id) as f_user_count
         |from t_user_details
         |where  device_type in ("1","2")
         |group by device_type,hour,range,province_id,city_id,region_id
      """.stripMargin)
  }

  private def delete(day: String, table: String): Unit = {
    val del_sql = s"delete from $table where f_date='$day'"
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, del_sql)
  }

}
