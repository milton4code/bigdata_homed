package cn.ipanel.homed.repots

import cn.ipanel.common._
import cn.ipanel.utils.{DBUtils, DateUtils, UserUtils}
import org.apache.spark.sql.hive.HiveContext

/**
  * 家庭开户报表统计
  * 主要读取iusm中的基础表，获取用户基本信息，然后做关联
  *
  * @author Liu gaohui
  * @date 2018/01/10
  **/
object HomeOpen {
  def main(args: Array[String]): Unit = {
    //    sc.setLogLevel("WARN")
    if (args.size != 1) {
      println("请输入指定日期 ，例如 20190101")
      sys.exit(-1)
    }
    val sparkSession = SparkSession("HomeOpen", CluserProperties.SPARK_MASTER_URL)
    val hiveContext = sparkSession.sqlContext
    val day = args(0)

    process(hiveContext, day )
    sparkSession.stop()
  }

  def process(hiveContext: HiveContext, day: String): Unit = {
    val day2 = DateUtils.transformDateStr(day)
    val nextDay =  DateUtils.getAfterDay(day,targetFormat = DateUtils.YYYY_MM_DD)

    //读取iusm中的account_info,home_info和address_info表，获取用户的基本信息
    val homesql =
      s"""(
         |select address_id,home_name as f_home_name,REPLACE(REPLACE(group_ids ,'[',''),']','') as f_group_ids,
         |member_count as f_member_count,status as f_status,home_id as f_home_id, f_create_time as f_open_account_time
         |FROM home_info
         |WHERE f_create_time between  '$day2' and '$nextDay' and master_account > 0
         |) as home
        """.stripMargin
    val homeInfoDF = DBUtils.loadMysql(hiveContext, homesql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)

    import hiveContext.implicits._
    val userDF = UserUtils.getHomedUserDevice(hiveContext, nextDay)
    val result = userDF.alias("a").join(homeInfoDF.alias("b"), $"a.home_id" === $"b.f_home_id")
      .selectExpr("a.account_name as f_account_name","b.f_open_account_time","b.f_status","b.f_home_id"
        ,"b.f_home_name","b.f_group_ids","b.f_member_count","a.address_name as f_address_name",
        "a.region_id f_region_id","a.city_id f_city_id","a.province_id as f_province_id",
        "a.region_name f_region_name","a.city_name as f_city_name","a.province_name as f_province_name")
    delete(day)
    DBUtils.saveToHomedData_2(result, Tables.mysql_home_open) //存入到数据库中
  }

  private def delete(day: String) = {
    println("=======delete=============")
    val dateTime = DateUtils.dateStrToDateTime(day, DateUtils.YYYYMMDD)
    val startDay = dateTime.toString(DateUtils.YYYY_MM_DD_HHMMSS)
    val endDay = dateTime.plusDays(1).toString(DateUtils.YYYY_MM_DD_HHMMSS)
    val sql = s"delete from t_home_open where f_open_account_time >= '$startDay' and f_open_account_time <= '$endDay'"
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, sql)
  }
}
