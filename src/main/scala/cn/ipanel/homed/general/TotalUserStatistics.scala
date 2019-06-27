package cn.ipanel.homed.general

import cn.ipanel.common._
import cn.ipanel.utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.util.Random

/**
  * 用户总量统计
  * create  liujjy
  * time    2019-05-09 0009 0:16
  */
object TotalUserStatistics {

  def main(args: Array[String]): Unit = {
    val time = new LogUtils.Stopwatch("TotalUserStatistics 用户总量统计 ")
    if (args.length != 1) {
      println("请输入参数, 日期 ,例如[20190101]")
      sys.exit(-1)
    }

    val sparkSession = SparkSession("TotalUserStatistics")
    val sqlContext = sparkSession.sqlContext
    val day = args(0)
    val nextDay = DateUtils.getAfterDay(day, targetFormat = DateUtils.YYYY_MM_DD)

    val allHomedUsersDF = UserUtils.getHomedUserDevice(sqlContext, nextDay)
    val homedInfoDF = UserUtils.getHomeInfo(sqlContext,nextDay).selectExpr("home_id","user_id")

    //关联主账号,确保设备都是唯一
    val disUserDF = allHomedUsersDF.join(homedInfoDF,Seq("home_id","user_id")).persist()

//    //数据导出功能
    val savedDF = disUserDF.selectExpr("cast(home_id as string) f_home_id","user_id f_user_id",
      "f_reg_source","account_name f_account_name", "nick_name f_nick_name",
      "cast(province_id as string) f_province_id","cast(city_id as string) f_city_id",
      "cast(region_id as string )f_region_id", " province_name f_province_name","city_name f_city_name",
      "region_name f_region_name","address_name f_address_name",
      "cast(create_time as string)f_create_time","cast(mobile_no as string) f_mobile_no",
      "certificate_no f_certificate_no","device_id f_device_id",
      "cast(device_type as bigint)f_terminal" ,"uniq_id f_uniq_id","substr(create_time,0,10) f_date")

    DBUtils.saveDataFrameToPhoenixNew(savedDF,"t_user")

     val random = new Random().nextInt(1000)
    //-----------------------数据验证代码
    val dateTime = DateUtils.getNowDate("yyyyMMdd-HHmm")
    allHomedUsersDF.selectExpr( "create_time", "user_id","device_id","uniq_id")
      .map(_.toString().replace("[", "").replace("]", ""))
      .repartition(1).saveAsTextFile(s"/tmp/total_user/$day/$dateTime")
    //-----------------------数据验证代码

    disUserDF.registerTempTable("t1")

    /*
      用户总量统计有两种方式
      1.按机顶盒设备数量统计
      2.按账号(user_id)统计移动端(手机,pad,pc)
     */
    val df = CluserProperties.STATISTICS_TYPE match {
      case StatisticsType.STB => statisticsByDevice(sqlContext)
      case StatisticsType.MOBILE => statisticsByMobile(sqlContext, nextDay)
      case StatisticsType.ALL => statisticsAll(sqlContext, nextDay)
    }

    val result = df.selectExpr(s" '$day' as f_date ", "f_province_id", "f_city_id", "f_region_id",
      "f_province_name", "f_city_name", "f_region_name", "f_reg_source", "nvl(f_user_count,0) f_user_count")

    delete(day, Tables.T_USER_SUMMARY)
    DBUtils.saveToHomedData_2(result, Tables.T_USER_SUMMARY)
//
    allHomedUsersDF.unpersist()

    println(time)
  }

  private def statisticsAll(sqlContext: HiveContext, nextDay: String): DataFrame = {
    val stbDF = statisticsByDevice(sqlContext)
    val mobileDF = statisticsByMobile(sqlContext, nextDay)
    if(mobileDF.count()>1){
      stbDF.unionAll(mobileDF).registerTempTable("t5")
    }else{
      stbDF.registerTempTable("t5")
    }
    sqlContext.sql(
      """
        |select f_province_id,f_city_id,f_region_id,
        |f_province_name,f_city_name,f_region_name,f_reg_source,
        |sum(f_user_count)  f_user_count
        |from t5
        |group by  f_province_id,f_city_id,f_region_id,f_province_name,f_city_name,f_region_name,f_reg_source
      """.stripMargin)

  }

  //根据移动端统计
  private def statisticsByMobile(sqlContext: HiveContext, nextDay: String): DataFrame = {

    val sql = new StringBuilder()
    sql.append(
      """
        |select province_id as f_province_id,city_id as f_city_id,region_id as f_region_id,
        |first(province_name) as f_province_name,first(city_name) as f_city_name, first(region_name) as f_region_name,
        |f_reg_source,
        |nvl( count(distinct user_id) ,0)f_user_count
        |from (
        |   select t1.*
        |   from t1
      """.stripMargin)

    if (Constant.PANYU_CODE.equals(RegionUtils.getRootRegion)) {
      //在番禺，没有绑定家庭的手机账号也是无效用户
      //需要关联到device_info 找到对应设备类型
      UserUtils.getMobileCaBinds(sqlContext, nextDay).registerTempTable("t3")
      sql.append(" join t3 on t1.home_id= t3.home_id ")
    }

    sql.append(
      """
        | where t1.device_type not in (1,2) ) t4
        |group by f_reg_source,province_id,city_id,region_id
      """.stripMargin)
    println("statisticsByMobile -->  " + sql.toString())
    sqlContext.sql(sql.toString())
  }

  //根据stb设备统计
  private def statisticsByDevice(sqlContext: HiveContext): DataFrame = {
    sqlContext.sql(
      """
        |select province_id as f_province_id,city_id as f_city_id,region_id as f_region_id,
        |first(province_name) as f_province_name,first(city_name) as f_city_name, first(region_name) as f_region_name,
        |f_reg_source,
        |count(distinct device_id) f_user_count
        |from t1
        |where device_type in ('1','2')
        |group by f_reg_source,province_id,city_id,region_id
      """.stripMargin)

  }

  private def delete(day: String, table: String): Unit = {
    val del_sql = s"delete from $table where f_date='$day'"
    print("delete=======" + del_sql)
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, del_sql)
  }
}
