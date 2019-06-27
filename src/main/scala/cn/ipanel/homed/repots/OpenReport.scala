package cn.ipanel.homed.repots

import cn.ipanel.common.{Constant, DBProperties, SparkSession}
import cn.ipanel.etl.AppVersionTools
import cn.ipanel.utils.{DBUtils, DateUtils, RegionUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object OpenReport {
  /**
    * 开机报表
    * 按照厂商 app 机型分组
    */
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("请输入日期 [20181111]")
      sys.exit(-1)
    }
    val time = args(0)
    val sparkSession = SparkSession("OpenReport")
    val hiveContext = sparkSession.sqlContext
    val month = DateUtils.getFirstDateOfMonth(time)
    val week = DateUtils.getFirstDateOfWeek(time) //周
    val beforeWeek = DateUtils.getNDaysBefore(6, time)
    val regionCode = RegionUtils.getRootRegion
    val regionInfo = getRegion(hiveContext, regionCode)
    val br = sparkSession.sparkContext.broadcast(AppVersionTools.getDefaultVersion(hiveContext))
    hiveContext.udf.register("defaultVersion", (key: String) => br.value.get(key))
    getOpenManufacturer(time, time, hiveContext, regionInfo, cn.ipanel.common.Tables.t_manufacturer_report_by_day)
    getOpenManufacturer(week, time, hiveContext, regionInfo, cn.ipanel.common.Tables.t_manufacturer_report_by_week)
    getOpenManufacturer(month, time, hiveContext, regionInfo, cn.ipanel.common.Tables.t_manufacturer_report_by_month)
    getOpenManufacturerHistory(beforeWeek, time, hiveContext, regionInfo, cn.ipanel.common.Tables.t_manufacturer_report_by_history, 3)
    println("执行成功汇总")
  }

  def getOpenManufacturer(startTime: String, endTime: String, hiveContext: HiveContext, regionInfo: DataFrame, tableName: String) = {
    val monthOfDay = DateUtils.getFirstDateOfMonth(endTime)
    var startTimeNew = ""
    if (startTime == monthOfDay) {
      startTimeNew = endTime.substring(0, 6)
    } else {
      startTimeNew = startTime
    }

   hiveContext.sql("use bigdata")
//    //开机301 // 机型M     // MA 厂商 // A app 版本号
//    val df = hiveContext.sql(
//      s"""
//         |select '$startTimeNew' as f_date,cast(a.devicetype as int ) as f_terminal,
//         |if((a.f_phone_model is not null),a.f_phone_model,'${Constant.UNKOWN}') as f_phone_model,
//         |if((a.f_manufacturer is not null),a.f_manufacturer,'${Constant.UNKOWN}') as f_manufacturer,
//         |if((a.f_app_version is not null),a.f_app_version,'${Constant.UNKOWN}') as f_app_version,
//         |count(*) as f_user_count,a.regionid as f_region_id
//         |from
//         |(select userid,devicetype,exts['M'] as f_phone_model,
//         |if(exts['MA']= null or exts['MA']='' ,defaultVersion(concat(exts['MA'],'_V')),exts['MA'] ) as f_manufacturer,
//         |exts['A'] as f_app_version,
//         |regionid
//         |from  orc_report_behavior
//         |where reporttype='301' and day between '$startTime' and '$endTime'
//         |and exts is not null
//         |group by userid,devicetype,exts['M'],exts['MA'],regionid,
//         |exts['A']) a
//         |group by a.devicetype,a.f_phone_model,
//         |a.f_manufacturer,a.f_app_version,a.regionid
//        """.stripMargin)
val df = hiveContext.sql(
  s"""
     |select '$startTimeNew' as f_date,cast(a.devicetype as int ) as f_terminal,
     |a.f_phone_model,
     |a.f_manufacturer,
     |a.f_app_version,
     |count(*) as f_user_count,a.regionid as f_region_id
     |from
     |(select userid,devicetype,f_phone_model,f_app_version,regionid,
     |f_manufacturer
     |from open_app
     |where  day between '$startTime' and '$endTime'
     |group by
     |userid,devicetype,f_phone_model,f_app_version,regionid,
     |f_manufacturer
     |) a
     |group by a.devicetype,a.f_phone_model,a.regionid,
     | a.f_manufacturer,a.f_app_version
            """.stripMargin)
    val df1 = regionInfo.join(df, Seq("f_region_id"))
    DBUtils.saveDataFrameToPhoenixNew(df1, tableName)
  }

  def getOpenManufacturerHistory(startTime: String, endTime: String, hiveContext: HiveContext, regionInfo: DataFrame, tableName: String, ftype: Int) = {
    hiveContext.sql("use bigdata")
    //开机301 // 机型M     // MA 厂商 // A app 版本号
//    val df = hiveContext.sql(
//      s"""
//         |select '$startTime' as f_start_date, '$endTime' as f_end_date,cast(a.devicetype as int ) as f_terminal,
//         |if((a.f_phone_model is not null),a.f_phone_model,'${Constant.UNKOWN}') as f_phone_model,
//         |if((a.f_manufacturer is not null),a.f_manufacturer,'${Constant.UNKOWN}') as f_manufacturer,
//         |if((a.f_app_version is not null),a.f_app_version,'${Constant.UNKOWN}') as f_app_version,
//         |count(*) as f_user_count,a.regionid as f_region_id,$ftype as f_type
//         |from
//         |(select userid,devicetype,exts['M'] as f_phone_model,
//         |if(exts['MA']= null or exts['MA']='' ,defaultVersion(concat(exts['MA'],'_V')),exts['MA'] ) as f_manufacturer,
//         |exts['A'] as f_app_version,regionid
//         |from  orc_report_behavior
//         |where reporttype='301' and day between '$startTime' and '$endTime'
//         |and exts is not null
//         |group by userid,devicetype,exts['M'],exts['MA'],regionid,
//         |exts['A']) a
//         |group by a.devicetype,a.f_phone_model,
//         |a.f_manufacturer,a.f_app_version,a.regionid
//        """.stripMargin)
val df = hiveContext.sql(
  s"""
     |select '$startTime' as f_start_date, '$endTime' as f_end_date,cast(a.devicetype as int ) as f_terminal,
     |a.f_phone_model,
     |a.f_manufacturer,a. f_app_version,
     |count(*) as f_user_count,a.regionid as f_region_id,$ftype as f_type
     |from
     |(
     |select userid,devicetype,f_phone_model,f_app_version,regionid,f_manufacturer
     |from open_app
     |where  day between '$startTime' and '$endTime'
     |group by
     |userid,devicetype,f_phone_model,f_app_version,regionid,
     |f_manufacturer
     |) a
     |group by a.devicetype,a.f_phone_model,a.f_manufacturer,
     |a.f_app_version,a.regionid
        """.stripMargin)
    val df1 = regionInfo.join(df, Seq("f_region_id"))
    DBUtils.saveDataFrameToPhoenixNew(df1, tableName)
  }

  def getRegion(hiveContext: HiveContext, regionCode: String) = {
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
         |cast(b.f_city_id as string) as f_city_id,b.f_city_name,
         |cast(c.f_region_id as string) as f_region_id,c.f_region_name
         |from province a
         |join city b on a.f_province_id = b.province_id
         |join region c on b.f_city_id = c.city_id
          """.stripMargin)
    val basic_df = basic_df1.distinct()
    basic_df
  }

}
