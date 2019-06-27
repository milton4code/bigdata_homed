package cn.ipanel.homed.repots

import cn.ipanel.common.{Constant, SparkSession}
import cn.ipanel.etl.{AppVersionTools, LogConstant}
import cn.ipanel.utils.{DBUtils, DateUtils, RegionUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

/**
  * 开机报表
  * 按照 app 机型分组
  */
object OpenAppReport {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("请输入日期 分区数 [20181111] [30]")
      sys.exit(-1)
    }
    val time = args(0)
    val num = args(1).toInt
    val sparkSession = SparkSession("OpenAppReport")
    val hiveContext = sparkSession.sqlContext
    val month = DateUtils.getFirstDateOfMonth(time)
    val week = DateUtils.getFirstDateOfWeek(time) //周
    val beforeWeek = DateUtils.getNDaysBefore(6, time)
    val beforeMonth = DateUtils.getDateByDays(time, 29)
    val regionCode = RegionUtils.getRootRegion
    val regionInfo = OpenReport.getRegion(hiveContext, regionCode)

    val br = sparkSession.sparkContext.broadcast(AppVersionTools.getDefaultVersion(hiveContext))
    hiveContext.udf.register("defaultVersion", (key: String) => br.value.get(key))
    getOpenAppVersionDetails(num, time, hiveContext: HiveContext, regionInfo: DataFrame)
    println("执行详情成功")
    //每一天基础数据
    getBasicOpenAppVersion(time, hiveContext, regionInfo, num)
    println("每一天基础数据执行成功")
    getOpenAppVersion(time, time, hiveContext, regionInfo, cn.ipanel.common.Tables.t_appversion_report_by_day)
    getOpenAppVersion(week, time, hiveContext, regionInfo, cn.ipanel.common.Tables.t_appversion_report_by_week)
    getOpenAppVersion(month, time, hiveContext, regionInfo, cn.ipanel.common.Tables.t_appversion_report_by_month)
    getOpenAppVersionHistory(beforeWeek, time, hiveContext, regionInfo, cn.ipanel.common.Tables.t_appversion_report_by_history, 3)
    println("执行成功")

  }

  def getBasicOpenAppVersion(time: String, hiveContext: HiveContext, regionInfo: DataFrame, num: Int) = {
    hiveContext.sql("use bigdata")
    //开机301 // 机型M     // MA 厂商 // A app 版本号  H 硬件版本 S 软件版本
    val sql =
      s"""
         |(select * from t_appversion_ca_details)
         |as df
      """.stripMargin
    val info = DBUtils.loadDataFromPhoenix2(hiveContext, sql)
    info.registerTempTable("info")
    val df1 = hiveContext.sql(
      s"""
         |select user_id as userid,device_type as devicetype,
         |if((b.f_phone_model is not null),b.f_phone_model,'${LogConstant.PHONEMODEL}' ) as f_phone_model,
         |region_id as regionid,
         |if((b.f_app_version is not null),b.f_app_version,'${LogConstant.APPVERSION}' ) as f_app_version,
         |if((b.f_hard_version is not null),b.f_hard_version,'${LogConstant.HARDVERSION}' ) as f_hard_version,
         |if((b.f_soft_version is not null),b.f_soft_version,'${LogConstant.SOFTVERSION}' ) as f_soft_version,
         |if((b.f_manufacturer is not null),b.f_manufacturer,'${LogConstant.MAVERSION}' ) as f_manufacturer
         |from orc_user a
         |left join info b
         |on (a.user_id = b.f_user_id and a.device_type=b.f_terminal)
         |where
         |( day ='$time' and
         | a.device_id != 0)
       """.stripMargin)
      .registerTempTable("basicdf1")
    hiveContext.sql("use bigdata")
    hiveContext.sql(s"alter table open_app  drop IF EXISTS PARTITION(day='$time')")
    //    hiveContext.sql(s"alter table  bigdata.open_app  drop IF EXISTS PARTITION(day='$time')")
    hiveContext.sql(
      s"""
         |insert  overwrite table bigdata.open_app  partition(day='$time')
         |select userid,devicetype,f_phone_model,
         |regionid,f_app_version,f_hard_version,f_soft_version,f_manufacturer
         | from
         |basicdf1
         |group by
         |userid,devicetype,f_phone_model,
         |regionid,f_app_version,f_hard_version,f_soft_version,f_manufacturer
                        """.stripMargin)
    //其他
    //    val df2 = hiveContext.sql(
    //      s"""
    //         |select user_id as userid,device_type as devicetype,
    //         |'${LogConstant.PHONEMODEL}'  as f_phone_model,
    //         |region_id as regionid,
    //         |'${LogConstant.APPVERSION}'  as f_app_version,
    //         |'${LogConstant.HARDVERSION}'  as f_hard_version,
    //         |'${LogConstant.SOFTVERSION}'  as f_soft_version
    //         |from orc_user a
    //         |where
    //         |(day ='$time'
    //         |and (not a.source like '%1%') and  a.device_id != 0)
    //              """.stripMargin)
    //    df2.registerTempTable("basicdf")
    //    hiveContext.sql(
    //      s"""
    //         |insert  into table bigdata.open_app  partition(day='$time')
    //         |select userid,devicetype,f_phone_model,
    //         |regionid,f_app_version,f_hard_version,f_soft_version
    //         | from
    //         |basicdf
    //         |group by
    //         |userid,devicetype,f_phone_model,
    //         |regionid,f_app_version,f_hard_version,f_soft_version
    //                            """.stripMargin)

  }


  def getOpenAppVersionDetails(num: Int, time: String, hiveContext: HiveContext, regionInfo: DataFrame) = {
    hiveContext.sql("use bigdata")
    //开机301 // 机型M     // MA 厂商 // A app 版本号
    //3 M 机型_V -> 厂商 4 H 硬件版本_V -> 厂商
    val df = hiveContext.sql(
      s"""
         |select  '$time' as f_date,a.f_user_id,
         |a.f_terminal,a.f_phone_model,a.f_app_version,a.f_region_id, a.f_hard_version,
         |a.f_ca_id, a.f_soft_version,a.f_manufacturer
         |from
         |(
         |select  userid as f_user_id,
         |cast(devicetype as int) as f_terminal,
         |(
         |case  when  exts['MA']  is not null then exts['MA']
         |when exts['H'] is not null  then  defaultVersion(concat(exts['H'],'_V'))
         |when exts['M'] is not null  then  defaultVersion(concat(exts['M'],'_V'))
         |when (defaultVersion(concat(exts['H'],'_V')) is null or defaultVersion(concat(exts['M'],'_V')) is null) then  '${Constant.UNKOWN}'
         |else '${Constant.UNKOWN}' end) as  f_manufacturer,
         |if((exts['M']  is not null),exts['M'] , defaultVersion(exts['H'])) as f_phone_model,
         |if((exts['A']  is  not null),exts['A'] ,'${Constant.UNKOWN}') as  f_app_version,
         |regionid as f_region_id,
         |if((exts['H']  is not null),exts['H'] , defaultVersion('M')) as  f_hard_version,
         |ca as f_ca_id,
         |if((exts['S']  is not null),exts['S'] ,'${Constant.UNKOWN}') as  f_soft_version
         |from orc_report_behavior
         |where reporttype='301' and day = '$time'
         |and exts is not null and ( exts["H"] is not null or exts["M"] is not null)
         |) a
         |group by
         |a.f_user_id,
         |a.f_terminal,a.f_phone_model,
         |a.f_app_version,a.f_region_id, a.f_hard_version,
         |a.f_ca_id, a.f_soft_version,a.f_manufacturer
        """.stripMargin)

    DBUtils.saveDataFrameToPhoenixNew(df, cn.ipanel.common.Tables.t_appversion_ca_details)

    val df1 = regionInfo.join(df, Seq("f_region_id"))
    //历史表
    //    val sql =
    //      s"""
    //         |(select
    //         |f_user_id,f_id_card,f_user_name,f_user_address,f_user_mobile_no,f_terminal
    //         |from t_service_visit_users
    //         |where f_date ='$time' ) as aa
    //         """.stripMargin
    //    val df2 = DBUtils.loadDataFromPhoenix2(hiveContext, sql)
    val finaldf = df1.repartition(num)
      .drop("f_manufacturer")

    //详情表
    DBUtils.saveDataFrameToPhoenixNew(finaldf, cn.ipanel.common.Tables.t_appversion_ca_details_by_day)
  }


  def getOpenAppVersionHistory(startTime: String, endTime: String, hiveContext: HiveContext, regionInfo: DataFrame, tableName: String, ftype: Int) = {
    hiveContext.sql("use bigdata")
    //开机301 // 机型M     // MA 厂商 // A app 版本号
    val df = hiveContext.sql(
      s"""
         |select '$startTime' as f_start_date, '$endTime' as f_end_date,cast(a.devicetype as int ) as f_terminal,
         |a.f_phone_model,
         |a.f_app_version,
         |count(*) as f_user_count,a.regionid as f_region_id,$ftype as f_type,
         |a.f_hard_version,
         |a.f_soft_version
         |from
         |(
         |select userid,devicetype,f_phone_model,f_app_version,regionid,
         |f_hard_version,f_soft_version
         |from open_app
         |where  day between '$startTime' and '$endTime'
         |group by
         |userid,devicetype,f_phone_model,f_app_version,regionid,
         |f_hard_version,f_soft_version
         |) a
         |group by a.devicetype,a.f_phone_model,a.f_soft_version,
         |a.f_app_version,a.regionid,a.f_hard_version
        """.stripMargin)
    val df1 = regionInfo.join(df, Seq("f_region_id"))
    DBUtils.saveDataFrameToPhoenixNew(df1, tableName)
  }

  def getOpenAppVersion(startTime: String, endTime: String, hiveContext: HiveContext, regionInfo: DataFrame, tableName: String) = {
    val monthOfDay = DateUtils.getFirstDateOfMonth(endTime)
    var startTimeNew = ""
    if (startTime == monthOfDay) {
      startTimeNew = endTime.substring(0, 6)
    } else {
      startTimeNew = startTime
    }
    hiveContext.sql("use bigdata")
    //开机301 // 机型M     // MA 厂商 // A app 版本号  H 硬件版本 S 软件版本

    val df = hiveContext.sql(
      s"""
         |select '$startTimeNew' as f_date,cast(a.devicetype as int ) as f_terminal,
         |a.f_phone_model,
         |a.f_app_version,
         |count(*) as f_user_count,a.regionid as f_region_id,
         |a.f_hard_version,
         |a.f_soft_version
         |from
         |(select userid,devicetype,f_phone_model,f_app_version,regionid,
         |f_hard_version,f_soft_version
         |from open_app
         |where  day between '$startTime' and '$endTime'
         |group by
         |userid,devicetype,f_phone_model,f_app_version,regionid,
         |f_hard_version,f_soft_version
         |) a
         |group by a.devicetype,a.f_phone_model,a.regionid,
         | a.f_app_version,a.f_hard_version,a.f_soft_version
            """.stripMargin)
    val df1 = regionInfo.join(df, Seq("f_region_id"))
    DBUtils.saveDataFrameToPhoenixNew(df1, tableName)
  }
}
