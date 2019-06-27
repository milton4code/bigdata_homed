package cn.ipanel.etl

import cn.ipanel.common.Constant
import cn.ipanel.homed.repots.RecommendSearchReport.str_to_map
import cn.ipanel.utils.DBUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object UserDeviceInfo {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("请输入日期和boolean值 如20190217 、true/false(true 代表是第一次启动，false 则说明不是)")
      System.exit(-1)
    }
    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.codegen", "true")
    conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
    conf.set("spark.sql.autoBroadcastJoinThreshold", "209715200")
    conf.setAppName("UserDeviceInfo")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val day = args(0) // 时间
    val isFirst = args(1).toBoolean //是否是第一次
    val lines = getData(sqlContext, day)
    if (isFirst) {
      dealOpenDetail_arate(sc, day, sqlContext)
    }
    dealOpenDetail(lines, day, sqlContext)
  }

  def getData(sqlContext: SQLContext, day: String): DataFrame = {
    val sql =
      s"""
         |select * from bigdata.orc_user_report
         |where day='$day' and  servicetype='301'
      """.stripMargin
    sqlContext.sql(sql)
  }

  //处理301开机数据得到用户和app版本信息及设备相关信息
  def dealOpenDetail(lines: DataFrame, day: String, hiveContext: SQLContext) {
    lines.registerTempTable("deviceInfo_table")
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

  case class USER_OPEN(starttime: String = "", userid: String = "", deviceid: String = "", paras: mutable.HashMap[String, String])

  //处理301开机数据得到用户和app版本信息及设备相关信息(第一次启动时，获取更多用户的设备信息)
  def dealOpenDetail_arate(sc: SparkContext, day: String, hiveContext: SQLContext) {
    import hiveContext.implicits._
    //    文件路径
    val dayNew = day.substring(0, 6) + "*"
    val path = LogConstant.HDFS_ARATE_LOG + dayNew + "/*"
    val lines = sc.textFile(path).filter(x => x.startsWith("<?><[0301,"))
      .map(_.substring(3).replaceAll("(<|>|\\[|\\]|\\(|\\))", "").replaceAll("\\s*|\t|\r|\n", ""))
      .filter(x => {
        var b = false
        if (x.contains("|")) {
          val arr = x.split("\\|")
          if (arr.length == 2) {
            val userid = arr(0).split(",")(2)
            val expandDate = arr(1)
            val maps = str_to_map(expandDate)
            if (maps.contains("M") && maps.contains("A")) b = true
          }
        }
        b
      }).map(x => {
      val user_info_arr = x.split("\\|")(0).split(",")
      val expand_info = x.split("\\|")(1)
      if (user_info_arr.length >= 5) {
        val starttime = user_info_arr(1)
        val user_id = user_info_arr(2)
        val deviced_id = user_info_arr(4)
        val maps = str_to_map(expand_info)
        USER_OPEN(starttime, user_id, deviced_id, maps)
      } else {
        USER_OPEN("", "", "", null)
      }
    }).filter(x => x.userid != "").toDF().registerTempTable("user_open_table")

    val df = hiveContext.sql(
      s"""
         |select
         |if((a.f_phone_model is not null),a.f_phone_model,'${Constant.UNKOWN}') as f_phone_model,
         |if((a.f_manufacturer is not null),a.f_manufacturer,'${Constant.UNKOWN}') as f_manufacturer,
         |if((a.f_app_version is not null),a.f_app_version,'${Constant.UNKOWN}') as f_app_version,
         |if(a.f_device_hardware_version is not null,a.f_device_hardware_version,'${Constant.UNKOWN}') as f_device_hardware_version,
         |if(a.f_device_software_version is not null,a.f_device_software_version,'${Constant.UNKOWN}') as f_device_software_version,
         |a.deviceid as f_device_id,
         |a.userid as f_user_id
         |from
         |(select userid,paras['M'] as f_phone_model,
         |paras['MA'] as f_manufacturer,paras['A'] as f_app_version,
         |paras['H'] as f_device_hardware_version,
         |paras['S'] as f_device_software_version,
         |deviceid,starttime,
         |row_number() over(partition by userid ,deviceid order by starttime desc ) as num
         |from user_open_table
         |) a
         |where num <2
        """.stripMargin)

    //    df.show(100, false)
    DBUtils.saveDataFrameToPhoenixNew(df, "t_user_device_info")
  }
}
