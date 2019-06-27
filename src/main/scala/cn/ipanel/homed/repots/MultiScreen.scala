package cn.ipanel.homed.repots

import cn.ipanel.common.SparkSession
import cn.ipanel.etl.LogConstant
import cn.ipanel.utils.DateUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

case class MultiRdd(f_hour: String, f_timerange: String, f_program_id: String, f_user_id: String, time: String, f_device_id: String)

/**
  * 多屏互动
  */
object MultiScreen {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("请输入有效日期,例如【20180401】")
      System.exit(-1)
    }
    val sparkSession = SparkSession("MultiScreen")
    val sc = sparkSession.sparkContext
    val hiveContext = sparkSession.sqlContext
    val time = args(0)
    val logPathDate: String = DateUtils.transformDateStr(time, DateUtils.YYYYMMDD, DateUtils.YYYY_MM_DD)
    val rdd = sc.textFile(LogConstant.IACS_LOG_PATH + logPathDate + "/*")
    //得到终端
    val deviceToTerminal = fromDeviceGetTerminal(hiveContext, time)
    //通过用户id得到区域
    val regionDF = getRegionDF(hiveContext, time)
    iacsActMultiScreen(deviceToTerminal, regionDF, hiveContext, rdd, time)
    sparkSession.stop()
  }

  //[24781]2018-05-03 18:22:31:952 - [INFO]
  // - ActMultiScreen ActionType=10151,SrcUserId=50312151,SrcDeviceId=1000358618,
  // DstUserId=50312151,DstDeviceId=2000371722,ProgramId=100003957
  def iacsActMultiScreen(deviceToTerminal: DataFrame, regionDF: DataFrame, sqlContext: HiveContext, rdd: RDD[String], time: String) = {
    import sqlContext.implicits._
    val lines = rdd.map(_.split(","))
      .filter(x => x(0).contains("ActMultiScreen") && (x(0).contains("ActionType=10151") || x(0).contains("ActionType=10102")) && x.length == 6 && x(5) != "ProgramId=0")
    val info = lines.map(v => {
      val hour = v(0).substring(v(0).indexOf("]") + 11, v(0).indexOf("]") + 14)
      var timerange = v(0).substring(v(0).indexOf("]") + 15, v(0).indexOf("]") + 17)
      timerange = if (timerange.toInt > 30) "60" else "30"
      val time = v(0).substring(v(0).indexOf("]") + 1, v(0).indexOf("]") + 19)
      val programid = if (v(5).split("=").length == 2) v(5).split("=")(1) else "0"
      val deviceid = if (v(2).split("=").length == 2) v(2).split("=")(1) else "0"
      val userid = if (v(1).split("=").length == 2) v(1).split("=")(1) else "0"
      MultiRdd(hour, timerange, programid, userid, time, deviceid)
    }).toDF()
    info.join(deviceToTerminal, Seq("f_device_id"), "inner")
      .join(regionDF, Seq("f_user_id"), "inner")
      .registerTempTable("final_df")
    val sql1 =
      s"""
         |insert overwrite table t_multi_screen partition(day='$time')
         |select a.f_user_id,a.f_region_id,a.f_terminal,a.f_program_id,
         |a.f_hour,a.f_timerange,
         |count(a.*) as f_screen
         |from final_df a
         |group by a.f_terminal,a.f_program_id,a.f_hour,a.f_timerange,
         |a.f_region_id,a.f_user_id
         |""".stripMargin
    writeToHive(sql1, sqlContext, "t_multi_screen", time)
  }

  //得到终端信息
  def fromDeviceGetTerminal(hiveContext: HiveContext, time: String) = {
    hiveContext.sql("use bigdata")
    val deviceDF = hiveContext.sql(
      s"""
         |select deviceId as f_device_id,
         |deviceType as f_terminal
         |from orc_video_play
         |where day='$time'
         |group by deviceId,deviceType
            """.stripMargin)
    deviceDF
  }

  //得到区域信息
  def getRegionDF(hiveContext: HiveContext, time: String) = {
    hiveContext.sql("use bigdata")
    val regionDF = hiveContext.sql(
      s"""
         |select userId as f_user_id,
         |regionId as f_region_id
         |from orc_video_play
         |where day='$time'
         |group by  userId,regionId
            """.stripMargin)
    regionDF
  }

  /**
    * 写入Hive表方法
    */
  def writeToHive(sql: String, sqlContext: SQLContext, tableName: String, day: String) = {
    sqlContext.sql("use userprofile")
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlContext.sql(s"alter table $tableName drop IF EXISTS PARTITION(day='$day')")
    sqlContext.sql(sql)
  }
}

