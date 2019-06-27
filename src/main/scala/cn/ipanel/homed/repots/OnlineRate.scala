package cn.ipanel.homed.repots

import cn.ipanel.common.{DBProperties, GatherType, SparkSession}
import cn.ipanel.utils.{DBUtils, DateUtils, RegionUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext


import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

case class UserDevice(f_date: String, f_terminal: String, f_province_id: Long, f_province_name: String, f_city_id: Long, f_city_name: String, f_region_id: Long, f_region_name: String, f_demand_user_id_count: Int, f_lookback_user_id_count: Int, f_live_user_id_count: Int, f_time_shift_user_id_count: Int, f_demand_device_id_count: Int, f_lookback_device_id_count: Int, f_live_device_id_count: Int, f_time_shift_device_id_count: Int)

case class UserRdd(f_user_id: String, f_region_id: Int, f_hour: Int, f_timerange: String, f_device_id: String, f_terminal: Int, f_play_type: String, f_program_id: String, f_start_time: String, f_play_time: Long, f_play_time1: Long, videoPlay: Int, f_column_id: String)

/**
  * 点播   报表  图表
  * t_demand_online_report
  * t_demand_watch_statistics_report
  * 回看  报表  图表
  * t_lookback_online_report
  * t_look_back_statistics_report
  */
@deprecated
object OnlineRate {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("OnlineRate")
    val sc = sparkSession.sparkContext
    val hiveContext = sparkSession.sqlContext
    val regionCode = RegionUtils.getRootRegion
    val regionInfo = LookBackReport.getRegionInfo(hiveContext, regionCode)
    val time = if (args.length == 1) args(0) else DateUtils.getYesterday()
    //从数据库读取信息
    //回看
    val backMeizi = sc.broadcast(LookBackReport.getLookBackMeiZi(hiveContext))
    //点播
    val demandMeiZiInfoDF = sc.broadcast(DemandWatch.getDemandMeiZi(hiveContext))
    // 栏目
    val columnMap = DemandWatch.getColumnInfo(hiveContext)
    val columndf = DemandWatch.getColumn(hiveContext: HiveContext)
    // 得到基本数据信息  以半小时为单位
    val user_df_basic1 = getBasicInfo(time, hiveContext, regionInfo)
    //回看基本信息
    val basicLookBack = LookBackReport.getBasicDF(time, hiveContext, backMeizi.value, regionInfo, regionCode, user_df_basic1)
    //分享
    val userBehaviour = getDemandBehaviour(time, hiveContext)
    //多屏互动
    val multiScreen = getMultiScreen(hiveContext, time)
    // 回看汇总表t_look_back_statistics_report
    LookBackReport.getLookBackReport(basicLookBack, hiveContext, userBehaviour, multiScreen)
    //计算在线人数的  人数按内容分
    LookBackReport.getLookBackUser(basicLookBack, hiveContext)
    // 点播基本信息
    val basicDemand = DemandWatch.getBasicDF(time, hiveContext, demandMeiZiInfoDF.value, user_df_basic1, columnMap)
    //得到点播的表格 t_demand_watch_statistics_report
    DemandWatch.getDemandWatchBasic(basicDemand, hiveContext, userBehaviour, multiScreen)
    //按内容类型分
    DemandWatch.getDemandUser(basicDemand, hiveContext)
    sc.stop()
  }

  def getMultiScreen(hiveContext: HiveContext, time: String) = {
    hiveContext.sql("use userprofile")
    hiveContext.sql(
      s"""
         |select * from t_multi_screen where day = '$time'
      """.stripMargin)

  }

  def getBasicInfo(time: String, hiveContext: HiveContext, regionInfo: DataFrame) = {
    import hiveContext.implicits._
    hiveContext.sql("use bigdata")
    val user_basic = hiveContext.sql(
      s"""
         |select userId as f_user_id,deviceId as f_device_id,
         |regionId as f_region_id,
         |hour(startTime) as f_hour,
         |(case when minute(startTime)>30 then '60' else '30' end) as f_timerange,
         |deviceType as f_terminal,playType as f_play_type,
         |serviceId as f_program_id,endTime,
         |startTime as f_start_time,playTime as f_play_time,
         |if ((ext['column_id'] is null),'0',ext['column_id'])  as f_column_id
         |from orc_video_play
         |where day='$time'
            """.stripMargin)
    val user_timerange = user_basic.map(x => {
      val f_user_id = x.getAs[String]("f_user_id")
      val f_hour = x.getAs[Int]("f_hour")
      val f_timerange = x.getAs[String]("f_timerange")
      val f_region_id = x.getAs[String]("f_region_id").toInt
      val f_device_id = x.getAs[Long]("f_device_id").toString
      val f_terminal = x.getAs[String]("f_terminal").toInt
      val f_play_type = x.getAs[String]("f_play_type")
      val f_program_id = x.getAs[Long]("f_program_id").toString
      val f_start_time = x.getAs[String]("f_start_time")
      val f_play_time = x.getAs[Long]("f_play_time")
      val f_column_id = x.getAs[String]("f_column_id")
      val videoPlay = 1
      UserRdd(f_user_id, f_region_id, f_hour, f_timerange, f_device_id, f_terminal, f_play_type, f_program_id, f_start_time, f_play_time, f_play_time, videoPlay, f_column_id)
    }).map(x => {
      val listBuffer = new ListBuffer[(String, String, Int, String, String, Int, String, Long, Long, String, Int, Int, String)]()
      val initialTime = if (getTimeSecond(x.f_start_time) > 1800) {
        (getTimeSecond(x.f_start_time) - 1800)
      } else getTimeSecond(x.f_start_time)
      var playTime = 0L
      var time = initialTime
      var playTime1 = x.f_play_time
      var hour1 = x.f_hour
      var timerange1 = x.f_timerange
      if (time + playTime1 <= 1800) {
        listBuffer += ((x.f_user_id, x.f_device_id, x.f_terminal, x.f_play_type, x.f_start_time, hour1, timerange1, playTime1, x.f_play_time, x.f_program_id, x.f_region_id, x.videoPlay, x.f_column_id))
      } else {
        val loop = new Breaks;
        loop.breakable {
          playTime = 1800 - initialTime
          listBuffer += ((x.f_user_id, x.f_device_id, x.f_terminal, x.f_play_type, x.f_start_time, hour1, timerange1, playTime, x.f_play_time, x.f_program_id, x.f_region_id, x.videoPlay, x.f_column_id))
          var i = 0
          while (true) {
            //      for (j <- 0 to Comments.LOOP_Num) {
            time = 1800
            playTime1 = x.f_play_time - (1800 - initialTime) - 1800 * i
            i = i + 1
            if (playTime1 <= 0) {
              loop.break;
            }
            if (time >= playTime1) {
              playTime = playTime1
            } else {
              playTime = time
            }
            var timeRange = getHalfHourPierod(hour1, timerange1)
            hour1 = timeRange._1
            timerange1 = timeRange._2
            if (hour1 > 23) {
              loop.break;
            }
            listBuffer += ((x.f_user_id, x.f_device_id, x.f_terminal, x.f_play_type, x.f_start_time, hour1, timerange1, playTime, x.f_play_time, x.f_program_id, x.f_region_id, 0, x.f_column_id))
          }
        }
      }
      listBuffer.toList
    })
    val user_timerange2 = user_timerange.flatMap(x => x).map(x => {
      UserRdd(x._1, x._11, x._6, x._7, x._2, x._3, x._4, x._10, x._5, x._8, x._9, x._12, x._13)
    }).toDF()
    //关联区域
    val user_df_basic1 = getFinalUser(user_timerange2, hiveContext, regionInfo)
    user_df_basic1
  }

  def getUserDevice(time: String, user_df_basic1: DataFrame, hiveContext: HiveContext) = {
    user_df_basic1.registerTempTable("user_df_basic1")
    val user_df_basic = hiveContext.sql(
      s"""
         |select '$time' as f_date,
         |if(f_play_type='${GatherType.DEMAND_NEW}',f_user_id,null) as f_demand_userid,
         |if(f_play_type='${GatherType.LOOK_BACK_NEW}',f_user_id,null) as f_lookback_userid,
         |if(f_play_type='${GatherType.LIVE_NEW}',f_user_id,null) as f_live_userid,
         |if((f_play_type='${GatherType.TIME_SHIFT_NEW}' or f_play_type='${GatherType.one_key_timeshift}'),f_user_id,null) as f_time_shift_userid,
         |if(f_play_type='${GatherType.DEMAND_NEW}',f_device_id,null) as f_demand_deviceid,
         |if(f_play_type='${GatherType.LOOK_BACK_NEW}',f_device_id ,null) as f_lookback_deviceid,
         |if(f_play_type='${GatherType.LIVE_NEW}',f_device_id ,null) as f_live_deviceid,
         |if((f_play_type='${GatherType.TIME_SHIFT_NEW}' or f_play_type='${GatherType.one_key_timeshift}'),f_device_id ,null) as f_time_shift_deviceid,
         |f_hour,
         |f_timerange,
         |f_region_id,f_terminal,
         |f_region_name,f_province_id,
         |f_province_name,
         |f_city_id,f_city_name
         |from user_df_basic1
         |where f_city_name is not null
              """.stripMargin)
    user_df_basic
  }

  def getCountUserTimeRange(hiveContext: HiveContext, user_df_basic1: DataFrame) = {
    user_df_basic1.registerTempTable("user_online_rate")
    val user_df2 = hiveContext.sql(
      s"""
         |select f_date,
         |if((f_hour>9),cast(f_hour as STRING),concat('0',cast(f_hour as STRING))) as f_hour,
         |f_timerange,
         |f_terminal,
         |f_province_id,f_province_name,
         |f_city_id,f_city_name,
         |f_region_id,f_region_name,
         |concat_ws(',',collect_set(f_demand_userid)) as f_demand_user_ids,
         |concat_ws(',',collect_set(f_lookback_userid)) as f_lookback_user_ids,
         |concat_ws(',',collect_set(f_live_userid)) as f_live_user_ids,
         |concat_ws(',',collect_set(f_time_shift_userid)) as f_time_shift_user_ids,
         |concat_ws(',',collect_set(f_demand_deviceid)) as f_demand_device_ids,
         |concat_ws(',',collect_set(f_lookback_deviceid)) as f_lookback_device_ids,
         |concat_ws(',',collect_set(f_live_deviceid)) as f_live_device_ids,
         |concat_ws(',',collect_set(f_time_shift_deviceid)) as f_time_shift_device_ids
         |from user_online_rate
         |where f_city_name is not null
         |group by f_date,f_hour,f_timerange,
         |f_region_id,f_terminal,f_province_name,
         |f_province_id,f_city_name,f_city_id,
         |f_region_name
             """.stripMargin)
    DBUtils.saveToHomedData_2(user_df2, cn.ipanel.common.Tables.t_online_rate)
    DBUtils.saveDataFrameToPhoenixNew(user_df2, cn.ipanel.common.Tables.t_online_rate)
  }

  def getDeviceUserCount(hiveContext: HiveContext, user_df_basic1: DataFrame) = {
    import hiveContext.implicits._
    user_df_basic1.registerTempTable("user_online_rate_day")
    val user_df3 = hiveContext.sql(
      s"""
         |select f_date,
         |f_terminal,f_province_id,f_province_name,f_city_id,f_city_name,
         |f_region_id,f_region_name,
         |concat_ws(',',collect_set(f_demand_userid)) as f_demand_user_ids,
         |concat_ws(',',collect_set(f_lookback_userid)) as f_lookback_user_ids,
         |concat_ws(',',collect_set(f_live_userid)) as f_live_user_ids,
         |concat_ws(',',collect_set(f_time_shift_userid)) as f_time_shift_user_ids,
         |concat_ws(',',collect_set(f_demand_deviceid)) as f_demand_device_ids,
         |concat_ws(',',collect_set(f_lookback_deviceid)) as f_lookback_device_ids,
         |concat_ws(',',collect_set(f_live_deviceid)) as f_live_device_ids,
         |concat_ws(',',collect_set(f_time_shift_deviceid)) as f_time_shift_device_ids
         |from user_online_rate_day
         |where f_city_name is not null
         |group by f_date,
         |f_region_id,f_terminal,f_province_name,
         |f_province_id,f_city_name,f_city_id,
         |f_region_name
             """.stripMargin)
    val user_df4 = user_df3.map(x => {
      val f_date = x.getAs[String]("f_date")
      val f_terminal = x.getAs[Int]("f_terminal").toString
      val f_province_id = x.getAs[Long]("f_province_id")
      val f_province_name = x.getAs[String]("f_province_name")
      val f_city_id = x.getAs[Long]("f_city_id")
      val f_city_name = x.getAs[String]("f_city_name")
      val f_region_id = x.getAs[Int]("f_region_id")
      val f_region_name = x.getAs[String]("f_region_name")
      val f_demand_user_id_count = getCount(x.getAs[String]("f_demand_user_ids"))
      val f_lookback_user_id_count = getCount(x.getAs[String]("f_lookback_user_ids"))
      val f_live_user_id_count = getCount(x.getAs[String]("f_live_user_ids"))
      val f_time_shift_user_id_count = getCount(x.getAs[String]("f_time_shift_user_ids"))
      val f_demand_device_id_count = getCount(x.getAs[String]("f_demand_device_ids"))
      val f_lookback_device_id_count = getCount(x.getAs[String]("f_lookback_device_ids"))
      val f_live_device_id_count = getCount(x.getAs[String]("f_live_device_ids"))
      val f_time_shift_device_id_count = getCount(x.getAs[String]("f_time_shift_device_ids"))
      UserDevice(f_date, f_terminal, f_province_id, f_province_name, f_city_id, f_city_name, f_region_id, f_region_name, f_demand_user_id_count, f_lookback_user_id_count, f_live_user_id_count, f_time_shift_user_id_count, f_demand_device_id_count, f_lookback_device_id_count, f_live_device_id_count, f_time_shift_device_id_count)
    }).toDF()
    DBUtils.saveToHomedData_2(user_df4, cn.ipanel.common.Tables.t_day_online_rate)
  }

  def getCount(string: String) = {
    if ("" == string || null == string) {
      0
    } else {
      string.split(",").length
    }
  }

  //得到区域相关信息 转换终端
  def getFinalUser(user_df: DataFrame, hiveContext: HiveContext, regionInfo: DataFrame) = {
    val user_df2 = regionInfo.join(user_df, Seq("f_region_id"), "right")
    user_df2
  }

  def getTimerange(f_user_id: String, f_device_id: Long, f_terminal: String, f_play_type: String, startTime: String, hour: Int, timerange: String, playS: Long) = {
    //用户id,终端，区域，小时，半小时，节目id,时间
    //用户id,小时，半小时，时间
    val listBuffer = new ListBuffer[(String, Long, String, String, String, Int, String, Long)]()
    val initialTime = if (getTimeSecond(startTime) > 1800) {
      (getTimeSecond(startTime) - 1800)
    } else getTimeSecond(startTime)
    var playTime = 0L
    var time = initialTime
    var playTime1 = playS
    var hour1 = hour
    var timerange1 = timerange //60
    if (time + playTime1 <= 1800) {
      listBuffer += ((f_user_id, f_device_id, f_terminal, f_play_type, startTime, hour1, timerange1, playTime1))
    } else {
      val loop = new Breaks;
      loop.breakable {
        playTime = 1800 - initialTime
        listBuffer += ((f_user_id, f_device_id, f_terminal, f_play_type, startTime, hour1, timerange1, playTime))
        var i = 0
        while (true) {
          //      for (j <- 0 to Comments.LOOP_Num) {
          time = 1800
          playTime1 = playS - (1800 - initialTime) - 1800 * i
          i = i + 1
          if (playTime1 < 0) {
            loop.break;
          }
          if (time >= playTime1) {
            playTime = playTime1
          } else {
            playTime = time
          }
          var timeRange = getHalfHourPierod(hour1, timerange1)
          hour1 = timeRange._1
          timerange1 = timeRange._2
          if (hour1 > 23) {
            loop.break;
          }
          listBuffer += ((f_user_id, f_device_id, f_terminal, f_play_type, startTime, hour1, timerange1, playTime))
        }
      }
      //      listBuffer.toList
    }
    listBuffer.toList
  }

  def getHalfHourPierod(hour: Int, timerange: String) = {
    var hour1 = hour
    var timerange1 = ""
    if (timerange == "30") {
      timerange1 = "60"
    } else {
      timerange1 = "30"
      hour1 += 1
      if (hour1 == 24) {
        hour1 == 23
      }
    }
    (hour1, timerange1)
  }

  def getTimeSecond(time: String): Int = {
    val times = time.split(":")
    //      val res = times(2)
    val res = times(1).toInt * 60 + times(2).toInt
    res
  }

  /**
    * 防止重跑导致重复数据
    **/
  def delMysql(day: String, table: String): Unit = {
    val del_sql = s"delete from $table where f_date='$day'"
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, del_sql)
  }

  def getDemandBehaviour(time: String, hiveContext: HiveContext) = {
    hiveContext.sql("use bigdata")
    val use_behavior_basic = hiveContext.sql(
      s"""
         |select userId as f_user_id,
         |regionId as f_region_id,
         |hour(reportTime) as f_hour,
         |(case when minute(reportTime)>30 then 60 else 30 end) as f_timerange,
         |cast(deviceType AS INT) as f_terminal,reportType as f_play_type,
         |reportTime as f_start_time,
         |if((reportType = 'ShareSuccess'),exts['ID'],"") as f_video_id,
         |if((reportType = 'ShareSuccess'),1,0) as f_share_count
         |from orc_user_behavior
         |where day='$time'
         |and reportType = 'ShareSuccess'
            """.stripMargin).registerTempTable("use_behavior_basic")
    val use_behavior = hiveContext.sql(
      s"""
         |select f_region_id,f_hour,f_timerange,f_terminal,
         |f_video_id,sum(f_share_count) as f_share
         |from use_behavior_basic
         |group by f_region_id,f_hour,f_timerange,f_video_id,f_terminal
      """.stripMargin)
    use_behavior
  }


}
