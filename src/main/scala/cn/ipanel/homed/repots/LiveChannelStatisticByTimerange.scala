package cn.ipanel.homed.repots

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.ipanel.common.{DBProperties, GatherType, SparkSession, Tables}
import cn.ipanel.homed.repots.LiveChannelStatistics.{getRegionDf}
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ListBuffer

case class ChannelLiveByFiveMinute(hour: Int, timeRange: Int, playtime: Long, playcount: Int, userid: String, terminal: Int, f_region_id: String, f_channel_id: String, starttime: String, endtime: String)

object LiveChannelStatisticByTimerange {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession("LiveChannelStatisticByTimerange")
    val sc: SparkContext = sparkSession.sparkContext
    sc.getConf.registerKryoClasses(Array(classOf[UserArray], classOf[ChannelCnt], classOf[ChannelLiveByFiveMinute], classOf[ChannelSubType]))
    val hiveContext: HiveContext = sparkSession.sqlContext
    var date = DateUtils.getYesterday()
    var partAmt = 200
    if (args.length != 2) {
      System.err.println("请输入正确参数：统计日期,分区数")
      System.exit(-1)
    } else {
      date = args(0)
      partAmt = args(1).toInt
    }
    val regionDF = getRegionDf(sparkSession, hiveContext)
    val channelDF = LiveChannelStatistics.getChannelTypeDf(sparkSession: SparkSession, hiveContext: HiveContext)
    //每一天的基础数据
    println("每一天基础数据")
    getChannelFiveMinuteDetailes(date, hiveContext, regionDF, channelDF)
    //按五分钟的
    //    println("5min基础数据")
    //    getChannelFiveMinute(date, hiveContext)
    //按天的
    //    println("天基础数据")
    //    getChannelByDay(date, hiveContext)
    //按十五分钟
    //    println("十五分钟基础数据")
    //    getChannelByFifteen(date, hiveContext)
  }

  def getChannelByFifteen(date: String, hiveContext: HiveContext) = {
    hiveContext.sql("use bigdata")
    hiveContext.udf.register("gettimerange", gettimerange _)
    val finaldf = hiveContext.sql(
      s"""
         |select  '$date' as f_date,
         |b.f_hour,b.f_timerange,
         |sum(b.f_play_time) as f_play_time,
         |sum(b.f_play_count) as f_play_count,
         |count(distinct(b.userid)) as f_user_count,
         |b.f_terminal,b.f_region_id,b.f_region_name,b.f_city_id,
         |b.f_city_name,b.f_province_id,b.f_province_name,b.f_channel_name,b.f_channel_id,
         |concat_ws(',',collect_set(b.f_event_id)) as f_event_id,
         |concat_ws(',',collect_set(b.f_event_name)) as f_event_name
         |from
         |(select
         |a.f_hour,gettimerange(a.f_timerange) as f_timerange,
         |a.f_play_time,
         |a.f_play_count,
         |a.f_terminal,a.f_region_id,a.f_region_name,a.f_city_id,a.userid,
         |a.f_city_name,a.f_province_id,a.f_province_name,a.f_channel_name,a.f_channel_id,
         |a.f_event_id,a.f_event_name
         |from t_channel_basic a)  b
         |group by
         |b.f_hour,b.f_timerange,
         |b.f_terminal,b.f_region_id,b.f_region_name,b.f_city_id,
         |b.f_city_name,b.f_province_id,b.f_province_name,b.f_channel_name,b.f_channel_id
      """.stripMargin)
    DBUtils.excuteSqlPhoenix(s"delete from ${Tables.T_CHANNEL_LIVE_PROGRAM_BY_FIFTEEN} where f_date = '$date' ")
    DBUtils.saveDataFrameToPhoenixNew(finaldf, Tables.T_CHANNEL_LIVE_PROGRAM_BY_FIFTEEN)
  }

  def gettimerange(timerange: Int) = {
    var timerangeNew = 0
    if (timerange == 5 || timerange == 10 || timerange == 15) {
      timerangeNew = 15
    }
    else if (timerange == 20 || timerange == 25 || timerange == 30) {
      timerangeNew = 30
    }
    else if (timerange == 35 || timerange == 40 || timerange == 45) {
      timerangeNew = 45
    }
    else {
      timerangeNew = 60
    }
    timerangeNew
  }

  def getChannelByDay(date: String, hiveContext: HiveContext) = {
    hiveContext.sql("use bigdata")
    val finaldf = hiveContext.sql(
      s"""
         |select '$date' as f_date,
         |sum(a.f_play_time) as f_play_time,
         |sum(a.f_play_count) as f_play_count,
         |count(distinct(userid)) as f_user_count,
         |a.f_terminal,a.f_region_id,a.f_region_name,a.f_city_id,
         |a.f_city_name,a.f_province_id,a.f_province_name,a.f_channel_name,a.f_channel_id,
         |a.f_event_id,a.f_event_name
         |from t_channel_basic a
         |group by
         |a.f_terminal,a.f_region_id,a.f_region_name,a.f_city_id,
         |a.f_city_name,a.f_province_id,a.f_province_name,a.f_channel_name,a.f_channel_id,
         |a.f_event_id,a.f_event_name
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(finaldf, Tables.T_CHANNEL_LIVE_PROGRAM_BY_DAY)
  }

  def getChannelFiveMinute(date: String, hiveContext: HiveContext) = {
    hiveContext.sql("use bigdata")
    val finaldf = hiveContext.sql(
      s"""
         |select '$date' as f_date,a.f_hour,a. f_timerange,
         |sum(a.f_play_time) as f_play_time,
         |sum(a.f_play_count) as f_play_count,
         |count(distinct(userid)) as f_user_count,
         |a.f_terminal,a.f_region_id,a.f_region_name,a.f_city_id,
         |a.f_city_name,a.f_province_id,a.f_province_name,a.f_channel_name,a.f_channel_id,
         |a.f_event_id,a.f_event_name
         |from t_channel_basic a
         |group by
         |a.f_hour,a.f_timerange,a.f_terminal,a.f_region_id,a.f_region_name,a.f_city_id,
         |a.f_city_name,a.f_province_id,a.f_province_name,a.f_channel_name,a.f_channel_id,
         |a.f_event_id,a.f_event_name
      """.stripMargin)
    DBUtils.excuteSqlPhoenix(s"delete from ${Tables.T_CHANNEL_LIVE_PROGRAM_FIVEMINUTE} ")
    DBUtils.saveDataFrameToPhoenixNew(finaldf, Tables.T_CHANNEL_LIVE_PROGRAM_FIVEMINUTE)
  }


  def getChannelName(hiveContext: HiveContext) = {
    val sql =
      """
        |(SELECT DISTINCT CAST(channel_id AS CHAR) AS channel_id,chinese_name AS f_channel_name
        |FROM channel_store
        |UNION
        |SELECT DISTINCT CAST(f_monitor_id AS CHAR) channel_id ,f_monitor_name AS f_channel_name
        |FROM t_monitor_store) as channel_store
      """.stripMargin
    val channelDF = DBUtils.loadMysql(hiveContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    channelDF
  }

  def getChannelFiveMinuteDetailes(date: String, hiveContext: HiveContext, regionDF: DataFrame, channelDF: DataFrame) = {
    import hiveContext.implicits._
    hiveContext.sql("use bigdata")
    val chnnCntDf = hiveContext.sql(
      s"""
         |select day as f_date, userId as f_user_id ,
         |cast(deviceType as int) as f_terminal,
         |regionId as f_region_id, nvl(cast(serviceId as string),'${GatherType.UNKNOWN_CHANNEL}') as f_channel_id,
         |startTime,
         |playTime
         |from orc_video_play
         |where day=$date and playType='${GatherType.LIVE_NEW}' --and playtime > 0 and serviceId is not null
      """.stripMargin)
      .map(x => {
        val buffer = new ListBuffer[(ChannelLiveByFiveMinute)]
        val userid = x.getAs[String]("f_user_id")
        val terminal = x.getAs[Int]("f_terminal")
        val f_region_id = x.getAs[String]("f_region_id")
        val f_channel_id = x.getAs[String]("f_channel_id")
        val starttime = x.getAs[String]("startTime")
        val playTime = x.getAs[Long]("playTime")
        val listBuffer = processTime(starttime, playTime, 5, userid, terminal, f_region_id, f_channel_id)
        for (list <- listBuffer) {
          val hour = if (list._2 == 0) list._1 - 1 else list._1
          val timeRange = if (list._2 == 0) 60 else list._2
          val playtime = list._3
          val playcount = list._4
          val userid = list._5
          val terminal = list._6
          val f_region_id = list._7
          val f_channel_id = list._8
          val starttime = list._9
          val endtime = list._10
          buffer += ChannelLiveByFiveMinute(hour, timeRange, playtime, playcount, userid, terminal, f_region_id, f_channel_id, starttime, endtime)
        }
        buffer.toList
      }).flatMap(x => x).toDF()
    channelDetailsByfiveminute(chnnCntDf, hiveContext, date, channelDF, regionDF)
  }

  def channelDetailsByfiveminute(chnnCntDf: DataFrame, hiveContext: HiveContext, date: String, channelDF: DataFrame, regionDF: DataFrame) = {
    val enddate = DateUtils.getNDaysAfter(1, date)
    val liveProgram = getLiveProgram(date, enddate, hiveContext).registerTempTable("liveprogram")
    val df = channelDF.join(chnnCntDf, chnnCntDf("f_channel_id") === channelDF("chanel_id")).drop("chanel_id")
    val channelResultDF = df.join(regionDF, df("f_region_id") === regionDF("f_area_id")).drop("f_area_id")
      .registerTempTable("channelfiveminute")
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    val finaldf = hiveContext.sql(
      s"""
         |insert overwrite table t_channel_basic
         |select '$date' as f_date,a.userid,
         |if(a.hour = -1,24,a.hour ) as f_hour,
         |a.timerange as f_timerange,a.f_play_time,a.f_play_count,
         |a.f_terminal,a.f_region_id,a.f_region_name,a.f_city_id,
         |a.f_city_name,a.f_province_id,a.f_province_name,a.f_channel_name,a.f_channel_id,
         |regexp_replace(a.f_channel_type, ',','|')
         |as f_channel_type,
         |b.f_event_id,b.f_event_name,a.endtime
         |from
         |(select hour,timerange,userid,
         |cast(sum(playtime) as double) as f_play_time,
         |sum(playcount) as f_play_count,
         |terminal as f_terminal,f_region_id,f_region_name,f_city_id,
         |f_city_name,f_province_id,f_province_name,f_channel_name,f_channel_id,
         |concat_ws('|',collect_set(f_channel_type)) as f_channel_type,
         |endtime
         |from  channelfiveminute
         |group by hour,timerange,terminal,f_region_id,f_channel_id,userid,
         |f_region_name,f_city_id,f_city_name,f_province_id,f_province_name,
         |f_channel_name,endtime) a
         |left join liveprogram b
         |on a.f_channel_id =b.f_channel_id
         |and a.endtime >=b.start_time and a.endtime<b.end_time
      """.stripMargin)
  }


  def processTime(starttime: String, playTime: Long, timeInterval: Int, userid: String, terminal: Int, f_region_id: String, f_channel_id: String) = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val hour = new SimpleDateFormat("HH")
    val minute = new SimpleDateFormat("mm")
    val list = new ListBuffer[(Int, Int, Long, Int, String, Int, String, String, String, String)]
    var flag = true
    var playtime = 0L
    //开始时间
    val startDate = format.parse(starttime)
    val startCalendar = Calendar.getInstance
    startCalendar.setTime(startDate)
    //结束时间
    startCalendar.add(Calendar.SECOND, playTime.toInt)
    val endDate = startCalendar.getTime
    //起始时间间隔
    var tmpDate = format.parse(starttime)
    val minSum = startDate.getMinutes / timeInterval
    var tmpCalendar = Calendar.getInstance
    tmpDate.setMinutes(minSum * timeInterval)
    tmpDate.setSeconds(0)
    tmpCalendar.setTime(tmpDate)
    //下一个时间间隔
    tmpCalendar.add(Calendar.MINUTE, timeInterval)
    var nextTmpDate = tmpCalendar.getTime
    if (endDate.before(tmpCalendar.getTime)) {
      playtime = DateUtils.getDiffSeconds(startDate, endDate)
      list += ((hour.format(nextTmpDate).toInt, minute.format(nextTmpDate).toInt, playTime, 1, userid, terminal, f_region_id, f_channel_id, starttime, format.format(nextTmpDate)))
    } else {
      list += ((hour.format(nextTmpDate).toInt, minute.format(nextTmpDate).toInt, DateUtils.getDiffSeconds(startDate, tmpCalendar.getTime), 1, userid, terminal, f_region_id, f_channel_id, starttime, format.format(nextTmpDate)))
      while (flag) {
        tmpDate = tmpCalendar.getTime
        tmpCalendar.add(Calendar.MINUTE, timeInterval)
        nextTmpDate = tmpCalendar.getTime
        if (endDate.equals(tmpDate)) {
          flag = false
        }
        else if (endDate.before(tmpCalendar.getTime)) {

          playtime = DateUtils.getDiffSeconds(tmpDate, endDate)
          list += ((hour.format(nextTmpDate).toInt, minute.format(nextTmpDate).toInt, playtime, 0, userid, terminal, f_region_id, f_channel_id, starttime, format.format(nextTmpDate)))
          flag = false
        } else {
          list += ((hour.format(tmpDate).toInt, minute.format(nextTmpDate).toInt, timeInterval * 60, 0, userid, terminal, f_region_id, f_channel_id, starttime, format.format(nextTmpDate)))
        }
      }
    }
    list
  }


  /**
    * 直播频道统计-按5min分段统计
    *
    * @param sparkSession
    * @param date 日期yyyymmdd
    * @return
    */
  def channelLineCountByFiveMinute(sparkSession: SparkSession, regionDF: DataFrame, date: String, partAmt: Int) = {
    import sparkSession.sqlContext.implicits._
    val hiveContext = sparkSession.sqlContext
    //数据源以切换
    val sqlNew =
      s"""
         |select day as f_date, userId as f_user_id ,cast(deviceId as string) as f_device_id,
         |cast(deviceType as int) as f_terminal,
         |regionId as f_region_id, nvl(cast(serviceId as string),'${GatherType.UNKNOWN_CHANNEL}') as f_channel_id,
         |startTime as f_channel_start_time ,endTime as f_channel_end_time,
         |playTime as f_play_time
         |from orc_video_play_tmp where day=$date and playType='${GatherType.LIVE_NEW}' -- and playtime>0 and serviceId is not null
      """.stripMargin
    hiveContext.sql("use bigdata")
    val liveRdd = hiveContext.sql(sqlNew).rdd
    val liveDF = liveRdd.repartition(partAmt)
      .map(x => {
        val buffer = new ListBuffer[(ChannelLiveByHalfHour)]
        val date = x.getAs[String]("f_date")
        val userId = x.getAs[String]("f_user_id")
        val deviceId = x.getAs[String]("f_device_id")
        val terminal = x.getAs[Integer]("f_terminal")
        val regionId = x.getAs[String]("f_region_id")
        val channelId = x.getAs[String]("f_channel_id")
        val startTime = x.getAs[String]("f_channel_start_time")
        val playTimes = x.getAs[Long]("f_play_time")
        val listBuffer = processMinute(startTime, playTimes)
        for (list <- listBuffer) {
          val hour = list._1
          val timeRange = list._2
          val startT = list._3
          val endT = list._4
          val plays = list._5
          val playCount = list._6
          val dateTime = DateUtils.transformDateStr(startT.substring(0, 10), DateUtils.YYYY_MM_DD, DateUtils.YYYYMMDD)
          buffer += ChannelLiveByHalfHour(dateTime, hour, timeRange, userId, deviceId, regionId, terminal, channelId, startT, endT, plays, playCount)
        }
        buffer.toList
      }).flatMap(x => x).toDF()

  }

  /**
    * 按5分划分时间片段，播放时长
    * 返回[hour,timerange,startTime,endTime,playTimes.playCount]
    *
    * @param startTime
    * @param playTimes
    * @return
    */
  def processMinute(startTime: String, playTimes: Long): ListBuffer[(Integer, Integer, String, String, Long, Integer)] = {
    val list = new ListBuffer[(Integer, Integer, String, String, Long, Integer)]
    var start = DateUtils.transferYYYY_DD_HH_MMHHSSToDate(startTime)
    val end = start.plusSeconds(playTimes.toInt)
    var hour: Integer = 0
    val m = start.getMinuteOfHour
    var timeRange: Integer = 30
    var endTemp = end
    var flag = true
    var plays = 0
    var i = 0
    var playCount = 0
    while (flag) {
      i match {
        case 0 => {
          if (m < 30) {
            endTemp = start.plusMinutes(30 - m).minusSeconds(start.getSecondOfMinute)
          } else {
            endTemp = start.plusMinutes(60 - m).minusSeconds(start.getSecondOfMinute)
          }
          i = 1
          playCount = 1
        }
        case _ => {
          endTemp = start.plusMinutes(30)
          playCount = 0
        }
      }
      hour = start.getHourOfDay
      timeRange = if (start.getMinuteOfHour < 30) 30 else 60
      if (end.compareTo(endTemp) > 0) {
        val startStr = start.toString(DateUtils.YYYY_MM_DD_HHMMSS)
        val endStr = endTemp.toString(DateUtils.YYYY_MM_DD_HHMMSS)
        //跨天
        if (endTemp.getDayOfYear != start.getDayOfYear) {
          plays = 86400 - start.getSecondOfDay
        } else {
          plays = endTemp.getSecondOfDay - start.getSecondOfDay
        }
        list += ((hour, timeRange, startStr, endStr, plays.toLong, playCount))
      } else {
        flag = false
        val startStr = start.toString(DateUtils.YYYY_MM_DD_HHMMSS)
        val endStr = end.toString(DateUtils.YYYY_MM_DD_HHMMSS)
        //跨天
        if (endTemp.getDayOfYear != start.getDayOfYear) {
          plays = 86400 - start.getSecondOfDay
        } else {
          plays = end.getSecondOfDay - start.getSecondOfDay
        }
        list += ((hour, timeRange, startStr, endStr, plays.toLong, playCount))
      }
      start = endTemp
    }
    list
  }

  def getLiveProgram(date: String, enddate: String, hiveContext: HiveContext) = {
    val sql =
      s"""
         |(select distinct(a.homed_service_id) as f_channel_id,cast(event_id as char)as f_event_id,event_name as f_event_name,
         |a.start_time,
         |date_add(a.start_time,interval duration hour_second) as end_time
         |from
         |homed_eit_schedule_history a
         |where a.start_time>='$date' and a.start_time<'$enddate'
         |) as aa
      """.stripMargin
    val liveProgramDF = DBUtils.loadMysql(hiveContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    liveProgramDF
  }
}
