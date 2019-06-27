package cn.ipanel.homed.repots

import cn.ipanel.common._
import cn.ipanel.utils.{DBUtils, DateUtils, RegionUtils}
import com.mysql.jdbc.StringUtils
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 媒资直播（频道、节目）统计
  * 入口参数[日期]
  *
  * @author ZouBo
  * @date 2018/1/9 0009 
  */
case class ChannelLive(f_date: String, f_hour: Integer, f_timerange: Integer, f_user_id: String, f_device_id: String, f_region_id: String
                       , f_terminal: Integer, f_channel_id: String, f_channel_start_time: String, f_channel_end_time: String, f_playtimes: Long, f_play_count: Integer)

case class ChannelType(chanel_id: String, f_channel_name: String, f_channel_type: String)

@Deprecated
object MeiZiLiveChannelStatistics {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("参数错误,程序退出")
      System.exit(1)
    } else {
      val day = args(0)
      val nextDay = DateUtils.getAfterDay(day)
      val sparkSession = SparkSession("MeiZiLiveChannelStatistics")
      val hiveContext = sparkSession.sqlContext

      sparkSession.sparkContext.getConf.registerKryoClasses(Array(classOf[ChannelLive], classOf[ChannelType]))
      //项目部署地code(省或者地市)
      val regionCode = RegionUtils.getRootRegion

      val regionBC = sparkSession.sparkContext.broadcast(regionCode)

      //先清表
      delMysql(day)

      val channel =
        """
          |(select distinct cast(channel_id as char) as chanel_id,chinese_name as f_channel_name,f_subtype
          |from channel_store
          |union
          |SELECT distinct cast(f_monitor_id as char ) chanel_id ,f_monitor_name f_channel_name ,f_sub_type as f_subtype from t_monitor_store
          |) as channel_store
        """.stripMargin
      val channelDF = DBUtils.loadMysql(hiveContext, channel, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
      // 将频道分类 1-标清，2-高清，3-央视，4-地方（本地），5-其他
      val subTypeInfo =
        """
          |(select (case  when  f_name like '%标清%' then '1'
          |when f_name like '%高清%' then '2'
          |when f_name like '%央视%' then '3'
          |when (f_name like '%本地%' or f_name like '%地方%') then '4'
          |else '5' end) as f_channel_type,
          |f_original_id from t_media_type_info where f_status=5
          |) as channelType
        """.stripMargin
      val channelTypeDF = DBUtils.loadMysql(hiveContext, subTypeInfo, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
      //区域信息表
      val region =
        s"""
           |(select cast(a.area_id as char) as f_area_id,a.area_name as f_region_name,
           |a.city_id as f_city_id,c.city_name as f_city_name,
           |c.province_id as f_province_id,p.province_name as f_province_name
           |from (area a left join city c on a.city_id=c.city_id)
           |left join province p on c.province_id=p.province_id
           |where a.city_id=${regionBC.value} or c.province_id=${regionBC.value}
           |) as region
        """.stripMargin
      //频道节目信息对照表
      //      val channelProgramRelation =
      //        s"""
      //           |(select h.homed_service_id as f_homed_channel_id,h.event_id as f_program_id,h.event_name as f_program_name
      //           |,date_format(h.start_time,'%Y-%m-%d %H:%i:%s') as f_program_start_time,
      //           |date_format(DATE_ADD(h.start_time,INTERVAL TIME_TO_SEC(h.duration) SECOND),'%Y-%m-%d %H:%i:%s') as f_program_end_time
      //           |from homed_eit_schedule_history h
      //           |where h.start_time BETWEEN '$day' AND '$nextDay'
      //           |) as channel_program_relation
      //              """.stripMargin

      //      val channelProgramRelationDF = DBUtils.loadMysql(hiveContext, channelProgramRelation, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
      val regionDF = DBUtils.loadMysql(hiveContext, region, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
      //按区域，终端统计在线用户数，有效注册用户总数
      channelStatistics(sparkSession, channelDF, channelTypeDF, regionDF, day, nextDay)

      sparkSession.stop()
    }
  }

  /**
    * 直播频道统计
    *
    * @param sparkSession
    * @param channelDF 频道信息
    * @param day       日期yyyymmdd
    * @return
    */
  def channelStatistics(sparkSession: SparkSession, channelDF: DataFrame, channelTypeDF: DataFrame, regionDF: DataFrame, day: String, nextDay: String) = {
    import sparkSession.sqlContext.implicits._
    val hiveContext = sparkSession.sqlContext
    //数据源以切换

    val sqlNew =
      s"""
         |select day as f_date, userId as f_user_id ,cast(deviceId as string) as f_device_id,
         |cast(deviceType as int) as f_terminal,
         |regionId as f_region_id, cast(serviceId as string) as f_channel_id,
         |startTime as f_channel_start_time ,endTime as f_channel_end_time,
         |playTime as f_playtimes
         |from orc_video_play where day=$day and playType='${GatherType.LIVE_NEW}'
      """.stripMargin

    //分享量
    val share =
      s"""
         |select regionId as f_region_id,hour(reportTime) as f_hour,
         |(case when minute(reportTime)>30 then 60 else 30 end) as f_timerange,
         |cast(deviceType AS INT) as f_terminal,
         |exts['ID'] as f_channel_id,
         |1 as f_share_count
         |from  orc_user_behavior where day=$day and reportType='ShareSuccess'
      """.stripMargin

    hiveContext.sql("use bigdata")
    //    val channelStatisticsDF = hiveContext.sql(sql)
    val liveRdd = hiveContext.sql(sqlNew).rdd
    val shareDF = hiveContext.sql(share)

    val liveDF = liveRdd.map(x => {
      val buffer = new ListBuffer[(ChannelLive)]
      val date = x.getAs[String]("f_date")
      val userId = x.getAs[String]("f_user_id")
      val deviceId = x.getAs[String]("f_device_id")
      val terminal = x.getAs[Integer]("f_terminal")
      val regionId = x.getAs[String]("f_region_id")
      val channelId = x.getAs[String]("f_channel_id")
      val startTime = x.getAs[String]("f_channel_start_time")
      val playTimes = x.getAs[Long]("f_playtimes")
      val listBuffer = process(startTime, playTimes)
      for (list <- listBuffer) {
        val hour = list._1
        val timeRange = list._2
        val startT = list._3
        val endT = list._4
        val plays = list._5
        val playCount = list._6
        val dateTime = DateUtils.transformDateStr(startT.substring(0, 10), DateUtils.YYYY_MM_DD, DateUtils.YYYYMMDD)
        buffer += (ChannelLive(dateTime, hour, timeRange, userId, deviceId, regionId, terminal, channelId, startT, endT, plays, playCount))
      }
      buffer.toList
    }).flatMap(x => x).toDF()
    val channelSubTypeDF = processChannelType(sparkSession, channelDF, channelTypeDF)
    val df = liveDF.join(channelSubTypeDF, liveDF("f_channel_id") === channelSubTypeDF("chanel_id"), "left_outer").drop("chanel_id")
    val channelRegionDF = df.join(regionDF, df("f_region_id") === regionDF("f_area_id"), "left_outer").drop("f_area_id")
    processDF(sparkSession, channelRegionDF, shareDF)
  }

  /**
    * 判断频道类型
    *
    * @param sparkSession
    * @param channelDF
    * @param channelTypeDF
    * @return
    */
  def processChannelType(sparkSession: SparkSession, channelDF: DataFrame, channelTypeDF: DataFrame): DataFrame = {
    import sparkSession.sqlContext.implicits._
    val temMap = new mutable.HashMap[String, String]()

    channelTypeDF.registerTempTable("t_channel_type_info")
    val sql =
      """
        |select f_channel_type,concat_ws(',',collect_set(f_original_id)) as f_original_ids
        |from t_channel_type_info
        |group by f_channel_type
      """.stripMargin
    val typeDF = sparkSession.sqlContext.sql(sql)

    typeDF.collect().foreach(row => {
      val channelType = row.getAs[String]("f_channel_type")
      val channelOriginals = row.getAs[String]("f_original_ids")
      temMap += (channelOriginals -> channelType)
    })
    val channelTypeBC = sparkSession.sparkContext.broadcast(temMap)
    channelDF.map(row => {
      val channelId = row.getAs[String]("chanel_id")
      val channelName = row.getAs[String]("f_channel_name")
      val subType = if (StringUtils.isNullOrEmpty(row.getAs[String]("f_subtype"))) "" else row.getAs[String]("f_subtype")
      val subTypeArr = subType.split("\\|")
      val buffer = new StringBuffer()
      val map = channelTypeBC.value
      for (subType <- subTypeArr) {
        val keys = map.keys
        for (key <- keys) {
          if (key.contains(subType) && !buffer.toString.contains(map.get(key).get)) {
            buffer.append(map.get(key).get).append(",")
          }
        }
      }
      var channelType = buffer.toString
      //如果频道找不到对应的类型，默认设置为5
      channelType = channelType match {
        case _ if (channelType.endsWith(",")) => channelType.substring(0, channelType.lastIndexOf(","))
        case _ if (channelType == null || channelType.equals("")) => "5"
        case _ => channelType
      }
      ChannelType(channelId, channelName, channelType)
    }).toDF()
  }

  /**
    * 按30分划分时间片段，播放时长
    * 返回[hour,timerange,startTime,endTime,playTimes.playCount]
    *
    * @param startTime
    * @param playTimes
    * @return
    */
  def process(startTime: String, playTimes: Long): ListBuffer[(Integer, Integer, String, String, Long, Integer)] = {
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


  /**
    * 业务处理逻辑
    * 将基础数据按指定条件聚合
    *
    * @param sparkSession
    * @param dataFrame
    */
  def processDF(sparkSession: SparkSession, dataFrame: DataFrame, shareDF: DataFrame) = {


    val hiveContext = sparkSession.sqlContext
    dataFrame.registerTempTable("t_chinel_program_live")
    shareDF.registerTempTable("t_share")
    //频道分享点击次数
    val shareSql =
      """
        |select f_region_id as f_region_id_share ,f_hour as f_hour_share,f_timerange as f_timerange_share,
        |f_terminal as f_terminal_share,f_channel_id as f_channel_id_share,
        |sum(f_share_count) as f_share_count
        |from t_share
        |group by f_region_id,f_hour,f_timerange,f_terminal,f_channel_id
      """.stripMargin
    val shareCount = hiveContext.sql(shareSql)


    //按同一用户、同一终端、同一区域、同一时间段，统一频道，聚合播放时长，点击次数
    //加上设备id
    val channel =
    """
      |select f_date,f_hour,f_timerange,concat_ws(',', collect_set(f_user_id)) as f_user_ids,
      |concat_ws(',', collect_set(f_device_id)) as f_device_ids,f_province_id,f_province_name
      |,f_city_id,f_city_name,f_region_id,f_region_name,f_terminal,
      |concat_ws(',',collect_set(f_channel_type)) as f_channel_type,
      |f_channel_id,f_channel_name,
      |sum(f_playtimes) as f_playtimes,
      |sum(f_play_count) as f_play_count
      |from t_chinel_program_live
      |group by f_date,f_hour,f_timerange,f_terminal,f_channel_id,f_channel_name,
      |f_region_id,f_region_name,f_province_id,f_province_name,f_city_id,f_city_name
    """.stripMargin
    val channelDF = hiveContext.sql(channel)

    val channelResultDF = channelDF.join(shareCount, channelDF("f_region_id") === shareCount("f_region_id_share")
      && channelDF("f_hour") === shareCount("f_hour_share") && channelDF("f_timerange") === shareCount("f_timerange_share")
      && channelDF("f_terminal") === shareCount("f_terminal_share") && channelDF("f_channel_id") === shareCount("f_channel_id_share"),
      "left_outer")
      .selectExpr("f_date", "f_hour", "f_timerange", "f_user_ids", "f_device_ids", "f_province_id",
        "f_province_name", "f_city_id", "f_city_name", "f_region_id", "f_region_name", "f_terminal",
        "f_channel_type", "f_channel_id", "f_channel_name", "f_playtimes", "f_play_count", "f_share_count")

    //    channelDF.registerTempTable("t_timeRange")

    val timeRangeSql =
      """
        |select f_date,f_hour,f_timerange,concat_ws(',', collect_set(f_user_id)) as f_user_ids ,
        |concat_ws(',', collect_set(f_device_id)) as f_device_ids,
        |f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name,f_terminal,
        |sum(f_playtimes) as f_playtimes,
        |sum(f_play_count) as f_play_count
        |from t_chinel_program_live
        |group by f_date,f_hour,f_timerange,f_terminal,
        |f_region_id,f_region_name,f_province_id,f_province_name,f_city_id,f_city_name
      """.stripMargin

    val timeRangeDF = hiveContext.sql(timeRangeSql)


    //计算出节目中30分钟点，60分钟点所播放的节目
    //        val programDF = processProgramDF(sparkSession, channelProgramRelationDF)

    //存mysql
    saveToMysql(timeRangeDF, Tables.t_meizi_statistics_terminal_timerange)
    saveToMysql(channelResultDF, Tables.t_meizi_statistics_terminal_channel)

    //   存HBASE
    //    DBUtils.saveDataFrameToPhoenixNew(timeRangeDF, Tables.t_meizi_statistics_terminal_timerange)
    //    DBUtils.saveDataFrameToPhoenixNew(channelResultDF, Tables.t_meizi_statistics_terminal_channel)
  }

  /**
    * 清表
    *
    * @param date
    */
  def delMysql(date: String): Unit = {

    delMysql(date, Tables.t_meizi_statistics_terminal_timerange)
    delMysql(date, Tables.t_meizi_statistics_terminal_channel)
  }

  /**
    * 防止重跑导致重复数据
    **/
  def delMysql(day: String, table: String): Unit = {
    val del_sql = s"delete from $table where f_date='$day'"
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, del_sql)
  }

  /**
    * 结果保存至mysql
    *
    * @param dataFrame
    * @param table
    */
  def saveToMysql(dataFrame: DataFrame, table: String) = {
    DBUtils.saveToHomedData_2(dataFrame, table)
  }


  /**
    * 从频道节目播放历史输数据中获取30分，60分时刻频道所播放节目
    *
    * @param sparkSession
    * @param dataFrame
    * @return
    */
  def processProgramDF(sparkSession: SparkSession, dataFrame: DataFrame) = {
    import sparkSession.sqlContext.implicits._
    dataFrame.map(row => {
      val listBuffer = new ListBuffer[(Long, Int, String, Int, Int)]
      val channelId = row.getAs[Long]("f_homed_channel_id")
      val programId = row.getAs[Int]("f_program_id")
      val programName = row.getAs[String]("f_program_name")

      val startTime = row.getAs[String]("f_program_start_time")
      val endTime = row.getAs[String]("f_program_end_time")
      val startHour = DateUtils.getHourFromDateTime(startTime)
      val startMinute = DateUtils.getMinuteFromDateTime(startTime)
      val endHour = DateUtils.getHourFromDateTime(endTime)
      val endMinute = DateUtils.getMinuteFromDateTime(endTime)
      //相差小时数
      val hnum = endHour.toInt - startHour.toInt

      if (hnum > 0) {
        //跨小时
        for (num <- 0 to (hnum)) {
          if (num == hnum) {
            if (endMinute.toInt >= 30) {
              listBuffer += ((channelId, programId, programName, startHour.toInt + num, 30))
            }
            //            listBuffer+=((channelId,programId,programName,startHour.toInt+num-1,60))
          } else {
            if (num == 0 && startMinute.toInt > 30) {
              listBuffer += ((channelId, programId, programName, startHour.toInt, 60))
            } else {
              listBuffer += ((channelId, programId, programName, startHour.toInt + num, 30))
              listBuffer += ((channelId, programId, programName, startHour.toInt + num, 60))
            }
          }
        }
      } else if (hnum < 0) {
        //跨天
        listBuffer += ((channelId, programId, programName, startHour.toInt, 60))
      } else {
        if (startMinute.toInt < 30 && endMinute.toInt > 30) {
          listBuffer += ((channelId, programId, programName, startHour.toInt, 30))
        }
      }
      listBuffer.toList
    }).flatMap(x => x).map(x => {
      ProgranPlayInfo(x._1, x._2, x._3, x._4, x._5)
    }).toDF
  }


}
