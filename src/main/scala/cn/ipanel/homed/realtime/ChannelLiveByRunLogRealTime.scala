package cn.ipanel.homed.realtime

import java.io

import cn.ipanel.common._
import cn.ipanel.utils.{DBUtils, DateUtils, RegionUtils}
import com.mysql.jdbc.StringUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 基于服务器run日志的直播实时统计程序
  * @author lizhy@20190228
  */
case class ChannelLiveDetailRunLog(f_date: String, f_hour: Int, f_timerange: Int,f_region_id: String,f_terminal: Int, f_channel_id: String,
                             f_user_id: String, f_play_time: Long,f_date_time:String)
case class LiveCountRunLog(f_date: String, f_hour: Int, f_timerange: Int, f_province_id: String, f_province_name: String, f_city_id: String, f_city_name: String,
                     f_region_id: String, f_region_name: String,f_terminal: String, f_channel_id: String, f_channel_name: String,f_user_count:Int,
                     f_play_time: Long,f_total_time:Long,f_audience_rating: Double,f_arrival_rating: Double,f_audience_share: Double)
object ChannelLiveByRunLogRealTime {
  val STATIC_DEVICE_WECHAT = "2999999999"
  val STATIC_DEVICE_STB = "1599999999"
  val STATIC_DEVICE_PHO = "2999999999"
  val STATIC_DEVICE_PC = "3900000000"
  val STATIC_SERVICE_TYPE = "live"
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("请输入正确参数")
      System.exit(1)
    }
    val groupId = args(0)
    val topic = args(1)
    val duration = args(2)
    val partition = args(3)

    //1.初始化spark上下文
    val session = SparkSession("ChannelLiveByUserLogRealTime")
    val sparkContext = session.sparkContext
    session.sparkContext.getConf.registerKryoClasses(Array(classOf[ChannelLiveDetail]))
    val topics = Set(topic)
    val brokers = CluserProperties.KAFKA_BROKERS
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "group.id" -> groupId
      , "serializer.class" -> "kafka.serializer.StringEncoder", "auto.offset.reset" -> "largest")
    val ssc = new StreamingContext(sparkContext, Durations.minutes(duration.toLong))
    val message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val sqlContext: HiveContext = session.sqlContext
    processLiveMessage(sparkContext:SparkContext,message, session, groupId,partition.toInt, duration.toInt,sqlContext: HiveContext)
    //开启ssc
    ssc.start()
    println("waiting for messages..")
    //等待计算完成
    ssc.awaitTermination()
  }

  /**
    * 加载区域信息
    *
    * @param sparkSession
    */
  def loadRegionInfo(sparkSession: SparkSession): mutable.HashMap[String, (String, String, String,String,String)] = {
    val sqlContext = sparkSession.sqlContext
    val map = new mutable.HashMap[String, (String, String, String,String, String)]()
    //项目部署地code(省或者地市)
    val regionCode = RegionUtils.getRootRegion
    val regionBC: Broadcast[String] = sparkSession.sparkContext.broadcast(regionCode)
    //区域信息表
    val region =
      s"""
         |(select cast(a.area_id as char) as f_area_id,a.area_name as f_region_name,
         |cast(a.city_id as char) as f_city_id,c.city_name as f_city_name,
         |cast(c.province_id as char) as f_province_id,p.province_name as f_province_name
         |from (area a left join city c on a.city_id=c.city_id)
         |left join province p on c.province_id=p.province_id
         |--where a.city_id=${regionBC.value} or c.province_id=${regionBC.value}
         |) as region
        """.stripMargin
    val regionDF = DBUtils.loadMysql(sqlContext, region, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    regionDF.collect.foreach(row => {
      val regionCode = row.getAs[String]("f_area_id")
      val regionName = row.getAs[String]("f_region_name")
      val cityCode = row.getAs[String]("f_city_id")
      val cityName = row.getAs[String]("f_city_name")
      val provCode = row.getAs[String]("f_province_id")
      val provName = row.getAs[String]("f_province_name")
      map += (regionCode -> (regionName, cityCode, cityName,provCode,provName))
    })
    map
  }

  /**
    * 处理结果保存至phoenix或者mysql
    *
    * @param message
    * @param sparkSession
    * @param partitionNum
    * @param duration
    */

  def processLiveMessage(sc:SparkContext,message: InputDStream[(String, String)], sparkSession: SparkSession, groupId: String, partitionNum: Int,duration: Int,sqlContext: HiveContext) = {
    message.foreachRDD(rdd => {
      println("message count:" + rdd.count())
      import sqlContext.implicits._
      //地域信息
      val regionInfo = loadRegionInfo(sparkSession)
      val regionBC = sc.broadcast[mutable.HashMap[String, (String, String, String,String, String)]](regionInfo)
      val defaultRegionBC = sc.broadcast[String](getDefaultRegionCode())
      //频道信息
      val channelInfo = loadChannelInfo(sqlContext)
      val channelBC = sc.broadcast[mutable.HashMap[String, String]](channelInfo)
      //频道分类信息
      val channelTypeDf = getChannelTypeDf(sparkSession,sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
      //各区域用户总数信息
      val registUserMap = loadUserAmt(sqlContext)
      val registUserMapBC = sc.broadcast[mutable.HashMap[(String,String),Int]](registUserMap)
      //终端类型信息
      val terminalDf = getTerminalDf(sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val filterRdd = rdd.filter(mess => {
        mess._2.contains("[" + GatherType.USER_LOG_HEARTBREATH +",") && mess._2.contains("(S,1)") && mess._2.length > 3
      })
      val time = DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS)
      val timeRangeSet: (String, Int, Int) = getTimeRange(time,duration.toInt)
      deleteHistChannelRate(sqlContext:HiveContext,timeRangeSet._1:String,timeRangeSet._2:Int,timeRangeSet._3:Int) //删除历史收视统计
      deleteHistPlaytime(sqlContext:HiveContext,timeRangeSet._1:String,timeRangeSet._2:Int,timeRangeSet._3:Int) //删除历史播放时长统计
      filterRdd.toDF().show(100,false) ////////test//////////
      val userLogDf = filterRdd
        .filter(_._2.split("\\|").length>1)
        .map(line => {
          val data = line._2.substring(3).replace("[", "").replace("]", "").replace("<", "").replace(">", "").replace("(", "").replace(")", "")
          val datas = data.split("\\|")
          val base = datas(0).split(",") //[service,event_time,userid,region,deviceid]
          val params = stringToMap(datas(1))
          val dateTime = base(1)
          val userId = base(2)
          val region = base(3)
          val deviceId = base(4)
          val channelId = params.getOrElse("I", "")
          val (date, hour, timeRange) = timeRangeSet
          (userId , (userId,deviceId,channelId,date,hour,timeRange,region,dateTime))
        })
        .reduceByKey((x,y)=>{
          if (x._8 >= y._8){
            (x)
          }else{
            (y)
          }
        })
        .map(x => {
          val value = x._2
          // (userId,deviceId,channelId,date,hour,timeRange,region,dateTime)
          (value._1,value._2,value._3,value._4,value._5.toInt,value._6.toInt,value._7,value._8)
        })
        .repartition(partitionNum).toDF()
        .withColumnRenamed("_1","user_id")
        .withColumnRenamed("_2","device_id")
        .withColumnRenamed("_3","channel_id")
        .withColumnRenamed("_4","date")
        .withColumnRenamed("_5","hour")
        .withColumnRenamed("_6","timerange")
        .withColumnRenamed("_7","region_id")
        .withColumnRenamed("_8","date_timestamp")
      val userDevDf = userLogDf.join(terminalDf,userLogDf("device_id")===terminalDf("d_device_id"),"left_outer")
        .selectExpr("user_id","device_id","channel_id","date","hour","timerange","region_id","nvl(d_device_type,'-1') as terminal","date_timestamp")
      val detailDF = userDevDf.map(x => {
        val userId = x.getAs[String]("user_id")
        val deviceId = x.getAs[String]("device_id")
        val channelId = x.getAs[String]("channel_id")
        val date = x.getAs[String]("date")
        val hour = x.getAs[Int]("hour").toInt
        val timeRange = x.getAs[Int]("timerange").toInt
        val playTime = Constant.SYSTEM_HEARTBEAT_DURATION * duration
        val regionId = x.getAs[String]("region_id")
        var terminal =x.getAs[String]("terminal").toInt
        if(terminal == -1){
          //处理非homed项目终端deviceid情况
          terminal = deviceId match{
            case STATIC_DEVICE_STB => 1
            case STATIC_DEVICE_PHO => 3
            case STATIC_DEVICE_WECHAT => 3
            case STATIC_DEVICE_PC => 5
            case _ => terminal
          }
        }
        val dateTime = DateUtils.unixTimeToDate(x.getAs[String]("date_timestamp").toLong,DateUtils.YYYY_MM_DD_HHMMSS)
        ChannelLiveDetail(date, hour, timeRange, regionId, terminal, channelId, userId, playTime,dateTime)
      }).toDF()
      //单频道
      val dfByChannel = detailDF.groupBy("f_date","f_hour","f_timerange","f_region_id","f_terminal","f_channel_id","f_user_id")
        .sum("f_play_time")
        .withColumnRenamed("sum(f_play_time)","f_play_time")
        .map(x =>{
          val date = x.getAs[String]("f_date")
          val hour = x.getAs[Int]("f_hour")
          val timeRange = x.getAs[Int]("f_timerange")
          val regionId = x.getAs[String]("f_region_id")
          val terminal = x.getAs[Int]("f_terminal")
          val channelId = x.getAs[String]("f_channel_id")
          val userId = x.getAs[String]("f_user_id")
          val playTime = x.getAs[Long]("f_play_time")
          val key = date + "," + hour + "," + timeRange + "," + regionId + "," + terminal + "," + channelId
          val value = (1,playTime)
          (key,value)
        }).reduceByKey((x,y) => {
        (x._1 + y._1,x._2 + y._2)
      }).map(x => {
        val keyArr = x._1.split(",")
        val date = keyArr(0)
        val hour = keyArr(1)
        val timeRange = keyArr(2)
        var regionId = keyArr(3)
        val regionMap = regionBC.value //(regionCode -> (regionName, cityCode, cityName,provCode,provName))
        val defaultRegionMap = regionMap.getOrElse(defaultRegionBC.value,("","","","",""))
        val (regionName,city_id,city_name,provinceId,provinceName) = regionMap.getOrElse(regionId,(defaultRegionMap._1,defaultRegionMap._2,defaultRegionMap._3,defaultRegionMap._4,defaultRegionMap._5))
        if (regionMap.get(regionId).size == 0){
          regionId = defaultRegionBC.value
        }
        val terminal = keyArr(4)
        val channelId = keyArr(5)
        val channelName = channelBC.value.getOrElse(channelId,"unknown")
        val registUserCnt = registUserMapBC.value.getOrElse((terminal,regionId),0)
        val userCnt = x._2._1
        val playTime = x._2._2
        (date,hour,timeRange,regionId,regionName,city_id,city_name,provinceId,provinceName,terminal,channelId,channelName,playTime,userCnt,registUserCnt)
      }).toDF()
        .withColumnRenamed("_1","f_date")
        .withColumnRenamed("_2","f_hour")
        .withColumnRenamed("_3","f_timerange")
        .withColumnRenamed("_4","f_region_id")
        .withColumnRenamed("_5","f_region_name")
        .withColumnRenamed("_6","f_city_id")
        .withColumnRenamed("_7","f_city_name")
        .withColumnRenamed("_8","f_province_id")
        .withColumnRenamed("_9","f_province_name")
        .withColumnRenamed("_10","f_terminal")
        .withColumnRenamed("_11","f_channel_id")
        .withColumnRenamed("_12","f_channel_name")
        .withColumnRenamed("_13","f_play_time")
        .withColumnRenamed("_14","f_online_users")
        .withColumnRenamed("_15","f_regist_users")
      //所有频道
      val dfByRegion = detailDF.groupBy("f_date","f_hour","f_timerange","f_region_id","f_terminal","f_user_id")
        .sum("f_play_time")
        .withColumnRenamed("sum(f_play_time)","f_play_time")
        .map(x =>{
          val date = x.getAs[String]("f_date")
          val hour = x.getAs[Int]("f_hour")
          val timeRange = x.getAs[Int]("f_timerange")
          val regionId = x.getAs[String]("f_region_id")
          val terminal = x.getAs[Int]("f_terminal")
          val userId = x.getAs[String]("f_user_id")
          val playTime = x.getAs[Long]("f_play_time")
          val key = date + "," + hour + "," + timeRange + "," + regionId + "," + terminal
          val value = (1,playTime)
          (key,value)
        }).reduceByKey((x,y) => {
        (x._1 + y._1,x._2 + y._2)
      }).map(x => {
        val keyArr = x._1.split(",")
        val date = keyArr(0)
        val hour = keyArr(1)
        val timeRange = keyArr(2)
        var regionId = keyArr(3)
        val regionMap = regionBC.value
        val defaultRegionMap = regionMap.getOrElse(defaultRegionBC.value,("","","","",""))
        val (regionName,city_id,city_name,provinceId,provinceName) = regionMap.getOrElse(regionId,(defaultRegionMap._1,defaultRegionMap._2,defaultRegionMap._3,defaultRegionMap._4,defaultRegionMap._5))
        if (regionMap.get(regionId).size == 0){
          regionId = defaultRegionBC.value
        }
        val terminal = keyArr(4)
        val userCnt = x._2._1
        val playTime = x._2._2
        (date,hour,timeRange,regionId,regionName,city_id,city_name,provinceId,provinceName,terminal,playTime,userCnt)
      }).toDF()
        .withColumnRenamed("_1","f_date")
        .withColumnRenamed("_2","f_hour")
        .withColumnRenamed("_3","f_timerange")
        .withColumnRenamed("_4","f_region_id")
        .withColumnRenamed("_5","f_region_name")
        .withColumnRenamed("_6","f_city_id")
        .withColumnRenamed("_7","f_city_name")
        .withColumnRenamed("_8","f_province_id")
        .withColumnRenamed("_9","f_province_name")
        .withColumnRenamed("_10","f_terminal")
        .withColumnRenamed("_11","f_play_time")
        .withColumnRenamed("_12","f_online_users")
      try{
        calculateOnlineUser(dfByRegion,sqlContext) //在线用户数统计
        println("开始直播用户状态列表："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
        saveLiveUsers(detailDF,sqlContext) //保存、更新用户状态列表
        println("结束直播用户状态列表："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
        //calculateUserCntByChannelType(dfByChannel,channelTypeDf,sqlContext) //按频道分类播放时长统计
        //calculateRates(dfByChannel,dfByRegion, duration, sqlContext, partitionNum) //计算收视率、收视份额、到达率等
        //deleteTimeNode(timeRangeSet._1,timeRangeSet._2,timeRangeSet._3) //删除节点
        //addTimeNode(timeRangeSet._1,timeRangeSet._2,timeRangeSet._3) //增加当前节点
      }catch {
        case e:Exception => {
          e.printStackTrace()
          sqlContext.clearCache()
          sqlContext.emptyDataFrame
        }
      }finally {
        regionBC.destroy()
        channelBC.destroy()
      }
    })
  }


  /**
    * 按设备类型、区域、频道聚合，并计算频道收视率
    * @param liveCntDf
    * @param dfByRegion
    * @param range
    * @param sqlContext
    * @param partAmt
    *  按区分终端类型
    */
  def calculateRates(liveCntDf: DataFrame, dfByRegion:DataFrame, range: Int, sqlContext: HiveContext, partAmt: Int): Unit = {
    import sqlContext.implicits._
    sqlContext.sql(s"use ${Constant.HIVE_DB}")
    val totalTimeDf = dfByRegion.selectExpr("f_date as t_date","f_hour as t_hour","f_timerange as t_timerange",
      "f_region_id as t_region_id","f_terminal as t_terminal","f_play_time as t_play_time")
    liveCntDf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    liveCntDf.registerTempTable("t_live_cnt")
    totalTimeDf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //按区分终端类型
    val regDevDf =
      liveCntDf.join(totalTimeDf,liveCntDf("f_date")===totalTimeDf("t_date") && liveCntDf("f_hour")===totalTimeDf("t_hour")
        && liveCntDf("f_timerange")===totalTimeDf("t_timerange")
        && liveCntDf("f_region_id")===totalTimeDf("t_region_id")
        && liveCntDf("f_terminal")===totalTimeDf("t_terminal"))
        .map(x => {
          val date = x.getAs[String]("f_date")
          val hour = x.getAs[String]("f_hour").toInt
          val timeRange = x.getAs[String]("f_timerange").toInt
          val provId = x.getAs[String]("f_province_id")
          val provName = x.getAs[String]("f_province_name")
          val cityId = x.getAs[String]("f_city_id")
          val cityName = x.getAs[String]("f_city_name")
          val regionId = x.getAs[String]("f_region_id")
          val regionName = x.getAs[String]("f_region_name")
          val terminal = x.getAs[String]("f_terminal")
          val channelId = x.getAs[String]("f_channel_id")
          val channelName = x.getAs[String]("f_channel_name")
          val userOnline = x.getAs[Int]("f_online_users")
          val registUsers = x.getAs[Int]("f_regist_users")
          val playTime = x.getAs[Long]("f_play_time") //当前频道播放时长
          val totalTime = x.getAs[Long]("t_play_time") //所有频道播放时长
          val audienceRate = playTime / (range.toLong * Constant.SYSTEM_HEARTBEAT_DURATION * registUsers)
          val arrivalRate = userOnline.toDouble / registUsers
          val audienceShare = playTime.toDouble / totalTime
          new LiveCount(date, hour, timeRange, provId,provName,cityId, cityName, regionId, regionName, terminal, channelId, channelName, userOnline, playTime, totalTime, audienceRate, arrivalRate, audienceShare)
        }).toDF()
    println("save rates calculation:" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    try{
      DBUtils.saveToMysql(regDevDf, Tables.T_CHNN_LIVE_REALTIME, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)
    }catch{
      case e:Exception => {
        println("收视率统计数据保存失败！")
        e.printStackTrace()
      }
    }
    println("save rates calculation end:" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    sqlContext.clearCache()
  }

  /**
    * 频道信息
    */
  def loadChannelInfo(sqlContext: HiveContext) = {
    val map = new mutable.HashMap[String, String]()
    val channel =
      """
        |(SELECT DISTINCT CAST(channel_id AS CHAR) AS channel_id,chinese_name AS f_channel_name
        |FROM channel_store
        |UNION
        |SELECT DISTINCT CAST(f_monitor_id AS CHAR) channel_id ,f_monitor_name AS f_channel_name
        |FROM t_monitor_store) as channel_store
      """.stripMargin
    val channelDF = DBUtils.loadMysql(sqlContext, channel, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    channelDF.collect().foreach(row => {
      val channelId = row.getAs[String]("channel_id")
      val channelName = row.getAs[String]("f_channel_name")
      map += (channelId -> channelName)
    })
    map
  }

  /**
    * 加载用户数统计数据-mysql
    * @param sqlContext
    * @return
    */
  def loadUserAmt(sqlContext: HiveContext)={
    val userAmtMap = new mutable.HashMap[(String,String),Int]()
    val sdevs=
      s"""
         |(SELECT a.device_id,a.device_type as terminal,cast(c.region_id as char) as region_id
         |from ${Tables.T_DEVICE_INFO} a
         |  JOIN ${Tables.T_ADDRESS_DEVICE} b ON a.device_id=b.device_id
         |  JOIN ${Tables.T_ADDRESS_INFO} c on c.address_id=b.address_id
         | WHERE a.status=1) as t_devs
       """.stripMargin
    DBUtils.loadMysql(sqlContext,sdevs, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
      .groupBy("terminal","region_id").count()
      .selectExpr("terminal","region_id","count as total_usr_amt")
      .collect().foreach(row => {
      val terminal = row.getAs[String]("terminal")
      val regionId = row.getAs[String]("region_id")
      val registUserAmt = row.getAs[Long]("total_usr_amt").toInt
      userAmtMap += ((terminal,regionId) ->registUserAmt)
    })
    userAmtMap
  }
  /**
    * 时间分段
    * lizhy added@20181024
    * @param date
    * @return
    */
  def getTimeRange(date: String,duration:Int): (String, Int, Int) = {
    val time = DateUtils.dateStrToDateTime(date)
    val day = date.substring(0, 10)
    val hour = time.getHourOfDay
    val minute = time.getMinuteOfHour
    val range = minute - minute%duration
    (day, hour, range)
    //    (day, hour, minute)
  }

  /**
    * 分解补充参数
    *
    * @param str
    * @return
    */
  def stringToMap(str: String): mutable.HashMap[String, String] = {
    val map = new mutable.HashMap[String, String]()
    if (str.trim.length > 0) {
      val kvs = str.split("&")
      for (kv <- kvs) {
        val keyValue = kv.split(",")
        if (keyValue.length == 2) {
          map += (keyValue(0) -> keyValue(1))
        }
      }
    }
    map
  }

  /**
    * 获取终端类型
    * @param sqlContext
    * @return
    */
  def getTerminalDf(sqlContext:HiveContext)={
    val devSql =
      s"""
         |(SELECT cast(device_id as char) as d_device_id,device_type as d_device_type from ${Tables.T_DEVICE_INFO} where status=1) as dev
       """.stripMargin
    DBUtils.loadMysql(sqlContext, devSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
  }
  /**
    * 删除历史数据 - 一天前的收视统计数据
    * @param sqlContext
    * @param date
    * @param hour
    * @param timerange
    */
  def deleteHistChannelRate(sqlContext:HiveContext,date:String,hour:Int,timerange:Int)={
    val delSql =
      s"""
         |delete from ${Tables.T_CHNN_LIVE_REALTIME}
         |WHERE f_date<'$date' and f_hour=$hour and f_timerange=$timerange
       """.stripMargin
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD,delSql)
  }

  /**
    * 删除历史数据 - 一天前的按频道分类播放时长数据
    * @param sqlContext
    * @param date
    * @param hour
    * @param timerange
    */
  def deleteHistPlaytime(sqlContext:HiveContext,date:String,hour:Int,timerange:Int)={
    val delSql =
      s"""
         |delete from ${Tables.T_CHANNEL_TYPE_PLAYTIME_REALTIME}
         |WHERE f_date<'$date' and f_hour=$hour and f_timerange=$timerange
       """.stripMargin
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD,delSql)
  }

  /**
    * 删除历史时间节点
    * @param date
    * @param hour
    * @param timerange
    */
  def deleteTimeNode(date:String,hour:Int,timerange:Int)={
    val delSql =
      s"""
         |delete from ${Tables.T_TIMENODE_REALTIME}
         |WHERE f_date<'$date'
         |and (
         |f_hour<$hour
         |or (f_hour=$hour and f_timerange<$timerange)
         |)
       """.stripMargin
    try{
      DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD,delSql)
    }catch {
      case e:Exception => e.printStackTrace()
    }
  }

  /**
    * 增加时间节点
    * @param date
    * @param hour
    * @param timerange
    */
  def addTimeNode(date:String,hour:Int,timerange:Int)={
    val dateTime = DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS)
    val addSql =
      s"""
         |INSERT INTO ${Tables.T_TIMENODE_REALTIME}(f_date,f_hour,f_timerange,f_update_time,f_if_rates_cnt,f_if_playtime_cnt,f_if_online_cnt)
         |SELECT '$date','$hour','$timerange','$dateTime',
         |(SELECT CASE WHEN COUNT(1)=0 THEN 0 ELSE 1 END FROM ${Tables.T_CHNN_LIVE_REALTIME} where f_date='$date' and f_hour='$hour' and f_timerange='$timerange') AS f_if_rates_cnt,
         |(SELECT CASE WHEN COUNT(1)=0 THEN 0 ELSE 1 END FROM ${Tables.T_CHANNEL_TYPE_PLAYTIME_REALTIME} where f_date='$date' and f_hour='$hour' and f_timerange='$timerange') as f_if_playtime_cnt,
         |(SELECT CASE WHEN COUNT(1)=0 THEN 0 ELSE 1 END FROM ${Tables.T_USER_COUNT_REALTIME} where f_date='$date' and f_hour='$hour' and f_timerange='$timerange') AS f_if_online_cnt
       """.stripMargin

    try{
      DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD,addSql)
    }catch {

      case e:Exception => {
        println("增加时间节点为失败！")
        e.printStackTrace()
      }
    }
  }
  /**
    * 获取频道分类
    * @param sparkSession
    * @param sqlContext
    * @return
    */
  def getChannelTypeDf(sparkSession: SparkSession,sqlContext:HiveContext): DataFrame = {
    import sqlContext.implicits._
    val temMap = new mutable.HashMap[String, String]()
    val channel =
      """
        |(SELECT DISTINCT CAST(channel_id AS CHAR) AS channel_id,chinese_name AS channel_name,f_subtype as subtypes
        |FROM channel_store
        |UNION
        |SELECT DISTINCT CAST(f_monitor_id AS CHAR) channel_id ,f_monitor_name AS channel_name,f_sub_type AS subtypes
        |FROM t_monitor_store) as channel_store
      """.stripMargin
    val channelDF = DBUtils.loadMysql(sqlContext, channel, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
      .map(x => {
        val channelList = new ListBuffer[(String,String,String)]
        val channelId = x.getAs[String]("channel_id")
        val channelName = x.getAs[String]("channel_name")
        val subtypes = x.getAs[String]("subtypes")
        var subtype = "-1"
        if(subtypes == ""){
          channelList += ((channelId,channelName,subtype))
        }else{
          val subtypeArr = subtypes.split("\\|")
          for(subtypeArrEle <- subtypeArr){
            channelList += ((channelId,channelName,subtypeArrEle))
          }
        }
        channelList.toList
      }).flatMap(x => x).toDF()
      .withColumnRenamed("_1","channel_id")
      .withColumnRenamed("_2","channel_name")
      .withColumnRenamed("_3","subtype_id")
    // 将频道分类 1-标清，2-高清，3-央视，4-地方（本地），5-其他
    val subTypeInfo =
      """
        |(select (case  when  f_name like '%标清%' then '1'
        |when f_name like '%高清%' then '2'
        |when f_name like '%央视%' then '3'
        |when (f_name like '%本地%' or f_name like '%地方%') then '4'
        |else '5' end) as channel_type_id,
        |f_original_id from t_media_type_info where f_status=5
        |) as channelType
      """.stripMargin
    val channelTypeDF = DBUtils.loadMysql(sqlContext, subTypeInfo, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)

    channelDF.join(channelTypeDF,channelDF("subtype_id")===channelTypeDF("f_original_id"),"left_outer")
      .selectExpr("channel_id as c_channel_id","channel_name as c_channel_name","nvl(channel_type_id,'5') as c_channel_type_id",
        "case when channel_type_id = '1' then '标清频道' when channel_type_id = '2' then '高清频道' when channel_type_id = '3' then '央视频道' when channel_type_id = '4' then '地方频道' else '其它频道' end as f_channel_type_name")
  }

  /**
    * 按频道分类统计播放时长（页面显示收视份额比例）
    * @param dfByChannel
    * @param channelTypeDf
    * @param sqlContext
    */
  def calculateUserCntByChannelType(dfByChannel:DataFrame,channelTypeDf:DataFrame,sqlContext:HiveContext)={
    val playtimeDf =
      dfByChannel.join(channelTypeDf,dfByChannel("f_channel_id")===channelTypeDf("c_channel_id"))
        .withColumnRenamed("c_channel_type_id","f_channel_type_id")
        .withColumnRenamed("c_channel_type_name","f_channel_type_name")
        .groupBy("f_date","f_hour","f_timerange","f_province_id","f_province_name","f_city_id","f_city_name","f_region_id"
          ,"f_region_name","f_terminal","f_channel_id","f_channel_name","f_channel_type_id","f_channel_type_name")
        .sum("f_play_time").withColumnRenamed("sum(f_play_time)","f_play_time")
        .selectExpr("f_date","f_hour","f_timerange","f_province_id","f_province_name","f_city_id","f_city_name","f_region_id"
          ,"f_region_name","f_terminal","f_channel_id","f_channel_name","f_channel_type_id","f_channel_type_name","f_play_time")
    println("save playtime by channel type begin:" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    try{
      DBUtils.saveToMysql(playtimeDf, Tables.T_CHANNEL_TYPE_PLAYTIME_REALTIME, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)
    }catch{
      case e: Exception =>{
        println("频道分类时长统计数据保存失败！")
        e.printStackTrace()
      }
    }
    println("save playtime by channel type end:" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
  }
  /**
    * 获取区域码
    * 获取的是默认区域码，如果区域码为省级，则改为省|01|01
    * 如果区域码为市级，则改为省|市|01
    */
  def getDefaultRegionCode(): String = {
    var regionCode = RegionUtils.getRootRegion
    println("regionCode==" + regionCode)
    if (regionCode.endsWith("0000")) {
      regionCode = (regionCode.toInt + 101).toString
    } else {
      regionCode = (regionCode.toInt + 1).toString
    }
    regionCode
  }

  /**
    * 计算直播在线用户数
    * @param liveDf
    * @param sqlContext
    */
  def calculateOnlineUser(liveDf:DataFrame,sqlContext:HiveContext)={
    val serviceType = "live"
    val liveOnlineDf = liveDf.selectExpr("f_date","f_hour","f_timerange","f_province_id","f_province_name","f_city_id","f_city_name",
      "f_region_id","f_region_name","f_terminal",s"'${serviceType}' as f_service_type","f_online_users as f_user_count")
    try{
      DBUtils.saveToMysql(liveOnlineDf, Tables.T_USER_COUNT_REALTIME, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)
    }catch{
      case e:Exception => {
        println("在线用户数保存失败！")
        e.printStackTrace()
      }
    }
  }

  def saveLiveUsers(liveDf:DataFrame,sqlContext:HiveContext) = {
    //(date, hour, timeRange, regionId, terminal, channelId, userId, playTime,dateTime)
    //f_date:String,f_hour:Int,f_timerange:Int,f_region_id:String,f_terminal:Int,f_channel_id:String,f_user_id:String,f_play_time:Long,f_date_time:String
    val registUserDf = getRegistUser(sqlContext)
    val saveUserDf = liveDf.join(registUserDf,liveDf("f_user_id")===registUserDf("r_user_id"),"left_outer")
      .where("r_update_time is not null and f_date_time>=r_update_time or r_update_time is null")
      .selectExpr("f_user_id","f_terminal","f_region_id","'live' as f_service_type","'' as f_program_id","f_date_time as f_update_time","1 as f_online_status")
    DBUtils.saveDataFrameToPhoenixNew(saveUserDf,Tables.T_RUNLOG_USER_STATUS_REALTIME)
    /*f_user_id varchar,f_terminal tinyint,f_region_id varchar,f_service_type varchar,f_program_id varchar,f_update_time varchar,f_online_status tinyint,*/
  }

  /**
    * 获取注册用户列表
    * @param sqlContext
    * @return
    */
  def getRegistUser(sqlContext:HiveContext):DataFrame = {
    val phoenixSql =
      s"""
         |(select f_user_id,f_update_time
         |from ${Tables.T_RUNLOG_USER_STATUS_REALTIME}) as allUsers
      """.stripMargin
    DBUtils.loadDataFromPhoenix2(sqlContext, phoenixSql).selectExpr("f_user_id as r_user_id","f_update_time as r_update_time")
  }

  /*def processChannelLive(sc:SparkContext,liveDf:DataFrame, sparkSession: SparkSession, groupId: String, partitionNum: Int,duration: Int,sqlContext: HiveContext) = {
      import sqlContext.implicits._
      //地域信息
      val regionInfo = loadRegionInfo(sparkSession)
      val regionBC = sc.broadcast[mutable.HashMap[String, (String, String, String,String, String)]](regionInfo)
      val defaultRegionBC = sc.broadcast[String](getDefaultRegionCode())
      //频道信息
      val channelInfo = loadChannelInfo(sqlContext)
      val channelBC = sc.broadcast[mutable.HashMap[String, String]](channelInfo)
      //频道分类信息
      val channelTypeDf = getChannelTypeDf(sparkSession,sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
      //各区域用户总数信息
      val registUserMap = loadUserAmt(sqlContext)
      val registUserMapBC = sc.broadcast[mutable.HashMap[(String,String),Int]](registUserMap)
      //终端类型信息

      val time = DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS)
      val timeRangeSet: (String, Int, Int) = getTimeRange(time,duration.toInt)
      deleteHistChannelRate(sqlContext:HiveContext,timeRangeSet._1:String,timeRangeSet._2:Int,timeRangeSet._3:Int) //删除历史收视统计
      deleteHistPlaytime(sqlContext:HiveContext,timeRangeSet._1:String,timeRangeSet._2:Int,timeRangeSet._3:Int) //删除历史播放时长统计

      val liveUserSql =
        s"""
          |(select f_user_id,f_terminal,f_region_id,f_service_type,f_program_id,f_update_time
          |from ${Tables.T_USER_STATUS_REALTIME} WHERE f_online_status = 1 and f_service_type = '${GatherType.LIVE_NEW}') as live_user
        """.stripMargin
      val liveDf = DBUtils.loadDataFromPhoenix2(sqlContext,liveUserSql)
        .map(x => {
        val userId = x.getAs[String]("f_user_id")
        val terminal = x.getAs[String]("f_terminal")
        val channelId = x.getAs[String]("f_program_id")
        val dateTime = DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS)
        val playTime = Constant.SYSTEM_HEARTBEAT_DURATION * duration
        val regionId = x.getAs[String]("region_id")
        ChannelLiveDetail(dateTime, regionId, terminal, channelId, userId, playTime,dateTime)
      }).toDF()
      //单频道
      val dfByChannel = detailDF.groupBy("f_date","f_hour","f_timerange","f_region_id","f_terminal","f_channel_id","f_user_id")
        .sum("f_play_time")
        .withColumnRenamed("sum(f_play_time)","f_play_time")
        .map(x =>{
          val date = x.getAs[String]("f_date")
          val hour = x.getAs[Int]("f_hour")
          val timeRange = x.getAs[Int]("f_timerange")
          val regionId = x.getAs[String]("f_region_id")
          val terminal = x.getAs[Int]("f_terminal")
          val channelId = x.getAs[String]("f_channel_id")
          val userId = x.getAs[String]("f_user_id")
          val playTime = x.getAs[Long]("f_play_time")
          val key = date + "," + hour + "," + timeRange + "," + regionId + "," + terminal + "," + channelId
          val value = (1,playTime)
          (key,value)
        }).reduceByKey((x,y) => {
        (x._1 + y._1,x._2 + y._2)
      }).map(x => {
        val keyArr = x._1.split(",")
        val date = keyArr(0)
        val hour = keyArr(1)
        val timeRange = keyArr(2)
        var regionId = keyArr(3)
        val regionMap = regionBC.value //(regionCode -> (regionName, cityCode, cityName,provCode,provName))
        val defaultRegionMap = regionMap.getOrElse(defaultRegionBC.value,("","","","",""))
        val (regionName,city_id,city_name,provinceId,provinceName) = regionMap.getOrElse(regionId,(defaultRegionMap._1,defaultRegionMap._2,defaultRegionMap._3,defaultRegionMap._4,defaultRegionMap._5))
        if (regionMap.get(regionId).size == 0){
          regionId = defaultRegionBC.value
        }
        val terminal = keyArr(4)
        val channelId = keyArr(5)
        val channelName = channelBC.value.getOrElse(channelId,"unknown")
        val registUserCnt = registUserMapBC.value.getOrElse((terminal,regionId),0)
        val userCnt = x._2._1
        val playTime = x._2._2
        (date,hour,timeRange,regionId,regionName,city_id,city_name,provinceId,provinceName,terminal,channelId,channelName,playTime,userCnt,registUserCnt)
      }).toDF()
        .withColumnRenamed("_1","f_date")
        .withColumnRenamed("_2","f_hour")
        .withColumnRenamed("_3","f_timerange")
        .withColumnRenamed("_4","f_region_id")
        .withColumnRenamed("_5","f_region_name")
        .withColumnRenamed("_6","f_city_id")
        .withColumnRenamed("_7","f_city_name")
        .withColumnRenamed("_8","f_province_id")
        .withColumnRenamed("_9","f_province_name")
        .withColumnRenamed("_10","f_terminal")
        .withColumnRenamed("_11","f_channel_id")
        .withColumnRenamed("_12","f_channel_name")
        .withColumnRenamed("_13","f_play_time")
        .withColumnRenamed("_14","f_online_users")
        .withColumnRenamed("_15","f_regist_users")
      //所有频道
      val dfByRegion = detailDF.groupBy("f_date","f_hour","f_timerange","f_region_id","f_terminal","f_user_id")
        .sum("f_play_time")
        .withColumnRenamed("sum(f_play_time)","f_play_time")
        .map(x =>{
          val date = x.getAs[String]("f_date")
          val hour = x.getAs[Int]("f_hour")
          val timeRange = x.getAs[Int]("f_timerange")
          val regionId = x.getAs[String]("f_region_id")
          val terminal = x.getAs[Int]("f_terminal")
          val userId = x.getAs[String]("f_user_id")
          val playTime = x.getAs[Long]("f_play_time")
          val key = date + "," + hour + "," + timeRange + "," + regionId + "," + terminal
          val value = (1,playTime)
          (key,value)
        }).reduceByKey((x,y) => {
        (x._1 + y._1,x._2 + y._2)
      }).map(x => {
        val keyArr = x._1.split(",")
        val date = keyArr(0)
        val hour = keyArr(1)
        val timeRange = keyArr(2)
        var regionId = keyArr(3)
        val regionMap = regionBC.value
        val defaultRegionMap = regionMap.getOrElse(defaultRegionBC.value,("","","","",""))
        val (regionName,city_id,city_name,provinceId,provinceName) = regionMap.getOrElse(regionId,(defaultRegionMap._1,defaultRegionMap._2,defaultRegionMap._3,defaultRegionMap._4,defaultRegionMap._5))
        if (regionMap.get(regionId).size == 0){
          regionId = defaultRegionBC.value
        }
        val terminal = keyArr(4)
        val userCnt = x._2._1
        val playTime = x._2._2
        (date,hour,timeRange,regionId,regionName,city_id,city_name,provinceId,provinceName,terminal,playTime,userCnt)
      }).toDF()
        .withColumnRenamed("_1","f_date")
        .withColumnRenamed("_2","f_hour")
        .withColumnRenamed("_3","f_timerange")
        .withColumnRenamed("_4","f_region_id")
        .withColumnRenamed("_5","f_region_name")
        .withColumnRenamed("_6","f_city_id")
        .withColumnRenamed("_7","f_city_name")
        .withColumnRenamed("_8","f_province_id")
        .withColumnRenamed("_9","f_province_name")
        .withColumnRenamed("_10","f_terminal")
        .withColumnRenamed("_11","f_play_time")
        .withColumnRenamed("_12","f_online_users")
      try{
        calculateOnlineUser(dfByRegion,sqlContext) //在线用户数统计
        println("开始直播用户状态列表："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
        saveLiveUsers(detailDF,sqlContext) //保存、更新用户状态列表
        println("结束直播用户状态列表："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
        //calculateUserCntByChannelType(dfByChannel,channelTypeDf,sqlContext) //按频道分类播放时长统计
        //calculateRates(dfByChannel,dfByRegion, duration, sqlContext, partitionNum) //计算收视率、收视份额、到达率等
        //deleteTimeNode(timeRangeSet._1,timeRangeSet._2,timeRangeSet._3) //删除节点
        //addTimeNode(timeRangeSet._1,timeRangeSet._2,timeRangeSet._3) //增加当前节点
      }catch {
        case e:Exception => {
          e.printStackTrace()
          sqlContext.clearCache()
          sqlContext.emptyDataFrame
        }
      }finally {
        regionBC.destroy()
        channelBC.destroy()
      }
  }*/
}
