package cn.ipanel.homed.realtime

import cn.ipanel.common.{CluserProperties, DBProperties, SparkSession, Tables}
import cn.ipanel.utils.{DBUtils, DateUtils}
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import cn.ipanel.homed.realtime.GatherConstant

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 用户上报在线用户实时程序
  * lizhy@20190301
  */
object UserStatusByUserLog {
  val HEARTBEAT_TIME_SEC = 60
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
    val session = SparkSession("OnlineUsersByUserLog")
    val sparkContext = session.sparkContext
//    session.sparkContext.getConf.registerKryoClasses(Array(classOf[ChannelLiveDetail]))
    val topics = Set(topic)
    val brokers = CluserProperties.KAFKA_BROKERS
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "group.id" -> groupId
      ,//"serializer.class" -> "kafka.serializer.StringEncoder",
      "auto.offset.reset" -> "largest")
    val ssc = new StreamingContext(sparkContext, Durations.minutes(duration.toLong))
    val message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val sqlContext: HiveContext = session.sqlContext
    processUserLogMessage(message, session, groupId,partition.toInt, duration.toInt,sqlContext: HiveContext)
    //开启ssc
    ssc.start()
    println("waiting for User Log messages..")
    //等待计算完成
    ssc.awaitTermination()

  }


  def processUserLogMessage(message: InputDStream[(String, String)], sparkSession: SparkSession, groupId: String, partitionNum: Int,duration: Int,sqlContext: HiveContext) = {
    message.foreachRDD(rdd => {
      println("message count:" + rdd.count())
      val sc = sparkSession.sparkContext
      import sqlContext.implicits._
      //终端类型信息
      val terminalDf = getTerminalDf(sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val filterRdd = rdd.filter(mess => {
        mess._2.contains("[" + GatherConstant.USER_LOG_HEARTBREATH +",") && mess._2.contains("(S,1)" ) || (mess._2.contains("[" + GatherConstant.USER_REPORT_LIVE +",") && !mess._2.contains("(S,0)" ))
      })
//      filterRdd.toDF().show(false)
      val nowTime = DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMM)
      val nowTimeBC = sc.broadcast(nowTime)

//      val nowTimestamp = new DateTime().getMillis
      val userLogDf = filterRdd
        .filter(_._2.split("\\|").length>1)
        .map(line => {
          val data = line._2.substring(3).replace("[", "").replace("]", "").replace("<", "").replace(">", "").replace("(", "").replace(")", "")
          val datas = data.split("\\|")
          val base = datas(0).split(",") //[service,event_time,userid,region,deviceid]
          val params = stringToMap(datas(1))
          val serviceCode = base(0)
          val serviceType = if(serviceCode == GatherConstant.USER_REPORT_LIVE || (serviceCode == GatherConstant.USER_LOG_HEARTBREATH && params.getOrElse("S", "-") == GatherConstant.BREAK_SCEN_LIVE)) GatherConstant.LIVE else "-"
          val logTimeStamp = base(1).toLong // if((base(1).toLong - nowTimestamp).abs > duration * HEARTBEAT_TIME_SEC * 1000) nowTimestamp else base(1).toLong
          val logDateTime = DateUtils.millisTimeStampToDate(logTimeStamp,DateUtils.YYYY_MM_DD_HHMMSS)
          val dateTime = nowTimeBC.value//nowTime // DateUtils.millisTimeStampToDate(timeStamp,DateUtils.YYYY_MM_DD_HHMM)
          val userId = base(2)
          val region = base(3)
          val deviceId = base(4)
          val channelId = params.getOrElse("I", params.getOrElse("ID", "-"))
          val video_id = params.getOrElse("I2","-")
          (userId,deviceId,serviceType,channelId,dateTime,region,logTimeStamp,serviceCode,logDateTime,video_id)
        }).toDF()
        .withColumnRenamed("_1","user_id")
        .withColumnRenamed("_2","device_id")
        .withColumnRenamed("_3","service_type")
        .withColumnRenamed("_4","program_id")
        .withColumnRenamed("_5","update_time")
        .withColumnRenamed("_6","region_id")
        .withColumnRenamed("_7","log_timestamp")
        .withColumnRenamed("_8","service_code")
        .withColumnRenamed("_9","log_time")
        .withColumnRenamed("_10","video_id")
      //最后日志时间
      val maxTimeLogDf = userLogDf.groupBy("user_id").max("log_timestamp").withColumnRenamed("max(log_timestamp)","m_log_timestamp")
          .selectExpr("user_id as m_user_id","m_log_timestamp as m_log_timestamp").persist(StorageLevel.MEMORY_AND_DISK_SER)
      //最后业务日志时间
      val maxTimeBusLogDf = userLogDf.where(s"service_code = ${GatherConstant.USER_REPORT_LIVE}")//.registerTempTable("t_maxtime_bus_log")
        .groupBy("user_id").max("log_timestamp")
        .withColumnRenamed("max(log_timestamp)","b_log_timestamp")
        .selectExpr("user_id as b_user_id","b_log_timestamp",s"from_unixtime(b_log_timestamp,'${DateUtils.YYYY_MM_DD_HHMMSS}') as bus_log_time")
//        .selectExpr("user_id as b_user_id","b_log_timestamp",s"from_unixtime(b_log_timestamp,'${DateUtils.YYYY_MM_DD_HHMMSS}') as bus_log_time","b_play_count")
      println("最后业务日志时间:")
//      maxTimeBusLogDf.show(100)
      val tarUserLogDf = userLogDf.join(maxTimeLogDf,userLogDf("user_id")===maxTimeLogDf("m_user_id") && userLogDf("log_timestamp")===maxTimeLogDf("m_log_timestamp"),"inner")
        .join(maxTimeBusLogDf,userLogDf("user_id")===maxTimeBusLogDf("b_user_id")&& userLogDf("log_timestamp")===maxTimeBusLogDf("b_log_timestamp"),"left_outer")
        .selectExpr("user_id","device_id","program_id","update_time","region_id","bus_log_time","log_time as last_log_time","service_type"
          ,"case when b_user_id is not null then 1 else 0 end as f_play_count","video_id as f_video_id")
      tarUserLogDf.persist(StorageLevel.MEMORY_AND_DISK_SER)
      val userDevDf = tarUserLogDf.join(terminalDf,tarUserLogDf("device_id")===terminalDf("d_device_id"),"left_outer")
        .selectExpr("user_id as f_user_id","nvl(d_device_type,0) as f_terminal","region_id as f_region_id","service_type as f_service_type","program_id as f_program_id"
          ,"f_video_id","nvl(bus_log_time,'1') as f_start_play_time","update_time as f_update_time","1 as f_online_status","last_log_time as f_log_time","f_play_count")
      try{
        println("开始更新用户状态列表："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
//        println("用户设备类型：")
//        userDevDf.show(100)
        updateOnlineUserStatus(userDevDf,sqlContext) //更新用户状态
        println("结束更新用户状态列表："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
      }catch {
        case e:Exception => {
          e.printStackTrace()
        }
      }finally {
        nowTimeBC.destroy()
        terminalDf.unpersist()
        tarUserLogDf.unpersist()
        maxTimeLogDf.unpersist()
        sqlContext.clearCache()
        sqlContext.emptyDataFrame
      }
    })
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
    * f_user_id varchar not null,
    * f_terminal tinyint,
    * f_region_id varchar,
    * f_service_type varchar,
    * f_program_id varchar,
    * f_start_play_time varchar,
    * f_update_time varchar,
    * f_online_status tinyint,
    * f_log_time varchar,
    */
  def getLastOnlineStatus(sqlContext:SQLContext) ={
    val onlineSql =
      s"""
        |(select f_user_id,f_terminal,f_region_id,f_service_type,f_update_time,f_log_time
        |from ${Tables.T_USERLOG_USER_STATUS_REALTIME}
        |where f_online_status=1) as online_user
      """.stripMargin
    DBUtils.loadDataFromPhoenix2(sqlContext,onlineSql)
      .selectExpr("f_user_id as l_user_id","f_terminal as l_terminal","f_region_id as l_region_id"
        ,"f_update_time as l_update_time","f_log_time as l_log_time")
  }
  /**
    * 更新用户状态 - 用户上报
    * @param userLogDf
    * @param sqlContext
    */
  def updateOnlineUserStatus(userLogDf:DataFrame,sqlContext:SQLContext): Unit ={
    val lastOnlineDf = getLastOnlineStatus(sqlContext)
    val updateDf = userLogDf.join(lastOnlineDf,userLogDf("f_user_id")===lastOnlineDf("l_user_id"),"outer")
      .selectExpr("nvl(f_user_id,l_user_id) as f_user_id","nvl(f_terminal,l_terminal) as f_terminal","nvl(f_region_id,l_region_id) as f_region_id"
      ,"nvl(f_service_type,'0') as f_service_type","nvl(f_program_id,'0') as f_program_id","f_video_id",
        "case when f_start_play_time='1' then l_update_time when f_start_play_time is null then '0' end  as f_start_play_time"
      ,"nvl(f_update_time,l_update_time) as f_update_time","nvl(f_online_status,0) as f_online_status","nvl(f_log_time,l_log_time) as f_log_time"
      ,"f_play_count")
    try{
      println("保存用户状态：")
//      updateDf.show(100)
      println("插入记录数：" + updateDf.count())
      DBUtils.saveDataFrameToPhoenixNew(updateDf,Tables.T_USERLOG_USER_STATUS_REALTIME)
    }catch{
      case e:Exception => {
        println("更新用户上报状态失败！")
        e.printStackTrace()
      }
    }
  }
}
