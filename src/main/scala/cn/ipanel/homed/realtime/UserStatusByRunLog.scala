package cn.ipanel.homed.realtime

import java.util.regex.Pattern

import cn.ipanel.common._
import cn.ipanel.homed.realtime.GatherConstant
import cn.ipanel.utils._
import com.mysql.jdbc.StringUtils
import jodd.util.StringUtil
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.storage.StorageLevel

/**
  * 实时处理在线用户状态
  * @author lizhy@20181103
  */

case  class VideoPlayLog(userId:String,logKey:String,dateTime:String,deviceType:String,serviceType:String,programId:String,playToken:String)
case class User(userId:String,deviceId:String)
case class UserOnlineStatus( f_user_id:String,
                              f_terminal:Int,
//                              f_region_id:String,
                              f_service_type:String,
                              f_program_id:String,
                              f_start_play_time:String,
                              f_update_time:String,
                              f_playtoken:String,
                              f_online_status:Int,
                             f_log_time:String,
                             f_play_count:Long)
object UserStatusByRunLog {
  private val pro = PropertiUtils.init("redis.properties")
  private lazy val SPLIT = ","
  /**
    * @param args args(0):groupid,args(1):topic,args(2):duration,args(3):partition
    */
  def main(args: Array[String]): Unit = {
    if(args.length != 4){
      println("请输入正确参数!")
      System.exit(1)
    }
    val groupId = args(0)
    val topic = args(1)
    val duration = args(2)
    val partition = args(3)

    //1.初始化spark上下文
    val session = SparkSession("UserStatusByRunLog")
    val sparkContext = session.sparkContext
    val topics = Set(topic)
    val brokers = CluserProperties.KAFKA_BROKERS
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "group.id" -> groupId
      ,// "serializer.class" -> "kafka.serializer.StringEncoder",
      "auto.offset.reset" -> "largest")
    val ssc = new StreamingContext(sparkContext,Durations.minutes(duration.toLong))
    val messsage: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    processRunLogMessage(messsage,session,groupId,partition.toInt) //处理kafka消息
    //开启spark streaming context
    ssc.start()
    println("waiting for Run Log messages..")
    ssc.awaitTermination()

  }

  /**
    * 获取socket在线用户
    * @param sqlContext
    * @return
    */
  def getSocketUser(sqlContext: SQLContext): DataFrame ={
    val sockeUserSql =
      s"""
        |(select cast(f_user_id as char) as s_user_id,cast(f_device_type as char) as s_terminal,f_region_id as s_region_id
        |from ${Tables.T_USER_ONLINE_WEBSOCKET} ) as socketUser
      """.stripMargin
    DBUtils.loadMysql(sqlContext, sockeUserSql, DBProperties.JDBC_URL_IACS, DBProperties.USER_IACS, DBProperties.PASSWORD_IACS)
  }

  /**
    * 获取上次在线用户
    * @param sqlContext
    * @return
    */
  def getLastOnlineUser(sqlContext:SQLContext):DataFrame = {
    val phoenixSql =
      s"""
        |(select f_user_id,f_terminal,f_region_id,f_service_type,f_program_id,f_update_time,f_online_status,f_playtoken,f_start_play_time,f_log_time
        |from ${Tables.T_RUNLOG_USER_STATUS_REALTIME}
        |where f_online_status=1) as allUsers
      """.stripMargin
    DBUtils.loadDataFromPhoenix2(sqlContext, phoenixSql)
      .selectExpr("f_user_id as o_user_id","f_terminal as o_terminal","f_region_id as o_region_id","f_service_type as o_service_type","f_program_id as o_program_id",
        "f_update_time as o_update_time", "f_online_status as o_online_status", "f_playtoken as o_playtoken", "f_start_play_time as o_start_play_time","f_log_time as o_log_time")
  }
  //
  def processRunLogMessage(message:InputDStream[(String,String)], sparkSession:SparkSession, groupId:String, partitionNum:Int):Unit ={
    message.foreachRDD(rdd => {
      println("数据处理开始："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
      println("message count:" + rdd.count())
      val sc = sparkSession.sparkContext
      val nowTime = DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMM)
      val nowTimeBC = sc.broadcast(nowTime)
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._
      val userRegionDf = getUserRegion(sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val gatherTypeBC = sc.broadcast(GatherConstant.RUN_LOG_GATHER_TYPE)
      val allLog = rdd.filter(log =>{
        log._2.contains(GatherConstant.videoSuccess) ||
          (log._2.contains(GatherConstant.videoFinished) && !log._2.contains(GatherConstant.RUN_LOG_LIVE_PRO_KEY))|| //直播推流只持续很短时间问题，忽略finish，会有误差
          log._2.contains(GatherConstant.videoSuccessNew) ||
          log._2.contains(GatherConstant.videoFinishedNew) //|| log._2.contains(LogConstant.programExit)//筛选视屏 播放成功、播放结束、播放退出 日志
      }).map(line =>{
        line._2.replace(GatherConstant.RUN_LOG_ONE_KEY_TIMESHIFT,GatherConstant.RUN_LOG_TIME_SHIFT)
          .replace(GatherConstant.RUN_LOG_ONE_KEY_TIMESHIFT_NEW,GatherConstant.RUN_LOG_TIME_SHIFT_NEW)
      })
      println("allLogDf:")
      allLog.toDF().show(100,false)
      //处理run日志上报用户
      import org.apache.spark.sql.expressions.Window
      import org.apache.spark.sql.functions._
      val allLogDf = getDfFromLog(allLog,sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
      /*println("allLogDf cnt:" + allLogDf.count())
      allLogDf.show(false)*/
      val maxSpec = Window.partitionBy("userId","playToken")
      val logDf = allLogDf.withColumn("lastDateTime",max("dateTime") over(maxSpec))
        .where(s"dateTime = lastDateTime and userId is not null")
        .map(x => {
          val userId = x.getAs[String]("userId")
          val logKey = x.getAs[String]("logKey")
          val dateTime = x.getAs[String]("dateTime") //.substring(0,19)
          val deviceType = x.getAs[String]("deviceType")
          //终端1stb,2 CA卡,3mobile,4 pad, 5pc
          val terminal = deviceType match{
            case "stb" => "1"
            case "CA" => "2"
            case "mobile" => "3"
            case "pad" => "4"
            case "pc" => "5"
            case _ => "0"
          }
          val playToken = x.getAs[String]("playToken")
          var serviceType = x.getAs[String]("serviceType")
          val programId = x.getAs[String]("programId")
        (userId,(userId,dateTime,terminal,serviceType,programId,logKey,playToken))
      })
        .filter(x => gatherTypeBC.value.contains(x._2._4)) //过滤run日志的类型
        .reduceByKey((x,y) => {
        if(x._6 == GatherConstant.videoSuccess || x._6 == GatherConstant.videoSuccessNew){
          (x)
        }else{
          (y)
        }
      }).map(x =>{
        val value = x._2
        var serviceType = ""
        var programId = ""
        if (value._6 == GatherConstant.videoSuccess || value._6 == GatherConstant.videoSuccessNew){
          serviceType = getServiceType(value._4) //转换成通用的业务类型码
          programId = value._5
        }else{
          serviceType = "0"
          programId = "0"
        }
        (value._1,value._2,value._3,serviceType,programId,value._7,value._6)
      }).toDF()
        .withColumnRenamed("_1","user_id")
        .withColumnRenamed("_2","date_time")
        .withColumnRenamed("_3","terminal")
        .withColumnRenamed("_4","service_type")
        .withColumnRenamed("_5","program_id")
        .withColumnRenamed("_6","play_token")
        .withColumnRenamed("_7","log_key")
      println("logDf cnt:" + logDf.count())
      logDf.show(false)
      //iacs数据库socket在线用户表用户
      val socketUserDf = getSocketUser(sqlContext)
      //phoenix上次在线用户
      val lastOnlineDf = getLastOnlineUser(sqlContext)
      //（1）日志用户与socket用户右关连，得到结果： 1维持原本状态 0结束状态为未知状态
      val onlineUsrDf = logDf.join(socketUserDf,logDf("user_id")===socketUserDf("s_user_id"),"outer")
        .map(x => {
          var userId = x.getAs[String]("s_user_id")
          if(userId == null){
            userId = x.getAs[String]("user_id")
            println("the user is not connecting to socket.user_id = " + userId)
          }
          var terminal = x.getAs[String]("terminal")
          if(terminal == null){
            terminal = x.getAs[String]("s_terminal")
          }
          var serviceType = x.getAs[String]("service_type")
          if(StringUtils.isNullOrEmpty(serviceType)){
            serviceType = "1"
          }
          var programId = x.getAs[String]("program_id")
          if(StringUtils.isNullOrEmpty(programId)){
            programId = "1"
          }
          val onlineStatus = 1
          val dateTime = if(StringUtils.isNullOrEmpty(x.getAs[String]("date_time")))  "1" else x.getAs[String]("date_time")
          val playToken = if(StringUtils.isNullOrEmpty(x.getAs[String]("play_token"))) "1" else x.getAs[String]("play_token")
          val logKey = if(StringUtils.isNullOrEmpty(x.getAs[String]("log_key"))) "1" else x.getAs[String]("log_key")
          val logTime = if(StringUtils.isNullOrEmpty(x.getAs[String]("date_time")))  "0" else x.getAs[String]("date_time")
          (userId,terminal,serviceType,programId,dateTime,playToken,onlineStatus,logKey,logTime)
        }).toDF()
        .withColumnRenamed("_1","user_id")
        .withColumnRenamed("_2","terminal")
        .withColumnRenamed("_3","service_type")
        .withColumnRenamed("_4","program_id")
        .withColumnRenamed("_5","date_time")
        .withColumnRenamed("_6","play_token")
        .withColumnRenamed("_7","online_status")
        .withColumnRenamed("_8","log_key")
        .withColumnRenamed("_9","log_time")
      println("onlineUsrDf cnt:" + onlineUsrDf.count())
//      onlineUsrDf.show(100,false)
      //（2） 在线用户与上次在线用户的全连接
      val onAnOutUserDf = onlineUsrDf.join(lastOnlineDf,onlineUsrDf("user_id")===lastOnlineDf("o_user_id"),"outer")
        .map(x => {
          var userId = x.getAs[String]("user_id")
          var onlineStatus = 1
          if(StringUtils.isNullOrEmpty(userId)){
            userId = x.getAs[String]("o_user_id")
            onlineStatus = 0
          }
          var terminal = x.getAs[Int]("o_terminal")
          if(terminal == 0){
            try{
              terminal = x.getAs[String]("terminal").toInt
            }catch {
              case e :Exception => {
                terminal = 1
                e.printStackTrace()
              }
            }
          }
          val logKey = x.getAs[String]("log_key")
          val logServiceType = x.getAs[String]("service_type")
          val logProgramId = x.getAs[String]("program_id")
          val logPlayToken = x.getAs[String]("play_token")
          val logStartPlayTime = x.getAs[String]("date_time")
          val logLogTime = x.getAs[String]("log_time")
          val lastServiceType = x.getAs[String]("o_service_type")
          val lastProgram_id = x.getAs[String]("o_program_id")
          val lastStartPlayTime = x.getAs[String]("o_start_play_time")
          val lastPlayToken = x.getAs[String]("o_playtoken")
          val lastLogTime = x.getAs[String]("o_log_time")
          //初始置为 - 未知状态
          var serviceType = "0"
          var programId = "0"
          var startPlayTime = "0"
          var playToken = "0"
          var logTime = if(logLogTime != null) logLogTime else if(logLogTime == null && lastLogTime != null) lastLogTime else "lastLogTime"
          var playCount = 0L
          if(logKey == "1" && lastServiceType != null){
              serviceType = lastServiceType
              programId = lastProgram_id
              startPlayTime = lastStartPlayTime
              playToken = lastPlayToken
              logTime = lastLogTime
          }else if(logKey == GatherConstant.videoSuccess || logKey == GatherConstant.videoSuccessNew){
            serviceType = logServiceType
            programId = logProgramId
            startPlayTime = if(logStartPlayTime.length > 19) logStartPlayTime.substring(0,19) else logStartPlayTime
            playToken = logPlayToken
            logTime = logLogTime
            playCount = 1L
          }else if(logKey == GatherConstant.videoFinished || logKey == GatherConstant.videoFinishedNew){
            if(lastPlayToken != null && logPlayToken != lastPlayToken){
              serviceType = lastServiceType
              programId = lastProgram_id
              startPlayTime = lastStartPlayTime
              playToken = lastPlayToken
              logTime = lastLogTime
            }else{
              serviceType = "0"
              programId = "0"
              startPlayTime = "0"
              playToken = "0"
              logTime = "0"
            }
          }
          val updateTime = nowTimeBC.value
          UserOnlineStatus(userId,terminal,serviceType,programId,startPlayTime,updateTime,playToken,onlineStatus,logTime,playCount)
        }).toDF()

      val saveOnlineUserDf = onAnOutUserDf.join(userRegionDf,onAnOutUserDf("f_user_id")===userRegionDf("r_user_id"),"left_outer")
          .selectExpr("f_user_id","f_terminal","nvl(r_region_id,'0') as f_region_id","f_service_type",
        "f_program_id","f_start_play_time","f_update_time","f_playtoken","f_online_status","f_log_time","f_play_count")
//      saveOnlineUserDf.show(100,false)
      try{
        println("插入记录数：" + saveOnlineUserDf.count())
        DBUtils.saveDataFrameToPhoenixNew(saveOnlineUserDf, Tables.T_RUNLOG_USER_STATUS_REALTIME)
      }catch {
        case e:Exception => {
          println("保存数据失败!")
          e.printStackTrace()
        }
      }finally {
        nowTimeBC.destroy()
        gatherTypeBC.destroy()
        userRegionDf.unpersist()
        allLogDf.unpersist()
        sqlContext.clearCache()
      }
      println("在线用户列表处理结束："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
      /*println("在线用户数统计开始："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
      countUserAmt(sparkSession.sqlContext)
      println("在线用户数统计结束："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))*/

    })
  }

  private def trimSpace(string: String): String ={
    var dest = ""
    if (StringUtil.isNotEmpty(string)) {
      val p = Pattern.compile("\\s*|\t|\r|\n")
      val m = p.matcher(string)
      dest = m.replaceAll("")
    }
    dest
  }
  private def getUserInfo(data:String):User = {
    val datas = data.split(SPLIT)
    var userId = ""
    var deviceId = ""
    try{
       userId = datas(2)
       deviceId = datas(4)
    }catch{
      case ex: Exception => ex.printStackTrace()
    }
    new User(userId,deviceId)
  }

  /**
    * 获取默认区域码
    * @return
    */
  def getDefaultRegionId(): String = {
    var regionId = RegionUtils.getRootRegion
    if (regionId.endsWith("0000")) {
      regionId = (regionId.toInt + 101).toString
    } else {
      regionId = (regionId.toInt + 1).toString
    }
    println("默认区域 - " + regionId)
    regionId
  }
  /**
    * 从日志行中获取DF，获取run日志用户
    * @param lines
    * @param sqlContext
    * @return
    */
  def getDfFromLog(lines: RDD[String], sqlContext: SQLContext) = {
    import sqlContext.implicits._
    lines.filter(_.split("\\[INFO\\]").length == 2) // 修复角标越界问题
      .map(line => {
      val splits = line.split("\\[INFO\\]") //日志分成时间 和 内容
      val reportTime = splits(0).substring(splits(0).indexOf("]") + 1, splits(0).indexOf("]") + 24) //上报时间
      val logInfo = splits(1).replace("DeviceID", "DeviceId")
      val logKey = logInfo.substring(0, logInfo.indexOf(Constant.SPLIT)).replace("-", "").trim
      val logMap: scala.collection.Map[String, String] = LogUtils.str_to_map(logInfo.substring(logInfo.indexOf(Constant.SPLIT) + 1), ",", " ")
      val deviceType = logMap.getOrElse("DeviceType",null)
      val serviceType = logMap.getOrElse("ProgramMethod",null)
      val programId = logMap.getOrElse("ProgramID",null)
//      val playTime = if (logMap.getOrElse("Play",null)==null){0L}else{logMap.getOrElse("Play",null)}.toLong
      val userId = logMap(GatherConstant.userId)
      val playToken = logMap.getOrElse("PlayToken","unknown")
      VideoPlayLog(userId, logKey,reportTime, deviceType,serviceType,programId,playToken)
//      (userId,VideoPlayLog(userId, logKey,reportTime, deviceType,serviceType,programId,playTime,timeStamp))
    }).toDF()
  }

  /**
    * 计算各围度人数
    * @param sqlContext
    */
  def countUserAmt(sqlContext:HiveContext)={
    import sqlContext.implicits._
    //区域信息
    //项目部署地code(省或者地市)
    val regionCode = RegionUtils.getRootRegion
    val region =
      s"""
         |(select cast(a.area_id as char) as f_area_id,a.area_name as f_region_name,
         |a.city_id as f_city_id,c.city_name as f_city_name,
         |c.province_id as f_province_id,p.province_name as f_province_name
         |from (area a left join city c on a.city_id=c.city_id)
         |left join province p on c.province_id=p.province_id
         |where a.city_id='${regionCode}' or c.province_id='${regionCode}'
         |) as region
        """.stripMargin
    val regionDF = DBUtils.loadMysql(sqlContext, region, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    regionDF.cache()
    val onlineUserSql =
      s"""
        |(select f_user_id,f_terminal,f_region_id,f_service_type
        |from ${Tables.T_RUNLOG_USER_STATUS_REALTIME}
        |where f_online_status = 1) as online_user
      """.stripMargin
    val onlineUserDf = DBUtils.loadDataFromPhoenix2(sqlContext,onlineUserSql)
    val nowDateTime = DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMM)
    val lastDateTime = DateUtils.getYesterday(DateUtils.YYYY_MM_DD_HHMM)
    //分终端、分区域、分服务类别进行数量统计
    val saveOnlineUserDf = onlineUserDf.join(regionDF,onlineUserDf("f_region_id")===regionDF("f_area_id"))
      .groupBy("f_province_id","f_province_name","f_city_id","f_city_name","f_region_id","f_region_name",
        "f_terminal","f_service_type").count()
        .selectExpr(s"'${nowDateTime}' as f_date_time","f_terminal","f_province_id","f_province_name","f_city_id","f_city_name","f_region_id","f_region_name",
          "f_service_type","count as f_user_count")
    try{

      DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD,s"delete from ${Tables.T_USER_COUNT_REALTIME} where f_date_time < '${lastDateTime}'") //删除前一天数据
    }catch {
      case e:Exception => {
        println("删除历史在线用户数统计数据失败！")
        e.printStackTrace()
      }
    }
    try{
//      saveOnlineUserDf.show(100,false)
      println("插入记录数：" + saveOnlineUserDf.count())
      DBUtils.saveToMysql(saveOnlineUserDf, Tables.T_USER_COUNT_REALTIME, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)
    }catch {
      case e:Exception => {
        println("在线用户数统计失败！")
        e.printStackTrace()
      }
    }
  }
  /**
    * 获取区域信息
    */
  def getUserRegion(sqlContext: SQLContext): DataFrame = {
    val userUsersql =
      s"""
        |(select f_user_id,f_region_id from ${Tables.T_USER_REGION}) as user_region
      """.stripMargin
    DBUtils.loadDataFromPhoenix2(sqlContext,userUsersql)
      .selectExpr("f_user_id as r_user_id", "f_region_id as r_region_id")
  }

  def getherLogType(logKey:String,gatherType:String)={
    GatherConstant.RUN_LOG_GATHER_TYPE.contains("live")
  }

  /**
    * 获取业务类型码
    * @param serviceType
    * @return
    */
  def getServiceType(serviceType: String = "-1"): String = {
    serviceType.toLowerCase match {
      case "tr" => GatherConstant.LOOK_BACK
      case "ts" => GatherConstant.TIME_SHIFT
      case "kts" => GatherConstant.ONE_KEY_TIMESHIFT
      case "vod" => GatherConstant.DEMAND
      case "live" => GatherConstant.LIVE
      case _ => serviceType
    }
  }
}
