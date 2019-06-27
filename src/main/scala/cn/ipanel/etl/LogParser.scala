package cn.ipanel.etl


import cn.ipanel.common._
import cn.ipanel.customization.wuhu.etl.LogProcess
import cn.ipanel.utils.DateUtils._
import cn.ipanel.utils.{DBUtils, DateUtils, LogUtils, RegionUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.{ListBuffer, HashMap => mutableMap}


/**
  * run日志解析程序
  */
object LogParser {
  val clusterRegion = RegionUtils.getRootRegion //配置文件中所配置的地方项目区域码 added @20190421
  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      System.err.println(
        """
          |请输入有效日期,分区数,日志版本,日志源 例如【20180401】, [200] ,[true] [0]
          |--日期: 日期
          |--分区数:确定写入到hive中文件个数
          |--日志版本:用true表示新版本,false表示老版本
          |--日志源(取消):  0 采用用户上报,1 采用推流日志，2 混用
          |        当混用的时候 需求确定直播，点播，回看，时移  通过配置文件更改demand_report，look_report，live_report，timeshift_report
          |         orc_video_report  采集日志   orc_video_play_tmp 推流
          | 由于湖南验证要做两份日志作对比 所以
          | 默认为1  推流日志基础数据存储
          | 由LogDelay 数据源切换
        """.stripMargin)
      System.exit(-1)
    }

    val day = args(0)
    val partitions = args(1).toInt
    val logVersion = args(2).toBoolean //true 新版 false 老版
    val sourceType = args(3).toInt
    val logPathDate: String = DateUtils.transformDateStr(day, DateUtils.YYYYMMDD, DateUtils.YYYY_MM_DD)
    val session = new SparkSession("LogParser")
    val sparkContext = session.sparkContext
    val sqlContext = session.sqlContext

    //注册序列化类
    sparkContext.getConf.registerKryoClasses(Array(classOf[Log], classOf[UserBehavior], classOf[Label]))
    //注册udf
    sqlContext.udf.register("deviceType", deviceTypeToNum _)
    sqlContext.udf.register("playType", playTypeMapping _)

    val regionCode = getRegionCode
    val regionDF = getUserRegion(regionCode, sqlContext)

    val regionCodeBr: Broadcast[String] = sparkContext.broadcast(regionCode)
    val regionBr: Broadcast[DataFrame] = sparkContext.broadcast(regionDF)
    regionBr.value.registerTempTable("t_region")
    //先处理nginx日志
    //var columnDF = sqlContext.emptyDataFrame
    val colNames = Array("serviceId", "columnId")
    //为了简单起见，字段类型都为String
    val schema = StructType(colNames.map(fieldName => StructField(fieldName, StringType, true)))
    //主要是利用了spark.sparkContext.emptyRDD
    var columnDF = sqlContext.createDataFrame(session.sparkContext.emptyRDD[Row], schema)

    try {
      val channelAndProgramTuple = NginxLogParseForLabel.parseHiveNginxLog(session, day)
      columnDF = mapToDataFrame(channelAndProgramTuple.value, sqlContext)
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
    val textRDD = sparkContext.textFile(getInputPath(logPathDate))
    // val textRDD = sparkContext.textFile("file:///C:\\Users\\Administrator\\Desktop\\11111111111111111")
    val logDF = processRunLog(textRDD, day, sqlContext, logVersion, regionCodeBr, partitions)
    //只用推流
    processRunLogSource(logDF, sqlContext, day, columnDF)
    sparkContext.stop()


  }


  def processRunLogSource(logDF: DataFrame, sqlContext: HiveContext, day: String, columnDF: DataFrame) = {
    sqlContext.sql("use bigdata")
    logDF.registerTempTable("logDF")
    columnDF.registerTempTable("columnDF")
    val demandDF = sqlContext.sql(
      s"""
         |insert  overwrite  table  orc_video_play_tmp  partition(day='$day')
         |select t1.userId,t1.deviceId,t1.deviceType,t1.regionId,t1.playType,t1.serviceId,t1.startTime,t1.endTime,
         |t1.playTime,  str_to_map(concat_ws(":", "CL",t2.columnId )) as ext,
         | case when deviceType  in ('1','2') then '1' else deviceType end   device_type
         | from
         |logDF  t1 left join columnDF t2
         |on t1.serviceId = t2.serviceId
                """.stripMargin)

    logDF.show()
    columnDF.show()
  }


  def processRunLog(textRDD: RDD[String], day: String, sqlContext: HiveContext, logVersion: Boolean, regionCodeBr: Broadcast[String], partitions: Int) = {
    import sqlContext.implicits._
    if (logVersion) { //新版日志
      val linesRdd = textRDD.filter(log => newFilter(log)).filter(x => x.contains(LogConstant.userId))
      val cacheDF = transform(linesRdd, day, sqlContext).filter($"keyWord" !== "VideoPlayFinish")
        .persist(StorageLevel.DISK_ONLY)
      cacheDF.show()
      val videoPlayDF = cacheDF.filter($"keyWord" === LogConstant.videoSuccess || $"keyWord" === LogConstant.videoFinished || $"keyWord" === LogConstant.videoFailed)
      //处理播放日志
      //得到播放日志基础表
      val logDF = processVideoPlay(videoPlayDF, day, regionCodeBr.value, sqlContext, partitions, logVersion)
      val userBehaviorDF = cacheDF.except(videoPlayDF)
      processOtherUserLog(userBehaviorDF, day, regionCodeBr.value, sqlContext, partitions)
      videoPlayDF.unpersist()
      cacheDF.unpersist()
      logDF
    }
    else { //老版日志
      val linesRdd = textRDD.filter(log => oldFilter(log)).filter(x => x.contains(LogConstant.userId))
      val cacheDF = transform(linesRdd, day, sqlContext).persist(StorageLevel.DISK_ONLY)
      val videoPlayDF = cacheDF.filter($"keyWord" === "VideoPlayStartSuccess" || $"keyWord" === "VideoPlayFinish" ||
        $"keyWord" === "VideoPlayBreak" || $"keyWord" === "VideoPlayStartFailed")
        .persist(StorageLevel.DISK_ONLY)
      // 处理播放日志
      val logDF = processVideoPlay(videoPlayDF, day, regionCodeBr.value, sqlContext, partitions, logVersion)
      val userBehaviorDF = cacheDF.except(videoPlayDF)
      processOtherUserLog(userBehaviorDF, day, regionCodeBr.value, sqlContext, partitions)
      videoPlayDF.unpersist()
      cacheDF.unpersist()
      logDF
    }
  }

  def processReportLogSource(sqlContext: HiveContext, day: String) = {
    //    select t1.userId,t1.deviceId,t1.deviceType,t1.regionId,t1.playType,t1.serviceId,t1.startTime,t1.endTime,t1.playTime,
    //    str_to_map(concat_ws(":", "column_id", t1.columnId )) as ext
    sqlContext.sql("use bigdata")
    //点播取数据源 关联节目
    val demandDF = sqlContext.sql(
      s"""
         |insert overwrite  table orc_video_play_tmp  partition(day='$day')
         |select userId,deviceId,deviceType,
         |regionId,playType,serviceId,startTime,endTime,
         |playTime,str_to_map(concat_ws(":", "column_id", ext['CL'] )) as ext,
         |case when deviceType  in ('1','2') then '1' else deviceType end   device_type
         | from
         |orc_video_report
         |where day = '$day'
          """.stripMargin)
  }

  def processLogSource(logDF: DataFrame, sqlContext: HiveContext, day: String, columnDF: DataFrame) = {
    sqlContext.sql("use bigdata")
    logDF.registerTempTable("logDF")
    //点播取数据源 关联节目
    if (   LogConstant.DEMAND_REPORT   == "orc_video_report") {
      sqlContext.sql(
        s"""
           |insert overwrite  table orc_video_play_tmp  partition(day='$day')
           |select userId,deviceId,deviceType,regionId,playType,serviceId,startTime,endTime,
           |playTime,str_to_map(concat_ws(":", "column_id", ext['CL'] )) as ext,
           | case when deviceType  in ('1','2') then '1' else deviceType end   device_type
           | from
           |orc_video_report
           |where day = '$day'  and playtype = 'demand'
          """.stripMargin)
    } else {
      columnDF.registerTempTable("columnDF")
      val demandDF = sqlContext.sql(
        s"""
           |insert into  orc_video_play_tmp  partition(day='$day')
           |select t1.userId,t1.deviceId,t1.deviceType,t1.regionId,t1.playType,t1.serviceId,t1.startTime,t1.endTime,
           |t1.playTime,  str_to_map(concat_ws(":", "column_id",t2.columnId )) as ext,
           | case when deviceType  in ('1','2') then '1' else deviceType end   device_type
           | from
           |logDF  t1 left join columnDF t2
           |on t1.serviceId = t2.serviceId
           |where t1.playtype = 'demand'
                """.stripMargin)
    }
    val lookDF = sqlContext.sql(
      s"""
         |insert into  orc_video_play_tmp  partition(day='$day')
         |select userId,deviceId,deviceType,regionId,playType,serviceId,startTime,endTime,
         |playTime,  str_to_map(concat_ws(":", "column_id", '' )) as ext,
         | case when deviceType  in ('1','2') then '1' else deviceType end   device_type
         | from
         |${LogConstant.LOOK_REPORT}
         |where playtype = 'lookback'
         |and day = '$day'
          """.stripMargin)

    val liveDF = sqlContext.sql(
      s"""
         |insert into  orc_video_play_tmp  partition(day='$day')
         |select userId,deviceId,deviceType,regionId,playType,serviceId,startTime,endTime,
         |playTime,  str_to_map(concat_ws(":", "column_id", '' )) as ext,
         | case when deviceType  in ('1','2') then '1' else deviceType end   device_type
         | from
         |${LogConstant.LIVE_REPORT}
         |where playtype = 'live'
         |and day = '$day'
          """.stripMargin)
    val timeshiftDF = sqlContext.sql(
      s"""
         |insert into orc_video_play_tmp  partition(day='$day')
         |select userId,deviceId,deviceType,regionId,playType,serviceId,startTime,endTime,
         |playTime,  str_to_map(concat_ws(":", "column_id", '' )) as ext,
         |case when deviceType  in ('1','2') then '1' else deviceType end   device_type
         | from
         |${LogConstant.TIMESHIFT_REPORT}
         |where (playtype = 'timeshift'
         |or playtype = 'one-key-timeshift')
         |and day = '$day'
          """.stripMargin)
  }


  /**
    * 处理播放类型日志,合并开始\结束时间
    */
  def processVideoPlay(df: DataFrame, day: String, regionCode: String, sqlContext: HiveContext,
                       nums: Int, logVersion: Boolean): DataFrame = {
    import sqlContext.sql
    df.registerTempTable("t_video_play")
    val startTime = getStartTimeOfDay(day)
    val endTime = getEndTimeOfDay(day)
    val tmpVideoDF = sql( //从map中获取需要字段
      s"""select reportTime,userId,
         |logMap['DeviceId'] as deviceId,deviceType(logMap['DeviceType']) as deviceType,
         |logMap['ProgramMethod'] as playType, logMap['ProgramID'] as serviceId,
         |cast(logMap['PlayS'] as bigint )as playTime,
         |logMap['PlayToken'] as playToken,
         |logMap['ProtocolType'] as protocolType,
         |keyWord,
         |logMap['URI'] as URI
         |from t_video_play
       """.stripMargin)
    //    |reportTime         |userId  |deviceId  |deviceType|playType|serviceId|playTime|playToken        |protocolType|keyWord                    |URI                                                                                                                                                                                                                                                      |
    //    +-------------------+--------+----------+----------+--------+---------+--------+-----------------+------------+---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    //    |2019-03-12 00:42:31|50312573|1005360181|1         |demand  |100060109|2609    |31617GLIWMHBYJF10|http        |StatisticsVideoPlayFinished|/playurl?playtype=demand&protocol=http&verifycode=1005360181&accesstoken=R5C80EA1AU10901023K3BEC9435IE92EA8C0P22BDM2FFB57DV10405Z6679063183W1589A47E8AA8D32B&programid=100060109&PlayToken=31617GLIWMHBYJF10&payloadreloc=true&relocsrcsrv=192.168.35.130|
    //      +-------------------+--------+----------+----------+--------+---------+--------+-----------------+------------+---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    var videoPlayTimeDF = sqlContext.emptyDataFrame
    if (logVersion) {
      videoPlayTimeDF = calcVideoPlayTimeNew(tmpVideoDF, startTime, endTime, sqlContext)
    } else {
      import sqlContext.implicits._
      val _df = tmpVideoDF.sortWithinPartitions($"reportTime".asc).selectExpr(
        "reportTime", "userId", "deviceId", "deviceType", "playType(playType) as playType", "serviceId", "playTime",
        "protocolType", "keyWord", "URI")
      videoPlayTimeDF = calcVideoPlayTimOld(_df, startTime, endTime, sqlContext)
    }
    videoPlayTimeDF.registerTempTable("t_video_tmp")
    //定义常用字段
    val hiveFileds = List("deviceId", "deviceType", "playType", "serviceId", "startTime", "endTime", "playTime").mkString(",")
    val logDF = sql(//获取区域字段
      s"""select $hiveFileds ,t1.userId,if(t2.regionId is null,$regionCode,t2.regionId) as regionId,
         |case when deviceType  in ('1','2') then '1' else deviceType end   device_type,
         |'$day' as day
         |from t_video_tmp t1
         |left join t_region t2
         |on t1.userId=t2.userId
        """.stripMargin)
      .repartition(nums)
    logDF
  }


  /**
    * 计算播放时长(旧版日志)
    *
    * @return userId, deviceId,deviceType,playType,serviceId,startTime,endTime,playTime,regionId
    */
  def calcVideoPlayTimOld(data: DataFrame, startTime: String, endTime: String, sqLContext: SQLContext): DataFrame = {
    import sqLContext._
    /*
      1.错位是为了将开始,结束,开始->break 等日志归并
      2.根据播放URL来确定唯一次播放,并且获取播放的开始,结束时间和总的播放时长
     */
    data.registerTempTable("t_log")
    sql(
      s"""
         |select userId, deviceId,deviceType,playType,serviceId,
         |reportTime as startTime,
         |lead(reportTime) over (partition  by userId,deviceId,URI order by reportTime) endTime,
         |keyWord as  t1_keyWord,
         |lead(keyWord) over (partition by  userId,deviceId,URI order by reportTime) t2_keyWord,
         |lead(playTime) over (partition by userId,deviceId,URI order by reportTime ) playTime,
         |URI
         |from t_log
         """.stripMargin)
      .filter(
        s"""
           |(t1_keyWord  = 'VideoPlayStartSuccess' and t2_keyWord='VideoPlayFinish') or
           |(t1_keyWord  = 'VideoPlayStartSuccess' and t2_keyWord='VideoPlayBreak')
           |""".stripMargin)
      .registerTempTable("t_calc_tmp")

    val df = sql(
      """
        | select userId, deviceId,deviceType,playType,serviceId,
        | min(startTime)  as startTime,
        | max(endTime) AS endTime,
        | sum(playTime) as playTime
        | from t_calc_tmp
        | group by userId, deviceId,deviceType,playType,serviceId,URI
        |
      """.stripMargin)
    df
  }

  /**
    * 计算播放时长
    *
    * @return userId, deviceId,deviceType,playType,serviceId,startTime,endTime,playTime,regionId
    */
  def calcVideoPlayTimeNew(data: DataFrame, startTime: String, endTime: String, sqLContext: SQLContext): DataFrame = {
    import sqLContext._

    data.registerTempTable("t_log")
    val df1 = sql(
      s"""
         |select userId, deviceId,deviceType,playType,serviceId,
         |reportTime as startTime,
         |lead(reportTime) over (distribute  by userId,deviceId,URI sort by reportTime) endTime,
         |keyWord as  t1_keyWord,
         |lead(keyWord) over (distribute by  userId,deviceId,URI sort by reportTime) t2_keyWord,
         |lead(playTime) over (distribute by userId,deviceId,URI sort by reportTime) playTime
         |from t_log
         """.stripMargin)
    df1.filter(
      s"""
         |(t1_keyWord  = '${LogConstant.videoSuccess}' and t2_keyWord='${LogConstant.videoFinished}')
         |
       """.stripMargin)

  }


  def playTypeMapping(playType: String): String = {
    playType.toLowerCase match {
      case "tr" => GatherType.LOOK_BACK_NEW
      case "ts" => GatherType.TIME_SHIFT_NEW
      case "kts" => GatherType.one_key_timeshift
      case "vod" => GatherType.DEMAND_NEW
      case "live" => GatherType.LIVE_NEW
      case _ => playType
    }
  }

  /**
    * 将设备类型转成数字
    *
    * @param deviceType 设备类型
    * @return 设备类型对应的数字
    */
  def deviceTypeToNum(deviceType: String): String = {
    deviceType.toLowerCase match {
      case "0" => LogConstant.TERMINAL
      case "机顶盒" => "1"
      case "stb" => "1"
      case "smartcard" => "2"
      case "ca" => "2"
      case "mobile" => "3"
      case "pad" => "4"
      case "pc" => "5"
      case "unknown" => LogConstant.TERMINAL
      case "" => LogConstant.TERMINAL
      case _ => deviceType
    }
  }


  def transform(lines: RDD[String], day: String, sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    lines.filter(_.split("\\[INFO\\]").length == 2) // 修复角标越界问题
      .map(line => {
      val splits = line.split("\\[INFO\\]") //日志分成时间 和 内容

      val reportTime = getReportTime(splits(0))
      val logInfo = splits(1).replace("DeviceID", "DeviceId").replace("playtoken", "PlayToken")
      //统一日志中DeviceId字段
      val keyWord = getKeyWord(logInfo)
      //      var logMap: scala.collection.Map[String, String] = stringToMap()
      var logMap: scala.collection.Map[String, String] = LogUtils.str_to_map_new(logInfo.substring(logInfo.indexOf(Constant.SPLIT) + 1), ",", " ", SourceType.RUN_LOG)
      val userId = logMap(LogConstant.userId)
      val _logMap = logMap.-(LogConstant.userId)
      Log(keyWord, userId, reportTime, _logMap)
    }).filter(x => x.logMap.nonEmpty && x.keyWord.nonEmpty && x.reportTime.nonEmpty && x.userId.nonEmpty)
      .filter(x => DateUtils.validateTimeRange(x.reportTime, day))
      .toDF()
  }


  /**
    * 获取关键字 例如Search，Filer
    */
  def getKeyWord(log: String): String = {
    log.substring(0, log.indexOf(Constant.SPLIT)).replace("-", "").trim
  }

  /**
    * 获取上报时间
    *
    * @param timeStr [23036]2018-03-26 10:38:36:472 - 或者[23036]2018-03-26 10:38:36.472 -
    * @return yyyy-MM-dd hh:mm:ss
    */
  def getReportTime(timeStr: String): String = {
    timeStr.substring(timeStr.indexOf("]") + 1, timeStr.indexOf("]") + 20)
  }

  /**
    * 新版run日志过滤器
    */
  def newFilter(log: String): Boolean = {
    !(log.contains("BINARYC") || log.contains("HTTPC") || log.contains("PlayCount")
      || log.contains("HdfsDownload") || log.contains("CGuestMgr")
      || log.contains("StatisticsPlayTimesCount")
      || log.contains("StatisticsSimplyOnlinePlayingCount")
      || log.contains("ProgramExit")
      || log.contains("VideoPlayStartSuccess")
      || log.contains("VideoPlayStartFailed")
      || log.contains("VideoPlayBreak")
      || log.contains("music")
      || log.contains("AssetDownload")
      )
  }

  /**
    * 获取区域信息
    */
  def getUserRegion(regionCode: String, sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    var _regionCode = RegionUtils.getRootRegion
    var filter_region = ""
    if (_regionCode.endsWith("0000")) {
      filter_region = _regionCode.substring(0, 2)
    } else {
      filter_region = _regionCode.substring(0, 4)
    }
    // 分开查询
    val accountInfoSql = "(select  DA as f_user_id,home_id as f_home_id from account_info) as account"
    val addressInfoSql =
      s"""
         |(select home_id,
         |case when  length(cast(region_id as char)) >= 6  and SUBSTR(region_id,5,2) ='00' then  SUBSTR(region_id,1,6)+1
         |     when  length(cast(region_id as char)) >= 6  then SUBSTR(region_id,1,6)
         |     when region_id=0 then '$regionCode'
         |     else region_id end f_region_id
         |from address_info ) as address
                    """.stripMargin
    val accountDF = DBUtils.loadMysql(sqlContext, accountInfoSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val addressDF = DBUtils.loadMysql(sqlContext, addressInfoSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    var region = sqlContext.emptyDataFrame
    if (CluserProperties.REGION_VERSION.toInt.equals(0)) {
      region = accountDF.join(addressDF, accountDF("f_home_id") === addressDF("home_id"))
        .selectExpr("f_user_id as userId", "f_region_id as regionId")
    } else {
      region = accountDF.join(addressDF, accountDF("f_home_id") === addressDF("home_id"))
        .selectExpr("f_user_id as userId", "f_region_id as regionId")
        .filter($"regionId".startsWith(filter_region))
    }
    region
  }

  /**
    * 处理（除播放外的）其他用户日志
    */
  def processOtherUserLog(df: DataFrame, day: String, regionCode: String, sqlContext: SQLContext, nums: Int): Unit = {
    import sqlContext.implicits._
    import sqlContext.sql
    val tmpDf = df.mapPartitions(it => {
      val listBuffer = new ListBuffer[UserBehavior]()
      while (it.hasNext) {
        val log = it.next()
        val reportType = log.getAs[String]("keyWord")
        val userId = log.getAs[String]("userId").toString
        val reportTime = log.getAs[String]("reportTime")
        var map: scala.collection.Map[String, String] = log.getAs[scala.collection.Map[String, String]]("logMap")
        val deviceId = map.getOrElse("DeviceId", "0").toLong
        val deviceType = map.getOrElse(LogConstant.deviceType, "0")
        listBuffer.+=(UserBehavior(userId, deviceId, deviceType, reportType, reportTime, map))
      }
      listBuffer.iterator
    }).toDF()

    // .registerTempTable("t_user_behavior")
    val logDf = clusterRegion match {
      case "340200" => { //芜湖
        LogProcess.getEffBehaviorDf(day, tmpDf, sqlContext).registerTempTable("t_user_behavior")
      }
      case _ => tmpDf.registerTempTable("t_user_behavior")
    }
    sql( //关联区域字段
      s"""
         |select t1.userId,t1.deviceId,t1.deviceType,t1.reportType,t1.reportTime,t1.exts,
         |if(t2.regionId is null,$regionCode,t2.regionId ) regionId
         |from t_user_behavior t1
         |left join t_region t2
         |on t1.userId = t2.userId
           """.stripMargin)
      .repartition(nums)
      .registerTempTable("t_user_behavior_hive")

    sql(s"use ${Constant.HIVE_DB}")
    sql( //保存数据
      s"""
         |insert into  table ${Tables.ORC_USER_BEHAVIOR} partition(day='$day')
         |select userId,regionId,deviceId,deviceType,reportType,reportTime,exts,
         | '' as ca
         | from t_user_behavior_hive
         |""".stripMargin)
    println("ORC_USER_BEHAVIOR 表插入成功")
  }

  def mapToDataFrame(data: mutableMap[String, String], sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    data.map(x => {
      Label(x._1, x._2)
    }).toSeq.toDF()
  }

  /**
    * 老版run日志过滤器
    */
  def oldFilter(log: String): Boolean = {
    !(log.contains("BINARYC") || log.contains("HTTPC") || log.contains("PlayCount")
      || log.contains("HdfsDownload") || log.contains("CGuestMgr")
      || log.contains("StatisticsPlayTimesCount")
      || log.contains("StatisticsSimplyOnlinePlayingCount")
      || log.contains("ProgramExit")
      || log.contains("StatisticsVideoPlaySuccess")
      || log.contains("StatisticsVideoPlayFinished")
      || log.contains("StatisticsVideoPlayFailed")
      || log.contains("music")
      || log.contains("AssetDownload")
      )
  }

  /**
    * 获取区域码
    * 获取的是默认区域码，如果区域码为省级，则改为省|01|01
    * 如果区域码为市级，则改为省|市|01
    */
  def getRegionCode(): String = {
    var regionCode = RegionUtils.getRootRegion
    println("regionCode==" + regionCode)
    if (regionCode.endsWith("0000")) {
      regionCode = (regionCode.toInt + 101).toString
    } else {
      regionCode = (regionCode.toInt + 1).toString
    }
    regionCode
  }


  def getInputPath(date: String): String = {
    val ilogslavePath = LogConstant.LOG_PATH + "/" + date + "/*"
    val ilogvssPath = if (LogConstant.LOGVSS_PATH == "") "" else LogConstant.LOGVSS_PATH + "/" + date + "/*"

    if (ilogvssPath.isEmpty) {
      ilogslavePath
    } else {
      List(ilogslavePath, ilogvssPath).mkString(",").replace(" ", "")
    }
  }

  /**
    * 用户行为日志
    *
    * @param userId     用户id
    * @param deviceId   设备ID
    * @param deviceType 设备类型（终端类型）
    * @param reportType 上报类型（类似filter,search等类型）
    * @param reportTime 上报时间（日志时间）
    * @param exts       拓展信息
    */
  case class UserBehavior(userId: String, deviceId: Long, deviceType: String, reportType: String, reportTime: String, exts: scala.collection.Map[String, String])

  case class Log(keyWord: String, userId: String, reportTime: String, logMap: scala.collection.Map[String, String])

  case class Label(serviceId: String, columnId: String)


}
