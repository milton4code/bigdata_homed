package cn.ipanel.etl


import cn.ipanel.common._
import cn.ipanel.utils.DateUtils._
import cn.ipanel.utils.{DBUtils, DateUtils, LogUtils, RegionUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.{ListBuffer, HashMap => mutableMap}
import scala.util.Random

/**
  * run日志解析程序
  * 传入4个参数 分别是[日期][分区数] [日志版本] [日志源]
  * 其中,日志版本用true表示新版本,false表示老版本
  * 日志源 用 0 表示采用推流日志,1表示采用用户上报,2表示混用
  * created by liujjy at 2018/03/30 13:03
  */
object RunLogParser {

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      System.err.println("请输入有效日期,分区数,日志版本,日志源 例如【20180401】, [200] ,[true] [0]")
      System.exit(-1)
    }

    val day = args(0)
    val partitions = args(1).toInt
    val logVersion = args(2).toBoolean //true 新版 false 老版
    val logPathDate: String = DateUtils.transformDateStr(day, DateUtils.YYYYMMDD, DateUtils.YYYY_MM_DD)
    val sourceType = args(3).toInt

    val session = new SparkSession("RunLogParser")
    val sparkContext = session.sparkContext
    val sqlContext = session.sqlContext
    import sqlContext.implicits._

    //注册序列化类
    sparkContext.getConf.registerKryoClasses(Array(classOf[Log], classOf[UserBehavior], classOf[Label]))
    //注册udf
    sqlContext.udf.register("deviceType", deviceTypeToNum _)
    sqlContext.udf.register("playType", playTypeMapping _)
    sqlContext.udf.register("random_column", randomColumn _)


    val regionCode = getRegionCode
    val regionDF = getUserRegion(regionCode, sqlContext)

    val regionCodeBr: Broadcast[String] = sparkContext.broadcast(regionCode)
    val regionBr: Broadcast[DataFrame] = sparkContext.broadcast(regionDF)
    regionBr.value.registerTempTable("t_region")
    //默认回看栏目id
    val defaultLookbackColumnBr = sparkContext.broadcast(getDefaultLookBackColumnId(sqlContext))

    //先处理nginx日志
    var columnDF = sqlContext.emptyDataFrame
    try {
      val channelAndProgramTuple = NginxLogParseForLabel.parseHiveNginxLog(session, day)
      columnDF = mapToDataFrame(channelAndProgramTuple.value, sqlContext)
    } catch {
      case ex: Exception => ex.printStackTrace()
    }

    val textRDD = sparkContext.textFile(getInputPath(logPathDate)).filter(x => filterLiveLog(x, sourceType))

    val series_columnDF = getSeriesColumnMapping(sqlContext)
    series_columnDF.persist().registerTempTable("t_series_column")

    // 处理ilogslave日志
    if (logVersion) { //新版日志
      val linesRdd = textRDD.filter(log => newFilter(log)).filter(x => x.contains(LogConstant.userId))

      val cacheDF = transform(linesRdd, day, sqlContext).filter($"keyWord" !== "VideoPlayFinish").persist(StorageLevel.DISK_ONLY)
      val videoPlayDF = cacheDF.filter($"keyWord" === LogConstant.videoSuccess || $"keyWord" === LogConstant.videoFinished || $"keyWord" === LogConstant.videoFailed)
      //处理播放日志
      processVideoPlay(videoPlayDF, day, regionCodeBr.value, sqlContext, columnDF, partitions, logVersion,defaultLookbackColumnBr)

      //处理其他日志
      val userBehaviorDF = cacheDF.except(videoPlayDF)
      processOtherUserLog(userBehaviorDF, day, regionCodeBr.value, sqlContext, partitions)

      videoPlayDF.unpersist()
      cacheDF.unpersist()

    } else { //老版日志
      val linesRdd = textRDD.filter(log => oldFilter(log)).filter(x => x.contains(LogConstant.userId))

      val cacheDF = transform(linesRdd, day, sqlContext).persist(StorageLevel.DISK_ONLY)
      val videoPlayDF = cacheDF.filter($"keyWord" === "VideoPlayStartSuccess" || $"keyWord" === "VideoPlayFinish" ||
        $"keyWord" === "VideoPlayBreak" || $"keyWord" ==="VideoPlayStartFailed")
        .persist(StorageLevel.DISK_ONLY)

      //处理播放日志
      processVideoPlay(videoPlayDF, day, regionCodeBr.value, sqlContext, columnDF, partitions, logVersion,defaultLookbackColumnBr)

      //处理其他日志
      val userBehaviorDF = cacheDF.except(videoPlayDF)
      processOtherUserLog(userBehaviorDF, day, regionCodeBr.value, sqlContext, partitions)

      videoPlayDF.unpersist()
      cacheDF.unpersist()
    }

    //只有用户上报或者混合使用使用 ,直播都需要采用上报日志
    if (sourceType != GatherType.PUSH_LOG) {
      saveUserReportLiveDataToVideoPlay(day, sqlContext, partitions)
    }
    series_columnDF.unpersist()
    LogDelayPatch.processDelayLog(day,sqlContext,partitions )
    sparkContext.stop()

  }

  /**
    *将用户上报直播数据保存到ORC_VIDEO_PLAY_TMP表中
    */
  private def saveUserReportLiveDataToVideoPlay(day: String, sqlContext: HiveContext, partionNums: Int): Unit = {
    import sqlContext._
    sql("use bigdata")
    val liveDF = sql(
      s"""
         |SELECT userid,deviceId,deviceType,regionId, 'live' as playtype ,paras['ID'] as serviceId,
         |starttime,endTime,playTime,
         |case when deviceType  in ('1','2') then '1' else deviceType end   device_type
         |from ${Tables.ORC_USER_REPORT}
          WHERE day=${day} and  servicetype='${GatherType.LIVE}' and paras['ID'] is not  null
         | """
        .stripMargin)

    val df = dealLiveTimeshiftColumn(liveDF, sqlContext).repartition(partionNums)
    df.registerTempTable("t_tmp_saved")

    sql(
      s"""
         |insert into table ${Tables.ORC_VIDEO_PLAY_TMP} partition(day='$day')
         |select t1.userId,t1.deviceId,t1.deviceType,t1.regionId,t1.playType,t1.serviceId,t1.startTime,t1.endTime,t1.playTime,
         |str_to_map(concat_ws(":", "column_id", t1.columnId )) as ext
         |from t_tmp_saved t1
    """.stripMargin)
  }

  /**
    * 处理播放类型日志,合并开始\结束时间
    */
  def processVideoPlay(df: DataFrame, day: String, regionCode: String, sqlContext: HiveContext,
                       columnDF: DataFrame, nums: Int, logVersion: Boolean,defaultLookbackColumnBr:Broadcast[String]): Unit = {
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
    tmpVideoDF.persist(StorageLevel.DISK_ONLY)
    tmpVideoDF.count()

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
         |case when deviceType  in ('1','2') then '1' else deviceType end   device_type
         |from t_video_tmp t1
         |left join t_region t2
         |on t1.userId=t2.userId
        """.stripMargin)
      .repartition(nums)

    saveVideoPlayToHive(columnDF, logDF, day, sqlContext,nums,defaultLookbackColumnBr)

    tmpVideoDF.unpersist()
  }

  /**
    * 关联栏目
    *
    * @param columnDF
    * @param logDF
    * @param sqlContext
    * @return
    */
  private def addColumn(columnDF: DataFrame, logDF: DataFrame, sqlContext: HiveContext,
                        defaultLookbackColumnBr:Broadcast[String]): DataFrame = {
    val program_seriesDF = getProgramSeriesMapping(sqlContext)
    program_seriesDF.persist(StorageLevel.MEMORY_ONLY)

    val series_columnDF = getSeriesColumnMapping(sqlContext)
    series_columnDF.persist(StorageLevel.MEMORY_ONLY).registerTempTable("t_series_column")

    //处理栏目分3块
    //1.点播,先和nginx日志退测栏目进行关联,如果关联不上则直接查数据
    val demandDF = logDF.filter(s"playType ='${GatherType.DEMAND_NEW}'")
    val columnDF1 = dealDemandColumn(columnDF, demandDF, program_seriesDF, sqlContext)
    //2.回看,如果没有关联到栏目,则给一个默认回看栏目ID
    val lookbackDF = logDF.filter(s"playType='${GatherType.LOOK_BACK_NEW}' ")
    val columnDF2 = dealLookbackColumn(lookbackDF, program_seriesDF, sqlContext,defaultLookbackColumnBr)

    // 3.直播,时移为一类
    val live_timeshiftDF = logDF.filter(s" playType  in ('${GatherType.LIVE_NEW}','${GatherType.TIME_SHIFT_NEW}') ")
    val columnDF3 = dealLiveTimeshiftColumn(live_timeshiftDF, sqlContext)

    program_seriesDF.unpersist()

    columnDF1.unionAll(columnDF2).unionAll(columnDF3) .repartition (columnDF3.rdd.getNumPartitions)
  }

  /**
    * 处理直播时移的栏目
    *
    * @param columnDF
    * @param live_timeshiftDF
    * @param hiveContext
    * @return
    */
  private def dealLiveTimeshiftColumn(live_timeshiftDF: DataFrame, hiveContext: HiveContext): DataFrame = {

    live_timeshiftDF.registerTempTable("t_live_log")
    hiveContext.sql(
      """
        |select t1.userId,t1.deviceId,t1.deviceType,t1.playType,t1.serviceId,t1.startTime,t1.endTime,t1.playTime,t1.regionId,
        |t2.columnId
        |from  t_live_log t1
        |left join  t_series_column t2
        |on  t1.device_type=t2.deviceType and t1.serviceId=t2.f_series_id
      """.stripMargin)
  }



  /**
    * 处理回看栏目
    *
    * @param lookbackDF
    * @param program_seriesDF
    * @param hiveContext
    * @return
    */
  private def dealLookbackColumn(lookbackDF: DataFrame, program_seriesDF: DataFrame,
                                 hiveContext: HiveContext,defaultLookbackColumnBr:Broadcast[String]): DataFrame = {
    val defaultColumnId = defaultLookbackColumnBr.value
    //serviceId| deviceId| deviceType| playType| startTime| endTime| playTime| userId| regionId| columnId
    lookbackDF.join(program_seriesDF, Seq("serviceId"), "left").registerTempTable("t_log_series")
    hiveContext.sql(
      s"""
        |select t1.userId,t1.deviceId,t1.deviceType,t1.playType,t1.serviceId,t1.startTime,t1.endTime,t1.playTime,t1.regionId,
        |nvl(t2.columnId,$defaultColumnId) columnId
        |from  t_log_series t1
        |left join  t_series_column t2
        |on t1.f_series_id=t2.f_event_series_id and t1.device_type=t2.deviceType
      """.stripMargin)
  }

  /**
    * 处理点播
    *
    * @param coloumnDF
    * @param demandDF
    * @param hiveContext
    * @return
    */
  private def dealDemandColumn(columnDF: DataFrame, demandDF: DataFrame, program_seriesDF: DataFrame, hiveContext: HiveContext): DataFrame = {
    demandDF.registerTempTable("t_lookback_demand")
    val tmp_look_demandDF = demandDF.join(columnDF, Seq("serviceId"), "left")
    //serviceId| deviceId| deviceType| playType| startTime| endTime| playTime| userId| regionId| columnId
    val log_empty_columnDF = tmp_look_demandDF.filter("columnId is null")


    log_empty_columnDF.join(program_seriesDF, Seq("serviceId"), "left").registerTempTable("t_log_series")

    val _empty_columnDF = hiveContext.sql(
      """
        |select t1.userId,t1.deviceId,t1.deviceType,t1.playType,t1.serviceId,t1.startTime,t1.endTime,t1.playTime,t1.regionId,
        |t2.columnId
        |from  t_log_series t1
        |left join  t_series_column t2
        |on t1.device_type=t2.deviceType and t1.f_series_id=t2.f_series_id
      """.stripMargin)

    val log_notempty_columnDF = tmp_look_demandDF.filter("columnId is not null")
      .selectExpr("userId", "deviceId", "deviceType", "playType", "serviceId", "startTime", "endTime", "playTime"
        , "regionId", "columnId")

    log_notempty_columnDF.unionAll(_empty_columnDF)
      .selectExpr("userId", "deviceId", "deviceType", "playType", "serviceId", "startTime", "endTime", "playTime"
        , "regionId", "columnId")
  }

  /**
    * 获取默认回看栏目ID
    * @param hiveContext
    * @return
    */
  private def getDefaultLookBackColumnId(hiveContext: HiveContext):String={
   val sql =
     """
       |(SELECT  DISTINCT(f_column_id) ,f_column_name
       |from t_column_info where f_column_name like '%回看%'
       |ORDER BY f_parent_id asc  limit 1) a
     """.stripMargin
    val df = DBUtils.loadMysql(hiveContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    var columnId = ""
    df.head(1).foreach(x=>{ columnId = x.getLong(0).toString})
    columnId
  }

  /**
    * 获取剧集和栏目映射
    *
    * @param hiveContext
    * @return f_series_id|deviceType|columnId
    */
  def getSeriesColumnMapping(hiveContext: HiveContext): DataFrame = {
    hiveContext.sql(
      """
        |select f_series_id,f_terminal as deviceType ,
        |random_column(collect_list(cast (f_column_id as string))) columnId,f_event_series_id
        |from  userprofile.t_column
        | group by f_series_id,f_terminal,f_event_series_id
      """.stripMargin)
  }

  //获取随机栏目
  private def randomColumn(list: Seq[String]): String = {
    var columnId = ""
    if (!list.isEmpty) {
      val random = new Random().nextInt(list.length)
      columnId = list.apply(random)
    }
    columnId
  }

  /**
    * 点播,回看单集和剧集映射关系
    *
    * @return serviceId|f_series_id
    */
  private def getProgramSeriesMapping(hiveContext: HiveContext): DataFrame = {
    val demandSql = "( SELECT  video_id as serviceId,f_series_id FROM video_info ) as vod_info "
    val demandDF = DBUtils.loadMysql(hiveContext, demandSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val lookbackSql = "(SELECT event_id as serviceId,f_series_id FROM homed_eit_schedule_history  ) as vod_info"
    val lookbackDF = DBUtils.loadMysql(hiveContext, lookbackSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    demandDF.unionAll(lookbackDF)
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

  /**
    * 保存播放类型日志到hive中
    */
  def saveVideoPlayToHive(columnDF: DataFrame, logDF: DataFrame, day: String,
                          sqlContext: HiveContext,nums:Int,defaultLookbackColumnBr:Broadcast[String]): Unit = {
    sqlContext.sql(s"use ${Constant.HIVE_DB}")

//    if (columnDF.rdd.isEmpty()) {
//      logDF.registerTempTable("t_video_hive")
//      //栏目为空,则 将栏目id赋值为null
//      sqlContext.sql(
//        s"""
//           |insert into table ${Tables.ORC_VIDEO_PLAY_TMP} partition(day='$day')
//           |select t1.userId,t1.deviceId,t1.deviceType,t1.regionId,t1.playType,t1.serviceId,t1.startTime,t1.endTime,t1.playTime,
//           |str_to_map(concat_ws(":", "column_id", null)) as ext
//           |from t_video_hive t1
//       """.stripMargin)
//    } else {
      //关联栏目
      val _columnDF = addColumn(columnDF, logDF, sqlContext,defaultLookbackColumnBr)
//
      _columnDF.repartition(nums).registerTempTable("t_column")
      sqlContext.sql(
        s"""
           |insert into table ${Tables.ORC_VIDEO_PLAY_TMP} partition(day='$day')
           |select t1.userId,t1.deviceId,t1.deviceType,t1.regionId,t1.playType,t1.serviceId,t1.startTime,t1.endTime,t1.playTime,
           |str_to_map(concat_ws(":", "column_id", t1.columnId )) as ext
           |from t_column t1
       """.stripMargin)
//    }
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
      case "0" => "0"
      case "机顶盒" => "1"
      case "stb" => "1"
      case "smartcard" => "2"
      case "ca" => "2"
      case "mobile" => "3"
      case "pad" => "4"
      case "pc" => "5"
      case "unknown" => "0"
      case "" => "0"
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
      var logMap: scala.collection.Map[String, String] = LogUtils.str_to_map(logInfo.substring(logInfo.indexOf(Constant.SPLIT) + 1), ",", " ")
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
      || log.contains("ProgramEnter")
      || log.contains("ProgramExit")
      || log.contains("VideoPlayStartSuccess")
      || log.contains("VideoPlayStartFailed")
      || log.contains("VideoPlayBreak")
      || log.contains("music")
      )
  }

  /**
    * 获取区域信息
    */
  def getUserRegion(regionCode: String, sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    // 分开查询
    val accountInfoSql = "(select  DA as f_user_id,home_id as f_home_id from account_info) as account"
    val addressInfoSql =
      s"""
         |(select home_id,
         |case when  length(cast(region_id as char)) >= 6  then cast(region_id as char)
         |     when length(cast(region_id as char)) = 6  and SUBSTR(region_id,5) ='00'then  region_id+1
         |    else region_id end f_region_id
         |from address_info ) as address
                    """.stripMargin
    val accountDF = DBUtils.loadMysql(sqlContext, accountInfoSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val addressDF = DBUtils.loadMysql(sqlContext, addressInfoSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)

    var _regionCode = RegionUtils.getRootRegion
    var filter_region = ""
    if (_regionCode.endsWith("0000")) {
      filter_region = _regionCode.substring(0, 2)
    } else {
      filter_region = _regionCode.substring(0, 4)
    }
    val region = accountDF.join(addressDF, accountDF("f_home_id") === addressDF("home_id"))
      .selectExpr("f_user_id as userId", "f_region_id as regionId")
      .filter($"regionId".startsWith(filter_region))
    region
  }

  /**
    * 处理（除播放外的）其他用户日志
    */
  def processOtherUserLog(df: DataFrame, day: String, regionCode: String, sqlContext: SQLContext, nums: Int): Unit = {
    import sqlContext.implicits._
    import sqlContext.sql
    df.mapPartitions(it => {
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
    }).toDF().registerTempTable("t_user_behavior")

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
         |insert overwrite  table ${Tables.ORC_USER_BEHAVIOR} partition(day='$day')
         |select userId,regionId,deviceId,deviceType,reportType,reportTime,exts from t_user_behavior_hive
         |""".stripMargin)
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
      || log.contains("ProgramEnter")
      || log.contains("ProgramExit")
      || log.contains("StatisticsVideoPlaySuccess")
      || log.contains("StatisticsVideoPlayFinished")
      || log.contains("StatisticsVideoPlayFailed")
      || log.contains("music")
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

  /**
    * 根据日志源类型过滤直播日志
    *
    * @param x
    * @param sourceType 0 表示推流 1表示用户上报 2表示混用
    * @return
    */
  def filterLiveLog(x: String, sourceType: Int): Boolean = {
    if (GatherType.REPORT_LOG == sourceType) { //只有用户上报才过滤直播日志
      !x.contains(GatherType.LIVE_NEW)
    } else {
      true
    }
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
