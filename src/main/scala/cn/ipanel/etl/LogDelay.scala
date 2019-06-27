package cn.ipanel.etl

import cn.ipanel.common._
import cn.ipanel.customization.hunan.etl.HNLogParser
import cn.ipanel.customization.wuhu.etl.LogProcess
import cn.ipanel.utils.{DBUtils, DateUtils, RegionUtils, UserUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

import scala.util.Random


/**
  * 通过配置文件数据源切换
  * 关联栏目
  * 上报日志 CL字段
  * run  分为三步  1  nginx 2  查数据库 3  附默认值
  *   1.过滤serviceId为空记录
  *   2.以用户为唯一分组条件,错位比较上一条开始时间和下一条的开始时间
  *   3.存数据到orc_video_play
  */
object LogDelay {
  val clusterRegion = RegionUtils.getRootRegion //配置文件中所配置的地方项目区域码 added @20190421
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("LogDelay")
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    if (args.length != 2) {
      System.err.println("请输入有效日期,分区数")
      System.exit(-1)
    }
    val date = args(0)
    val num = args(1).toInt
    sqlContext.udf.register("random_column", randomColumn _)
    val afterday = DateUtils.getAfterDay(date, targetFormat = DateUtils.YYYY_MM_DD)
    //回看默认栏目
    val defaultLookbackColumnBr = sc.broadcast(getDefaultLookBackColumnId(sqlContext))
    //点播默认栏目
    val defaultdemandColumnBr = sc.broadcast(getDefaultDemandColumnId(sqlContext))
    //直播默认栏目
    val defaultliveColumn = sc.broadcast(getDefaultLiveColumnId(sqlContext))
    val _columnDF = processRelevanceColumn(date, sqlContext, defaultLookbackColumnBr, defaultdemandColumnBr, defaultliveColumn)
    //游客过滤
    //val validUser = UserUtils.getAllUsers(sqlContext, afterday)
    //_columnDF.join(validUser, _columnDF("userid") === validUser("user_id")).repartition(num).registerTempTable("column_log")
    _columnDF.registerTempTable("column_log")
    println("延迟处理开始")
    processDelayLog(date, sqlContext, num)
    println("完成")
    sc.stop()
  }


  def processDelayLog(day: String, sqlContext: HiveContext, partitions: Int) = {

    //如果是用户上报则需要导入301数据，用户过滤需要用到该数据作为依据
    if (CluserProperties.LOG_SOURCE.contains(SourceType.USER_REPORT)) {
      UserExport.process(day, day, hiveContext = sqlContext)
    }
    //开机有效用户处理程序
    UserAgregate.process(day,hiveContext=sqlContext)

    sqlContext.sql("use bigdata")
    val df1 = sqlContext.sql(
      s"""
         |select t.userid,t.deviceType,t.deviceid,t.regionid,
         |t.startTime,t.endTime1,
         |if((t.endTime>t.endTime1),t.endTime1,t.endTime) as endTime,
         |t.playTime,
         |t.columnid  as column_id,
         |t.playType,t.serviceid
         |from
         |(select *,
         |lead(startTime) over (partition  by userId,deviceId order by startTime) endTime1
         |from column_log) t
       """.stripMargin)
    //增加终端配置
    val df = getFilterLog(df1.distinct().coalesce(partitions), sqlContext)
    //地方定制化 - 播放日志
    val logDf = clusterRegion match {
      case Constant.WUHU_CODE => //芜湖
        LogProcess.getEffVidPlayDf(day, df.distinct().coalesce(partitions), sqlContext)

      case Constant.HUNAN_CODE =>
        HNLogParser.getValidateVideoPlay(day, df.distinct().coalesce(partitions), sqlContext)

      case Constant.JILIN_CODE =>
        HNLogParser.getValidateVideoPlay(day, df.distinct().coalesce(partitions), sqlContext)

      case _ => //默认
        df.distinct().coalesce(partitions)
    }
    //    "userid","deviceType","deviceid","regionid","startTime","endTime1","endTime","playTime","column_id","playType","serviceid"
    logDf.registerTempTable("t_tmp")
    sqlContext.sql(
      s"""
         |insert overwrite table ${Tables.ORC_VIDEO_PLAY} partition(day='$day')
         |select userid,deviceid,devicetype,
         |regionid,playtype,
         |nvl(cast(serviceid as string) ,'unknown') as serviceid,
         |starttime,endtime,
         |if((((unix_timestamp(endTime)-unix_timestamp(startTime)))>playtime),playtime,(unix_timestamp(endTime)-unix_timestamp(startTime))) as playtime,
         |str_to_map(concat_ws(":", "column_id", column_id)) as exts
         |from t_tmp
            """.stripMargin)
    println("写入数据成功")
  }

  def processRelevanceColumn(date: String, sqlContext: HiveContext,
                             defaultLookbackColumn: Broadcast[String],
                             defaultdemandColumn: Broadcast[String], defaultliveColumn: Broadcast[String]) = {
    val _columnDF = addColumn(date: String, sqlContext, defaultLookbackColumn, defaultdemandColumn, defaultliveColumn)
    _columnDF
  }

  private def addColumn(date: String, sqlContext: HiveContext,
                        defaultLookbackColumnBr: Broadcast[String],
                        defaultdemandColumnBr: Broadcast[String],
                        defaultliveColumn: Broadcast[String]) = {
    sqlContext.sql("use bigdata")
    //判断上报日志是否为空
    val sql =
      s"""
         |select * from orc_video_report where day =  '$date' limit 1
      """.stripMargin
    val sql1 =
      s"""
         |select * from orc_video_play_tmp where day =  '$date' limit 1
      """.stripMargin
    //run日志
    val rundf = sqlContext.sql(sql1)
    //上报日志
    val reportdf = sqlContext.sql(sql) //上报日志
   //判断上报日志和run日志是否为空
    if (rundf.count() == 0) {
      getLog(defaultLookbackColumnBr,
        defaultdemandColumnBr,
        defaultliveColumn, sqlContext, "orc_video_report", "orc_video_report", "orc_video_report", "orc_video_report", date)
    } else if (reportdf.count() == 0) {
      getLog(defaultLookbackColumnBr,
        defaultdemandColumnBr,
        defaultliveColumn, sqlContext, "orc_video_play_tmp", "orc_video_play_tmp", "orc_video_play_tmp", "orc_video_play_tmp", date)
    } else {
      //自定义
      getLog(defaultLookbackColumnBr, defaultdemandColumnBr,
        defaultliveColumn, sqlContext, LogConstant.DEMAND_REPORT, LogConstant.LOOK_REPORT, LogConstant.LIVE_REPORT, LogConstant.TIMESHIFT_REPORT, date)
    }


  }


  def getLog(defaultLookbackColumnBr: Broadcast[String],
             defaultdemandColumnBr: Broadcast[String],
             defaultliveColumn: Broadcast[String],
             sqlContext: HiveContext, damendName: String, lookbackName: String, liveName: String, timeshiftName: String, date: String) = {
    val program_seriesDF = getProgramSeriesMapping(sqlContext)
    program_seriesDF.persist(StorageLevel.MEMORY_ONLY)
    val series_columnDF = getSeriesColumnMapping(sqlContext)
    series_columnDF.persist(StorageLevel.MEMORY_ONLY).registerTempTable("t_series_column")

    //处理栏目分3块
    var demandDF = sqlContext.emptyDataFrame
    var lookbackDF = sqlContext.emptyDataFrame
    var liveDF = sqlContext.emptyDataFrame
    var timeshiftDF = sqlContext.emptyDataFrame
    val sql =
      s"""
         |select userId,deviceId,deviceType,playType,serviceId,startTime,endTime,playTime,regionId,
         | case when deviceType  in ('1','2') then '1' else deviceType end   device_type,
         |ext['CL'] as columnId
         |from
         |$damendName
         |where day = '$date' and playtype ='demand'
       """.stripMargin
    demandDF = sqlContext.sql(sql)

    val columnDF1 = dealDemandColumn(demandDF, program_seriesDF, sqlContext, defaultdemandColumnBr)
    //回看,如果没有关联到栏目,则给一个默认回看栏目ID
    lookbackDF =
      sqlContext.sql(
        s"""
           |select userId,deviceId,deviceType,playType,serviceId,startTime,endTime,playTime,regionId,
           |case when deviceType  in ('1','2') then '1' else deviceType end   device_type,
           |ext['CL'] as columnId
           |from
           |$lookbackName
           |where day = '$date' and playtype ='lookback'
               """.stripMargin)
    val columnDF2 = dealLookbackColumn(lookbackDF, program_seriesDF, sqlContext, defaultLookbackColumnBr)
    liveDF =
      sqlContext.sql(
        s"""
           |select userId,deviceId,deviceType,playType,serviceId,startTime,endTime,playTime,regionId,
                 case when deviceType  in ('1','2') then '1' else deviceType end   device_type,
           |ext['column_id'] as columnId
           |from
           |$liveName
           |where day = '$date' and (playtype ='live')
                   """.stripMargin)
    val columnDF3 = dealLiveTimeshiftColumn(liveDF, sqlContext, defaultliveColumn)
    timeshiftDF =
      sqlContext.sql(
        s"""
           |select userId,deviceId,deviceType,playType,serviceId,startTime,endTime,playTime,regionId,
                 case when deviceType  in ('1','2') then '1' else deviceType end   device_type,
           |ext['column_id'] as columnId
           |from
           |$timeshiftName
           |where day = '$date' and (playtype ='timeshift' or playtype ='one-key-timeshift')
                   """.stripMargin)

    val columnDF4 = dealLiveTimeshiftColumn(timeshiftDF, sqlContext, defaultliveColumn)


    program_seriesDF.unpersist()
    series_columnDF.unpersist()

    columnDF1.show(1)
    columnDF2.show(2)
    columnDF3.show(3)
    columnDF4.show(4)

    columnDF1.unionAll(columnDF2).unionAll(columnDF3).unionAll(columnDF4).
      repartition(columnDF3.rdd.getNumPartitions)
  }


  def getSeriesColumnMapping(hiveContext: HiveContext): DataFrame = {
    hiveContext.sql(
      """
        |select f_series_id,f_terminal as deviceType ,
        |random_column(collect_list(cast (f_column_id as string))) columnId,f_event_series_id
        |from  userprofile.t_column
        | group by f_series_id,f_terminal,f_event_series_id
      """.stripMargin)
  }

  /**
    * 处理直播时移的栏目
    *
    * @param live_timeshiftDF
    * @param hiveContext
    * @return
    */
  private def dealLiveTimeshiftColumn(live_timeshiftDF: DataFrame, hiveContext: HiveContext, defaultliveColumn: Broadcast[String]): DataFrame = {
    val defaultlive = defaultliveColumn.value
    live_timeshiftDF.registerTempTable("t_live_log")
    val live_columnDF = hiveContext.sql(
      """
        |select t1.userId,t1.deviceId,t1.deviceType,t1.playType,t1.serviceId,t1.startTime,t1.endTime,t1.playTime,t1.regionId,
        |if((t1.columnId != '' and t1.columnId  is not null),t1.columnId,t2.columnId)   columnId
        |from  t_live_log t1
        |left join  t_series_column t2
        |on  t1.device_type=t2.deviceType and t1.serviceId=t2.f_series_id
      """.stripMargin)
    val map = Map("columnId" -> defaultlive)
    live_columnDF.na.fill(map)
  }


  private def dealDemandColumn(demandDF: DataFrame, program_seriesDF: DataFrame, hiveContext: HiveContext,
                               defaultdemandColumnBr: Broadcast[String]) = {

    val demandColumnId = defaultdemandColumnBr.value
    demandDF.join(program_seriesDF, Seq("serviceId"), "left").registerTempTable("t_log_series")
    val demand_columnDF = hiveContext.sql(
      s"""
         |select t1.userId,t1.deviceId,t1.deviceType,t1.playType,t1.serviceId,t1.startTime,t1.endTime,t1.playTime,t1.regionId,
         |if((t1.columnId != '' and t1.columnId  is not null),t1.columnId,t2.columnId)   columnId
         |from  t_log_series t1
         |left join  t_series_column t2
         |on t1.device_type=t2.deviceType
         |and t1.f_series_id=t2.f_series_id
      """.stripMargin)
    val map = Map("columnId" -> demandColumnId)
    demand_columnDF.na.fill(map)
  }

  def getDefaultLookBackColumnId(hiveContext: HiveContext): String = {
    val sql =
      """
        |(SELECT  DISTINCT(f_column_id) ,f_column_name
        |from t_column_info where f_column_name like '%回看%'
        |and f_column_status = 2
        |ORDER BY f_parent_id desc  limit 1) a
      """.stripMargin
    val df = DBUtils.loadMysql(hiveContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    var columnId = "0"
    df.head(1).foreach(x => {
      columnId = x.getLong(0).toString
    })
    columnId
  }

  private def getDefaultLiveColumnId(hiveContext: HiveContext): String = {
    val sql =
      """
        |(SELECT  DISTINCT(f_column_id) ,f_column_name
        |from t_column_info where f_column_name like '%频道%'
        |and f_column_status = 2
        |ORDER BY f_parent_id asc  limit 1) a
      """.stripMargin
    val df = DBUtils.loadMysql(hiveContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    var columnId = "0"
    df.head(1).foreach(x => {
      columnId = x.getLong(0).toString
    })
    columnId
  }


  private def getDefaultDemandColumnId(hiveContext: HiveContext): String = {
    val sql =
      """
        |(SELECT  DISTINCT(f_column_id) ,f_column_name
        |from t_column_info where f_column_name like '%点播%'
        |and f_column_status = 2
        |ORDER BY f_parent_id asc  limit 1) a
      """.stripMargin
    val df = DBUtils.loadMysql(hiveContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    var columnId = "0"
    df.head(1).foreach(x => {
      columnId = x.getLong(0).toString
    })
    columnId
  }

  private def getProgramSeriesMapping(hiveContext: HiveContext): DataFrame = {
    val demandSql = "( SELECT  video_id as serviceId,f_series_id FROM video_info ) as vod_info "
    val demandDF = DBUtils.loadMysql(hiveContext, demandSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val lookbackSql = "(SELECT event_id as serviceId,f_series_id FROM homed_eit_schedule_history  ) as vod_info"
    val lookbackDF = DBUtils.loadMysql(hiveContext, lookbackSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    demandDF.unionAll(lookbackDF)
  }


  private def randomColumn(list: Seq[String]): String = {
    var columnId = ""
    if (!list.isEmpty) {
      val random = new Random().nextInt(list.length)
      columnId = list.apply(random)
    }
    columnId
  }


  private def dealLookbackColumn(lookbackDF: DataFrame, program_seriesDF: DataFrame,
                                 hiveContext: HiveContext, defaultLookbackColumnBr: Broadcast[String]): DataFrame = {
    val defaultlookbackColumnId = defaultLookbackColumnBr.value
    lookbackDF.join(program_seriesDF, Seq("serviceId"), "left").registerTempTable("t_log_series")
    val lookback_columnDF = hiveContext.sql(
      s"""
         |select t1.userId,t1.deviceId,t1.deviceType,t1.playType,t1.serviceId,t1.startTime,t1.endTime,t1.playTime,t1.regionId,
         |if((t1.columnId != '' and t1.columnId  is not null),t1.columnId,t2.columnId)   columnId
         |from  t_log_series t1
         |left join  t_series_column t2
         |on t1.f_series_id=t2.f_event_series_id
         |and t1.device_type=t2.deviceType
      """.stripMargin)
    // val map = Map("列名1“　-> 指定数字, "列名2“　-> 指定数字, .....)
    // dataframe.na.fill(map)
    val map = Map("columnId" -> defaultlookbackColumnId)
    lookback_columnDF.na.fill(map)
  }

  def getFilterLog(df: DataFrame, sqlContext: HiveContext) = {
    val log = if (LogConstant.STATISTICS_TYPE == StatisticsType.STB) {
      df.filter(df.col("devicetype") === "1" || df.col("devicetype") === "2")
    }
    else if (LogConstant.STATISTICS_TYPE == StatisticsType.MOBILE) {
      {
        df.filter(df.col("devicetype") === "3" || df.col("devicetype") === "4" || df.col("devicetype") === "5")
      }
    }
    else {
      df
    }
    log
  }
}
