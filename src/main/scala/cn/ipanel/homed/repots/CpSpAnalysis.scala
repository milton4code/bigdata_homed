package cn.ipanel.homed.repots

import cn.ipanel.common.{SparkSession, Tables}
import cn.ipanel.homed.realtime.ProgramDemand.getDemandVideoDf
import cn.ipanel.homed.repots.DemandReport._
import cn.ipanel.utils.{DBUtils, DateUtils, RegionUtils, UserDeviceUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
  * 内容提供商相关统计程序
  */
object CpSpAnalysis {
  var partitionAmt = 128
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("CpSpAnalysis")
    val sc: SparkContext = sparkSession.sparkContext
    val hiveContext: HiveContext = sparkSession.sqlContext
    var date = DateUtils.getYesterday()
    if (args.length != 2) {
      System.err.println("请输入正确参数：统计日期,分区数,访问时间阈值， 例如【20190510】, [50]")
      System.exit(-1)
    }
    if (args.length == 1) {
      date = DateUtils.dateStrToDateTime(args(0), DateUtils.YYYYMMDD).toString(DateUtils.YYYYMMDD)
    } else if (args.length == 2) {
      date = DateUtils.dateStrToDateTime(args(0), DateUtils.YYYYMMDD).toString(DateUtils.YYYYMMDD)
      partitionAmt = args(1).toInt
    }
    hiveContext.sql("use bigdata")
    println("按天cp sp分析统计开始：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    processCpSpCount(hiveContext,date,"DAY")
    println("按天cp sp分析统计结束：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))

    println("按7天cp sp分析统计开始：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    processCpSpCount(hiveContext,date,"7DAYS")
    println("按7天cp sp分析统计结束：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))

    println("按周cp sp分析统计开始：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    processCpSpCount(hiveContext,date,"WEEK")
    println("按周cp sp分析统计结束：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))

    println("按月cp sp分析统计开始：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    processCpSpCount(hiveContext,date,"MONTH")
    println("按月cp sp分析统计结束：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))

  }
  def processCpSpCount(sqlContext:HiveContext,endDate:String,countType:String)={
    import sqlContext.implicits._
    val deviceNumDf = UserDeviceUtils.getDeviceNumDf(sqlContext)//.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val startDate = BusinessVisitCount.getStartDateByType(countType,endDate)
    val demandPkSql =
    /*s"""
       |select cast(f_terminal as int) as f_terminal,f_region_id,f_region_name,cast(f_province_id as string) as f_province_id,f_province_name,cast(f_city_id as string) as f_city_id,f_city_name,
       |if(f_cp_sp is null or f_cp_sp ='','unknown',f_cp_sp) as f_cp_sp,f_package_id,f_package_name,f_user_id,f_series_id,
       |f_series_name,if(f_video_id is null or f_video_id ='','-1',f_video_id) as f_video_id,if(f_video_name is null or f_video_name ='','unknown',f_video_name) as f_video_name,
       |f_play_time,f_count,day as date,
       |'${startDate}' as f_date,nvl(f_device_id,-1) as f_device_id
       |from userprofile.${Tables.orc_user_package} where f_user_id is not null and f_user_id<>'' and day>=${startDate} and day<=${endDate}
     """.stripMargin*/
      s"""
         |select cast(f_terminal as int) as f_terminal,cast(f_region_id as string) as f_region_id,f_region_name,cast(f_province_id as string) as f_province_id,f_province_name,cast(f_city_id as string) as f_city_id,f_city_name,
         |if(f_cp_id is null or f_cp_id ='','unknown',f_cp_id) as f_cp_sp,f_user_id,cast(f_series_id as string) as f_series_id,
         |f_series_name,if(f_video_id is null or f_video_id ='','-1',f_video_id) as f_video_id,if(f_video_name is null or f_video_name ='','unknown',f_video_name) as f_video_name,
         |f_play_time,cast(1 as bigint) as f_count,day as date,'${startDate}' as f_date,nvl(f_device_id,-1) as f_device_id
         |from userprofile.t_demand_video_basic where f_user_id is not null and f_user_id<>'' and day>=${startDate} and day<=${endDate}
    """.stripMargin
    val demandVideoBaseDf = sqlContext.sql(demandPkSql)//.persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("demandVideoBaseDf cnt:" + demandVideoBaseDf.count())
    val baseDf = demandVideoBaseDf.join(deviceNumDf,demandVideoBaseDf("f_device_id")===deviceNumDf("device_id"),"left_outer").drop("device_id").repartition(partitionAmt)
    //    baseDf.printSchema()
    println("--cp sp排行统计：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    processRank(baseDf,sqlContext,startDate,countType) //cp sp排行统计/点播量、人数等
    println("--cp sp用户播放详情统计：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    processUserPlay(baseDf,sqlContext,startDate,countType) //cp sp用户播放详情统计.
    println("--cp sp节目播放详情统计：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    processVideoPlay(baseDf,sqlContext,startDate,countType) //cp sp节目播放详情统计
    //    processUserType(baseDf,sqlContext,startDate,countType) //cp sp用户类型统计
//    deviceNumDf.unpersist()
//    demandVideoBaseDf.unpersist()
  }

  /**
    *
    * @param date
    * @param hiveContext
    * @return
    */
  def processHourDemandVideoBase(date:String,hiveContext:HiveContext)={
    val regionCode = RegionUtils.getRootRegion
    val regionInfo = Lookback.getRegionInfo(hiveContext, regionCode)
    //点播
    val demandMeiZiInfoDF = DemandReport.getDemandMeiZi(hiveContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    // 栏目
    val columnMap = DemandReport.getColumnInfo(hiveContext)
    // 得到基本数据信息  以半小时为单位 读取日志
    val user_df_basic1 = DemandReport.getDemandBasicInfo(date, hiveContext, regionInfo)
    val userBehaviour = DemandReport.getDemandBehaviour(date, hiveContext)
    val multiScreen = DemandReport.getMultiScreen(hiveContext, date)
    val basicDemand = DemandReport.getBasicDF(date, hiveContext, demandMeiZiInfoDF, user_df_basic1, columnMap, userBehaviour, multiScreen)
    basicDemand.selectExpr("f_hour","f_province_id","f_province_name","f_city_id","f_city_name","f_region_id","f_region_name","f_terminal","f_cp_id as f_cp_sp"
    ,"f_play_time","f_user_id")
      .repartition(partitionAmt)
    /*f_hour,f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name,f_terminal,f_cp_sp,
    sum(f_play_time) as f_play_time,sum(f_count) as f_play_count,count(distinct f_user_id) as f_user_count,
    (case when sum(f_play_time)<=600 then 1 when (sum(f_play_time)>600 and sum(f_play_time)<=3600) then 2 else 3 end) as f_user_type,*/
  }

  /**
    *
    * @param demandVideoBaseDf
    * @param sqlContext
    * @param startDate
    * @param countType
    */
  def processRank(demandVideoBaseDf:DataFrame,sqlContext:HiveContext,startDate:String,countType:String)={
    //按天统计时需要处理小时段排行
    if(countType == "DAY"){
      processHourDemandVideoBase(startDate,sqlContext).registerTempTable("t_demandVideoBase_hour")
      val cpSpRankHourDf = sqlContext.sql(
        s"""
           |select '${startDate}' as f_date,f_hour,cast(f_province_id as string) as f_province_id,f_province_name,cast(f_city_id as string) as f_city_id,
           |f_city_name,cast(f_region_id as string) as f_region_id,f_region_name,f_terminal,if(f_cp_sp is null or f_cp_sp ='','unknown',f_cp_sp) as f_cp_sp,
           |sum(f_play_time) as f_play_time,sum(1) as f_play_count,count(distinct f_user_id) as f_user_count,
           |(case when sum(f_play_time)<=600 then 1 when (sum(f_play_time)>600 and sum(f_play_time)<=3600) then 2 else 3 end) as f_user_type,
           |'HOUR' as f_count_type
           |from t_demandVideoBase_hour
           |group by f_hour,f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name,f_terminal,f_cp_sp
       """.stripMargin
      )
      val cpSpRankHourAllDf = sqlContext.sql(
        s"""
           |select '${startDate}' as f_date,f_hour,cast(f_province_id as string) as f_province_id,f_province_name,cast(f_city_id as string) as f_city_id,
           |f_city_name,cast(f_region_id as string) as f_region_id,f_region_name,f_terminal,'ALL' as f_cp_sp,
           |sum(f_play_time) as f_play_time,sum(1) as f_play_count,count(distinct f_user_id) as f_user_count,
           |(case when sum(f_play_time)<=600 then 1 when (sum(f_play_time)>600 and sum(f_play_time)<=3600) then 2 else 3 end) as f_user_type,
           |'HOUR' as f_count_type
           |from t_demandVideoBase_hour
           |group by f_hour,f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name,f_terminal
       """.stripMargin
      )
      try{
        //      cpSpRankDf.printSchema()
        DBUtils.saveDataFrameToPhoenixNew(cpSpRankHourDf,Tables.T_DEMAND_CP_SP_RANK_BY_HOUR)
        DBUtils.saveDataFrameToPhoenixNew(cpSpRankHourAllDf,Tables.T_DEMAND_CP_SP_RANK_BY_HOUR)
        println("cp sp排行按小时统计成功！")
      }catch {
        case e:Exception => {
          println("cp sp排行按小时统计成功！")
          e.printStackTrace()
        }
      }
    }
    demandVideoBaseDf.registerTempTable("t_demandVideoBaseDf")
    //分提供商统计
    val cpSpRankDf = sqlContext.sql(
      s"""
         |select '${startDate}' as f_date,f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name,f_terminal,f_cp_sp,
         |sum(f_play_time) as f_play_time,sum(f_count) as f_play_count,count(distinct f_user_id) as f_user_count,
         |(case when sum(f_play_time)<=600 then 1 when (sum(f_play_time)>600 and sum(f_play_time)<=3600) then 2 else 3 end) as f_user_type,
         |'${countType}' as f_count_type
         |from t_demandVideoBaseDf
         |group by f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name,f_terminal,f_cp_sp
       """.stripMargin
    )
    //全部提供商汇总
    val cpSpRankAllDf = sqlContext.sql(
      s"""
         |select '${startDate}' as f_date,f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name,f_terminal,'ALL' as f_cp_sp,
         |sum(f_play_time) as f_play_time,sum(f_count) as f_play_count,count(distinct f_user_id) as f_user_count,
         |(case when sum(f_play_time)<=600 then 1 when (sum(f_play_time)>600 and sum(f_play_time)<=3600) then 2 else 3 end) as f_user_type,
         |'${countType}' as f_count_type
         |from t_demandVideoBaseDf
         |group by f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name,f_terminal
       """.stripMargin
    )
    try{
//      cpSpRankDf.printSchema()
      DBUtils.saveDataFrameToPhoenixNew(cpSpRankDf,Tables.T_DEMAND_CP_SP_RANK)
      DBUtils.saveDataFrameToPhoenixNew(cpSpRankAllDf,Tables.T_DEMAND_CP_SP_RANK)
      println("cp sp排行统计成功！")
    }catch {
      case e:Exception => {
        println("cp sp排行统计失败！")
        e.printStackTrace()
      }
    }
  }

  /**
    *
    * @param demandVideoBaseDf
    * @param sqlContext
    * @param startDate
    * @param countType
    */
  def processUserPlay(demandVideoBaseDf:DataFrame,sqlContext:HiveContext,startDate:String,countType:String)={
    //    demandVideoBaseDf.registerTempTable("t_demandVideoBaseDf")
    import sqlContext.implicits._
    //    demandVideoBaseDf.show(false)
    val cpSpUserkDf = demandVideoBaseDf.map(x => {
      val userId = x.getAs[String]("f_user_id")
      var deviceNum = x.getAs[String]("device_num")
      if(deviceNum == null){
        deviceNum = "unknown"
      }
      val provinceId = x.getAs[String]("f_province_id")
      val provinceName = x.getAs[String]("f_province_name")
      val cityId = x.getAs[String]("f_city_id")
      val cityName = x.getAs[String]("f_city_name")
      val regionId = x.getAs[String]("f_region_id")
      val regionName = x.getAs[String]("f_region_name")
      val terminal = x.getAs[Int]("f_terminal")
      val cpSp = x.getAs[String]("f_cp_sp")
      val playTime = x.getAs[Long]("f_play_time")
      val playCount = x.getAs[Long]("f_count")
      val key = userId + "," + deviceNum + "," + provinceId + "," + provinceName + "," + cityId + "," + cityName + "," + regionId + "," + regionName + "," + terminal + "," + cpSp
      val value = (playTime,playCount)
      (key,value)
    }).reduceByKey((x,y) => {
      (x._1+y._1,x._2+y._2)
    }).map(x => {
      val keyArr = x._1.split(",")
      val userId = keyArr(0)
      val deviceNum = keyArr(1)
      val provinceId = keyArr(2)
      val provinceName = keyArr(3)
      val cityId = keyArr(4)
      val cityName = keyArr(5)
      val regionId = keyArr(6)
      val regionName = keyArr(7)
      val terminal = try{
        keyArr(8).toInt
      }catch {
        case e:Exception => {
          e.printStackTrace()
          1
        }
      }
      val cpSp = keyArr(9)
      val playTime = x._2._1
      val playCount = x._2._2
      (startDate,userId,deviceNum,provinceId,provinceName,cityId,cityName,regionId,regionName,terminal,cpSp,playTime,playCount,countType)
    }).toDF()
      .withColumnRenamed("_1","f_date")
      .withColumnRenamed("_2","f_user_id")
      .withColumnRenamed("_3","f_device_num")
      .withColumnRenamed("_4","f_province_id")
      .withColumnRenamed("_5","f_province_name")
      .withColumnRenamed("_6","f_city_id")
      .withColumnRenamed("_7","f_city_name")
      .withColumnRenamed("_8","f_region_id")
      .withColumnRenamed("_9","f_region_name")
      .withColumnRenamed("_10","f_terminal")
      .withColumnRenamed("_11","f_cp_sp")
      .withColumnRenamed("_12","f_play_time")
      .withColumnRenamed("_13","f_play_count")
      .withColumnRenamed("_14","f_count_type")
    /*val cpSpRankDf = sqlContext.sql(
      s"""
         |select '${startDate}' as f_date,f_user_id,nvl(device_num,'unknown') as f_device_num ,f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,
         |f_region_name,f_terminal,f_cp_sp,sum(f_play_time) as f_play_time,sum(f_count) as f_play_count,'${countType}' as f_count_type
         |from t_demandVideoBaseDf
         |group by f_user_id,device_num,f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name,f_terminal,f_cp_sp
       """.stripMargin
    )*/
    try{
      //      cpSpUserkDf.show(false)
//      cpSpUserkDf.printSchema()
      DBUtils.saveDataFrameToPhoenixNew(cpSpUserkDf,Tables.T_DEMAND_CP_SP_USER_PLAY)
      println("cp sp用户播放详情统计成功！")
    }catch {
      case e:Exception => {
        println("cp sp用户播放详情统计失败！")
        e.printStackTrace()
      }
    }
  }
  def processVideoPlay(demandVideoBaseDf:DataFrame,sqlContext:HiveContext,startDate:String,countType:String)= {
    //    demandVideoBaseDf.registerTempTable("t_demandVideoBaseDf")
    import sqlContext.implicits._
    val demandVideoListDf = getDemandVideoDf(sqlContext).selectExpr("cast(f_video_id as string) as video_id","f_content_type")//.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val cpspVideoDf = demandVideoBaseDf.map(x => {
      val userId = x.getAs[String]("f_user_id")
      val videoId = x.getAs[String]("f_video_id")
      val videoName = x.getAs[String]("f_video_name")
      val seriesId = x.getAs[String]("f_series_id")
      val seriesName = x.getAs[String]("f_series_name")
      val provinceId = x.getAs[String]("f_province_id")
      val provinceName = x.getAs[String]("f_province_name")
      val cityId = x.getAs[String]("f_city_id")
      val cityName = x.getAs[String]("f_city_name")
      val regionId = x.getAs[String]("f_region_id")
      val regionName = x.getAs[String]("f_region_name")
      val terminal = x.getAs[Int]("f_terminal")
      val cpSp = x.getAs[String]("f_cp_sp")
      val playTime = x.getAs[Long]("f_play_time")
      val playCount = x.getAs[Long]("f_count")
      val key = userId + "," + videoId + "," +videoName + "," +seriesId + "," +seriesName + "," + provinceId + "," + provinceName + "," + cityId + "," + cityName + "," + regionId + "," + regionName + "," + terminal + "," + cpSp
      val value = (playTime,playCount)
      (key,value)
    }).reduceByKey((x,y) => {
      (x._1+y._1,x._2+y._2)
    }).map(x => {
      val keyArr = x._1.split(",")
      val userId = keyArr(0)
      val videoId = keyArr(1)
      val videoName = keyArr(2)
      val seriesId = keyArr(3)
      val seriesName = keyArr(4)
      val provinceId = keyArr(5)
      val provinceName = keyArr(6)
      val cityId = keyArr(7)
      val cityName = keyArr(8)
      val regionId = keyArr(9)
      val regionName = keyArr(10)
      val terminal = keyArr(11)
      val cpSp = keyArr(12)
      val playTime = x._2._1
      val playCount= x._2._2
      val key = videoId + "," +videoName + "," +seriesId + "," +seriesName + "," + provinceId + "," + provinceName + "," + cityId + "," + cityName + "," + regionId + "," + regionName + "," + terminal + "," + cpSp
      val value = (playTime,playCount,1)
      (key,value)
    }).reduceByKey((x,y) => {
      (x._1+y._1,x._2+y._2,x._3+y._3)
    }).map(x => {
      val keyArr = x._1.split(",")
      /*val videoId = keyArr(0)
      val videoName = keyArr(1)
      val seriesId = keyArr(2)
      val seriesName = keyArr(3)
      val provinceId = keyArr(4)
      val provinceName = keyArr(5)
      val cityId = keyArr(6)
      val cityName = keyArr(7)
      val regionId = keyArr(8)
      val regionName = keyArr(9)
      val terminal = keyArr(10)
      val cpSp = keyArr(11)*/
      val playTime = x._2._1
      val playCount= x._2._2
      val userCount=x._2._3
      (keyArr(0),keyArr(1),keyArr(2),keyArr(3),keyArr(4),keyArr(5),keyArr(6),keyArr(7),keyArr(8),keyArr(9),keyArr(10).toInt,keyArr(11),playTime,playCount,userCount)
    }).toDF()
      .withColumnRenamed("_1","f_video_id")
      .withColumnRenamed("_2","f_video_name")
      .withColumnRenamed("_3","f_series_id")
      .withColumnRenamed("_4","f_series_name")
      .withColumnRenamed("_5","f_province_id")
      .withColumnRenamed("_6","f_province_name")
      .withColumnRenamed("_7","f_city_id")
      .withColumnRenamed("_8","f_city_name")
      .withColumnRenamed("_9","f_region_id")
      .withColumnRenamed("_10","f_region_name")
      .withColumnRenamed("_11","f_terminal")
      .withColumnRenamed("_12","f_cp_sp")
      .withColumnRenamed("_13","f_play_time")
      .withColumnRenamed("_14","f_play_count")
      .withColumnRenamed("_15","f_user_count")
    /*val cpSpRankDf = sqlContext.sql(
      s"""
         |select '${startDate}' as f_date,f_video_id,f_video_name,f_series_id,f_series_name,f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name,
         |f_terminal,f_cp_sp,sum(f_play_time) as f_play_time,sum(f_count) as f_play_count,count(distinct f_user_id) as f_user_count,'${countType}' as f_count_type
         |from t_demandVideoBaseDf
         |group by f_video_id,f_video_name,f_series_id,f_series_name,f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name,f_terminal,f_cp_sp
       """.stripMargin
    )*/
    val resultVideoDf = cpspVideoDf.join(demandVideoListDf,cpspVideoDf("f_video_id")===demandVideoListDf("video_id"),"left_outer").drop("video_id")
      .selectExpr(s"'$startDate' as f_date","f_video_id","f_video_name","f_series_id","f_series_name","cast(f_content_type as string) as f_content_type","f_province_id",
        "f_province_name","f_city_id","f_city_name","f_region_id","f_region_name","f_terminal","f_cp_sp","f_play_time","f_play_count","f_user_count",s"'$countType' as f_count_type")
    try{
      //      resultVideoDf.show(false)
//            resultVideoDf.printSchema()
      DBUtils.saveDataFrameToPhoenixNew(resultVideoDf,Tables.T_DEMAND_CP_SP_VIDEO_PLAY)
      println("cp sp节目播放详情统计成功！")
    }catch {
      case e:Exception => {
        println("cp sp节目播放详情统计失败！")
        e.printStackTrace()
      }
    }
  }
  def processUserType(demandVideoBaseDf:DataFrame,sqlContext:HiveContext,startDate:String,countType:String)={
    demandVideoBaseDf.registerTempTable("t_demandVideoBaseDf")
    val cpSpRankDf = sqlContext.sql(
      s"""
         |select '${startDate}' as f_date,f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name,f_terminal,f_cp_sp,
         |(case when sum(f_play_time)<=600 then 1
         |when (sum(f_play_time)>600 and sum(f_play_time)<=3600) then 2
         |else 3 end) as f_user_type,count(distinct f_user_id) as f_user_count,'${countType}' as f_count_type
         |from t_demandVideoBaseDf
         |group by f_province_id,f_province_name,f_city_id,f_city_name,f_region_id,f_region_name,f_terminal,f_cp_sp
       """.stripMargin
    )
    try{
      //      cpSpRankDf.show()
      DBUtils.saveDataFrameToPhoenixNew(cpSpRankDf,Tables.T_DEMAND_CP_SP_USER_TYPE)
    }catch {
      case e:Exception => {
        println("cp sp用户类型统计失败！")
        e.printStackTrace()
      }
    }
  }
  /**
    * 重复统计删除操作条件字符串获取
    * @param endDate
    * @param countType
    * @return
    */
  def getDeleteStrByCountType(startDate:String,endDate:String,countType:String):String={
    countType match {
      case "DAY" => s" and f_date='${startDate}' and f_count_type='${countType}'"
      case "7DAYS" => s" and f_date='${startDate}' and f_count_type='${countType}'"
      case "WEEK" => s" and f_date='${startDate}' and f_count_type='${countType}'"
      case "30DAYS" => s" and f_date='${startDate}' and f_count_type='${countType}'"
      case "MONTH" => s" and f_date='${startDate}' and f_count_type='${countType}'"
      case _ => " 1=0"
    }
  }
}
