package cn.ipanel.homed.repots

import cn.ipanel.common.{DBProperties, GatherType, SparkSession, Tables}
import cn.ipanel.homed.realtime.UserRegion
import cn.ipanel.utils.{DBUtils, DateUtils, RegionUtils, UserDeviceUtils}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime
import cn.ipanel.customization.wuhu.users.UsersProcess

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

//f_region_id,f_terminal,f_service_type,f_user_array,f_play_time,f_play_count
case class BusinessArray(f_region_id:String,f_terminal:Int,f_service_type:String,f_user_array:Array[String])
case class BusinessVisitByHalfHour(f_date: String, f_hour: Int, f_timerange: Int, f_terminal: Int, f_region_id: String, f_service_type: String,
                                   f_play_time:Long,f_play_count:Long,f_user_count:Int,f_active_user_count:Int)

case class BusinessVisitByDay(f_date: String, f_terminal: Int, f_region_id: String, f_service_type: String,
                              f_play_time:Long,f_play_count:Long,f_user_count:Int,f_active_user_count:Int)

case class BusinessVisitByType(f_start_date: String, f_end_date: String, f_terminal: Int, f_region_id: String,  f_service_type: String,
                               f_play_time:Long,f_play_count:Long,f_user_count:Int,f_active_user_count:Int)

//case class UserCntHalfhour(date: String, hour: Int, timeRange: Int, terminal: Int, regionId: String, serviceType: String, userCount:Int)
//case class UserCntByDay(date: String, terminal: Int, regionId: String, serviceType: String, userCount:Int)
//case class UserCntByType(startDate: String, endDate: String, terminal: Int, regionId: String, serviceType: String, userCount:Int)

/**
  * 业务访问总体统计
  *
  * @author lizhy@20181030
  */
object BusinessVisitCount {
  var partitionAmt = 200
  var minVisitTime = 0

  /**
    *
    * @param args
    * args(0):统计日期
    * args(1):分区数
    * args(2):访问时间阈值（秒），即播放低于此值不计入访问次数
    */
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("BusinessVisitCount")
    val sc: SparkContext = sparkSession.sparkContext
    val hiveContext: HiveContext = sparkSession.sqlContext
    var date = DateUtils.getYesterday()
//    var partAmt = 200
    if (args.length != 3) {
      System.err.println("请输入正确参数：统计日期,分区数,访问时间阈值， 例如【20181027】, [50] ,[5]")
      System.exit(-1)
    }
    if (args.length == 1) {
      date = DateUtils.dateStrToDateTime(args(0), DateUtils.YYYYMMDD).toString(DateUtils.YYYYMMDD)
    } else if (args.length == 2) {
      date = DateUtils.dateStrToDateTime(args(0), DateUtils.YYYYMMDD).toString(DateUtils.YYYYMMDD)
      partitionAmt = args(1).toInt
    } else if (args.length == 3) {
      date = DateUtils.dateStrToDateTime(args(0), DateUtils.YYYYMMDD).toString(DateUtils.YYYYMMDD)
      partitionAmt = args(1).toInt
      minVisitTime = args(2).toInt
    }

    try{


    hiveContext.sql("use bigdata")
    println("按天保存访问用户开始：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    saveBusinessArray(date,hiveContext,partitionAmt)
    println("按天保存访问用户结束：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))

    println("业务访问情况统计开始：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    businessVisitCount(sparkSession, sc, hiveContext, date, partitionAmt)
    println("业务访问情况统计结束：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))

    println("运营汇总业务访问情况用户详情开始：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
//    visitUserTopRank(hiveContext, date.toString, 100)
    visitUserAll(hiveContext, date.toString)
    println("运营汇总业务访问情况用户详情结束：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))

    println("各业务不活跃用户列表开始：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    inactiveUser(sc,hiveContext,date)
    println("各业务不活跃用户列表结束：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    }catch {
      case ex:Exception =>
        ex.printStackTrace()
    }
  }

  /**
    *
    * @param sparkSession
    * @param sc
    * @param sqlContext
    * @param date
    * @param partAmt
    */
  def businessVisitCount(sparkSession: SparkSession, sc: SparkContext, sqlContext: HiveContext, date: String, partAmt: Int) = {
    //项目部署地code(省或者地市)
    /*val regionCode = RegionUtils.getRootRegion
    val regionBC: Broadcast[String] = sparkSession.sparkContext.broadcast(regionCode)*/
    //区域信息
    val regionDF = getRegionDf(sc,sqlContext)
    //半小时统计
    println("半小时统计开始："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    busVisitCountByHalfhour(sqlContext, date, sc, "HALFHOUR",regionDF)
    //按天统计
    println("按天统计开始："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    busVisitCountByCountType(sqlContext, date, sc, "DAY",regionDF)
    //按自然周统计
    println("按自然周统计开始："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    busVisitCountByCountType(sqlContext, date, sc, "WEEK",regionDF)
    //按自然月统计
    println("按自然月统计开始："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    busVisitCountByCountType(sqlContext, date, sc, "MONTH",regionDF)
    //按季度统计
    /*println("按季度统计开始："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    busVisitCountByCountType(sqlContext, date, sc, "QUARTER")
    println("按季度统计结束："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    //按自然年统计
    println("按自然年统计开始："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    busVisitCountByCountType(sqlContext, date, sc, "YEAR")
    println("按自然年统计结束："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))*/
    //清除历史数据
//    println("清除历史数据:"+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    //deleteHist(date) //取消删除操作
    //7天内，30天内，一年内
    println("7天内开始："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    busVisitCountByCountType(sqlContext, date, sc, "7DAYS",regionDF)
/*    println("30天内开始："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    busVisitCountByCountType(sqlContext, date, sc, "30DAYS",regionDF)
    println("一年内开始："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    busVisitCountByCountType(sqlContext, date, sc, "1YEAR")
    println("一年内结束："+DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))*/
  }

  /**
    * 半小时统计
    * @param sqlContext
    * @param date
    * @param sc
    * @param regionDf
    */
  def busVisitCountByHalfhour(sqlContext: HiveContext, date: String, sc: SparkContext,countType:String, regionDf: DataFrame) = {
    import sqlContext.implicits._
    val daySql =
      s"""
         |select devicetype as terminal,regionid,playtype,starttime,endtime,userid
         |from ${Tables.ORC_VIDEO_PLAY} t
         |where day = '$date' and playtime > 0 and userid <> '' and userid is not null
      """.stripMargin
    val halfHourDf = sqlContext.sql(daySql)
      .map(x => {
        val busVisitBuffer = new ListBuffer[(String, Int, Int, String, String, String, Long, Long,String)] //date,hour,range,terminal,regionId,terminal,playTime,visitTimes
        val terminal = x.getAs[String]("terminal")
        val regionId = x.getAs[String]("regionid")
        val startTime = x.getAs[String]("starttime")
        var endTime = x.getAs[String]("endtime")
        if(DateUtils.dateStrToDateTime(endTime,DateUtils.YYYY_MM_DD_HHMMSS).toString(DateUtils.YYYYMMDD) > date){
          endTime = DateUtils.dateStrToDateTime(date,DateUtils.YYYYMMDD).plusDays(1).plusSeconds(-1).toString(DateUtils.YYYY_MM_DD_HHMMSS)
        }
        var serviceType = x.getAs[String]("playtype")
        val userId = x.getAs[String]("userid")
        if (serviceType == GatherType.one_key_timeshift) {
          serviceType = GatherType.TIME_SHIFT_NEW
        }
        val rangeList = getHalfHourRangeList(startTime, endTime)
        for (list <- rangeList) {
          val hour = list._1
          val range = list._2
          val playTime = list._3
          var visitTimes = 1
          if (playTime < minVisitTime) {
            visitTimes = 0
          }
          busVisitBuffer += ((date, hour, range, terminal, regionId, serviceType, playTime, visitTimes,userId))
        }
        busVisitBuffer.toIterator
      })
      .flatMap(x => x)
    //时长、播放次数、用户数、活跃用户数
    val countDf = halfHourDf.map(x => {
      //(date, hour, range, terminal, regionId, serviceType, playTime, visitTimes,userId)
      val date = x._1
      val hour = x._2
      val range = x._3
      val terminal = x._4
      val regionId = x._5
      val serviceType = x._6
      val playTime = x._7
      val playCount = x._8
      val userId = x._9
      val key = date + "," + hour + "," + range + "," + terminal + "," + regionId + "," + serviceType + "," + userId
      val value = (playTime,playCount)
      (key,value)
    }).reduceByKey((x,y) => {
      (x._1 + y._1,x._2 + y._2)
    })
      .map(x => {
        val keyArr = x._1.split(",")
        val date = keyArr(0)
        val hour = keyArr(1).toInt
        val range = keyArr(2).toInt
        val terminal = keyArr(3).toInt
        val regionId = keyArr(4)
        val serviceType = keyArr(5)
        //        val userId = keyArr(6)
        val playTime = x._2._1
        val playCount = x._2._2
        var activeuserCount = 0
        if(playCount >= 2){//活跃用户条件，访问次数大于等于两次
          activeuserCount = 1
        }
        val userCount = 1
        val key = date + "," + hour + "," + range + "," + terminal + "," + regionId + "," + serviceType
        val value = (playTime,playCount,activeuserCount,userCount)
        (key,value)
      }).reduceByKey((x,y) =>{
      (x._1 + y._1,x._2 + y._2,x._3 + y._3, x._4 + y._4)
    }).map(x => {
      val keyArr = x._1.split(",")
      val date = keyArr(0)
      val hour = keyArr(1).toInt
      val timeRange = keyArr(2).toInt
      val terminal = keyArr(3).toInt
      val regionId = keyArr(4)
      val serviceType = keyArr(5)
      val value = x._2
      val playTime = value._1
      val playCount = value._2
      val activeUserCount = value._3
      val userCount = value._4
      BusinessVisitByHalfHour(date,hour,timeRange,terminal,regionId,serviceType,playTime,playCount,userCount,activeUserCount)
    }).toDF()
    println("接半小时countDf记录数：" + countDf.count())
    saveDfToDB(regionDf,countDf,countType)
  }

  /**
    * 按不同类型统计
    *
    * @param sqlContext
    * @param date
    * @param sc
    * @param countType 统计类型:WEEK,MONTH,QUARTER,YEAR,7DAYS,30DAYS,1YEAR
    */
  def busVisitCountByCountType(sqlContext: HiveContext, date: String, sc: SparkContext, countType: String,regionDf:DataFrame) = {
    import sqlContext.implicits._
    val countFromDate = getStartDateByType(countType, date)
    val countSql =
      s"""
         |select f_region_id,f_terminal,f_service_type,f_user_array
         |from bigdata.${Tables.T_BUS_ARRAY_DAY} t
         |where date between '$countFromDate' and '$date'
      """.stripMargin
    //详细用户
    val busByUsersDf = sqlContext.sql(countSql).repartition(partitionAmt)
      .map(x => {
        //f_terminal: Int, f_region_id: String,  f_service_type: String,f_play_time:Long,f_play_count:Long:Int,f_active_user_count:Int
        val listBuffer = new ListBuffer[(String,Int, String,String,Long,Long)]
        val regionId = x.getAs[String]("f_region_id")
        val terminal = x.getAs[Int]("f_terminal")
        val serviceType = x.getAs[String]("f_service_type")
        val userArray = x.getAs[Seq[String]]("f_user_array").toArray
        for(usrArr <- userArray){
          val arr = usrArr.split("\\|")
          val userId = arr(0)
          val playCount= arr(1).toLong
          val playTime= arr(2).toLong
          listBuffer += ((userId,terminal,regionId,serviceType,playTime,playCount))
        }
        listBuffer.toList
      })
    //1.分业务统计汇总
    val saveByServiceDf = busByUsersDf.flatMap(x => x)
      .map(x => {
        val userId = x._1
        val terminal = x._2
        val regionId = x._3
        val serviceType = x._4
        val playTime = x._5
        val playCount = x._6

        val key = regionId + "," + terminal + "," +  serviceType  + "," + userId
        val value = (playTime,playCount)
        (key,value)
      })
      .reduceByKey((x,y) => {
        (x._1 + y._1, x._2 + y._2)
      })
      .map(x => {
        val arr = x._1.split(",")
        val regionId = arr(0)
        val terminal = arr(1)
        val serviceType = arr(2)
        val playTime = x._2._1
        val playCount = x._2._2
        val userCount = 1
        var activeUserCount = 0
        if(playCount >= 2){
          activeUserCount = 1
        }
        val key = regionId + "," + terminal + "," +  serviceType
        val value = (playTime,playCount,userCount,activeUserCount)
        (key,value)
      })
      .reduceByKey((x,y) =>{
        (x._1 + y._1,x._2 + y._2,x._3 + y._3,x._4 + y._4)
      })
      .map(x => {
        val arr = x._1.split(",")
        val regionId = arr(0)
        val terminal = arr(1).toInt
        val serviceType = arr(2)
        val playTime = x._2._1
        val playCount = x._2._2
        val userCount = x._2._3
        var activeUserCount = x._2._4
        BusinessVisitByType(countFromDate,date,terminal,regionId,serviceType,playTime,playCount,userCount,activeUserCount)
      }).toDF().where("f_play_count > 0")
    //2.不分业务统计汇总
    val saveByAllServiceDf = busByUsersDf.flatMap(x => x)
      .map(x => {
        val userId = x._1
        val terminal = x._2
        val regionId = x._3
        val serviceType = "ALL" //x._4
        val playTime = x._5
        val playCount = x._6
        val key = regionId + "," + terminal + "," +  serviceType  + "," + userId
        val value = (playTime,playCount)
        (key,value)
      })
      .reduceByKey((x,y) => {
        (x._1 + y._1, x._2 + y._2)
      })
      .map(x => {
        val arr = x._1.split(",")
        val regionId = arr(0)
        val terminal = arr(1)
        val serviceType = arr(2)
        val playTime = x._2._1
        val playCount = x._2._2
        val userCount = 1
        var activeUserCount = 0
        if(playCount >= 2){
          activeUserCount = 1
        }
        val key = regionId + "," + terminal + "," +  serviceType
        val value = (playTime,playCount,userCount,activeUserCount)
        (key,value)
      })
      .reduceByKey((x,y) =>{
        (x._1 + y._1,x._2 + y._2,x._3 + y._3,x._4 + y._4)
      })
      .map(x => {
        val arr = x._1.split(",")
        val regionId = arr(0)
        val terminal = arr(1).toInt
        val serviceType = arr(2)
        val playTime = x._2._1
        val playCount = x._2._2
        val userCount = x._2._3
        var activeUserCount = x._2._4
        BusinessVisitByType(countFromDate,date,terminal,regionId,serviceType,playTime,playCount,userCount,activeUserCount)
      }).toDF().where("f_play_count > 0")
    //指定类型保存到phoenix
    //saveDfToDB(regionDf,saveDf, countType)
    //需要同时统计分业务汇总及不分业务汇总
    println("saveByServiceDf记录数：" + saveByServiceDf.count())
    println("saveByAllServiceDf记录数：" + saveByAllServiceDf.count())
    saveDfToDB(regionDf,saveByServiceDf, countType) //保存分业务统计数据
    saveDfToDB(regionDf,saveByAllServiceDf, countType) //保存不分业务统计数据
    sqlContext.clearCache()
  }

  //根据指定类型保存到phoenix不同表
  def saveDfToDB(regionDf:DataFrame,visitDf: DataFrame, countType: String) = {
    val visitTmpDf = visitDf.join(regionDf,visitDf("f_region_id") === regionDf("f_area_id"))
    countType match {
      case "HALFHOUR" => {
        val df = visitTmpDf.selectExpr("f_date","f_hour","f_timerange","f_province_id","f_province_name","f_city_id",
          "f_city_name","f_region_id","f_region_name","f_terminal","f_service_type","f_play_time","f_play_count","f_user_count","f_active_user_count")
        DBUtils.saveDataFrameToPhoenixNew(df, Tables.T_BUS_VISIT_HALFHOUR)
      }
      case "DAY" => {
        val df = visitTmpDf.selectExpr("f_start_date as f_date","f_province_id","f_province_name","f_city_id",
          "f_city_name","f_region_id","f_region_name","f_terminal","f_service_type","f_play_time","f_play_count","f_user_count","f_active_user_count")
        DBUtils.saveDataFrameToPhoenixNew(df, Tables.T_BUS_VISIT_DAY)
      }
      case "WEEK" => {
        val df = visitTmpDf.selectExpr("f_start_date as f_date","f_province_id","f_province_name","f_city_id",
          "f_city_name","f_region_id","f_region_name","f_terminal","f_service_type","f_play_time","f_play_count","f_user_count","f_active_user_count")
        DBUtils.saveDataFrameToPhoenixNew(df, Tables.T_BUS_VISIT_WEEK)
      }
      case "MONTH" => {
        val df = visitTmpDf.selectExpr("substr(f_start_date,0,6) as f_date","f_province_id","f_province_name","f_city_id",
          "f_city_name","f_region_id","f_region_name","f_terminal","f_service_type","f_play_time","f_play_count","f_user_count","f_active_user_count")
        DBUtils.saveDataFrameToPhoenixNew(df, Tables.T_BUS_VISIT_MONTH)
      }
      case "QUARTER" => {
        val df = visitTmpDf.selectExpr("substr(f_start_date,0,6) as f_date","f_province_id","f_province_name","f_city_id",
          "f_city_name","f_region_id","f_region_name","f_terminal","f_service_type","f_play_time","f_play_count","f_user_count","f_active_user_count")
        DBUtils.saveDataFrameToPhoenixNew(df, Tables.T_BUS_VISIT_QUARTER)
      }
      case "YEAR" => {
        val df = visitTmpDf.selectExpr("substr(f_start_date,0,4) as f_date","f_province_id","f_province_name","f_city_id",
          "f_city_name","f_region_id","f_region_name","f_terminal","f_service_type","f_play_time","f_play_count","f_user_count","f_active_user_count")
        DBUtils.saveDataFrameToPhoenixNew(df, Tables.T_BUS_VISIT_YEAR)
      }
      case "7DAYS" => {
        val df = visitTmpDf.selectExpr("f_start_date","f_end_date","f_province_id","f_province_name","f_city_id",
          "f_city_name","f_region_id","f_region_name","f_terminal","f_service_type","f_play_time","f_play_count","f_user_count","f_active_user_count")
        DBUtils.saveDataFrameToPhoenixNew(df, Tables.T_BUS_VISIT_HISTORY)
      }
      case "30DAYS" => {
        val df = visitTmpDf.selectExpr("f_start_date","f_end_date","f_province_id","f_province_name","f_city_id",
          "f_city_name","f_region_id","f_region_name","f_terminal","f_service_type","f_play_time","f_play_count","f_user_count","f_active_user_count")
        DBUtils.saveDataFrameToPhoenixNew(df, Tables.T_BUS_VISIT_HISTORY)
      }
      case "1YEAR" => {
        val df = visitTmpDf.selectExpr("f_start_date","f_end_date","f_province_id","f_province_name","f_city_id",
          "f_city_name","f_region_id","f_region_name","f_terminal","f_service_type","f_play_time","f_play_count","f_user_count","f_active_user_count")
        DBUtils.saveDataFrameToPhoenixNew(df, Tables.T_BUS_VISIT_HISTORY)
      }
      case _ =>
    }
  }

  /**
    * 半小时列表
    *
    * @param startTime
    * @param endTime
    * @param datePatern
    * @return
    */
  def getHalfHourRangeList(startTime: String, endTime: String, datePatern: String = DateUtils.YYYY_MM_DD_HHMMSS): ListBuffer[(Int, Int, Long)] = {
    val list = new ListBuffer[(Int, Int, Long)]
    var startDt = DateUtils.dateStrToDateTime(startTime, datePatern)
    val year = startDt.getYear
    val month = startDt.getMonthOfYear
    var tmpDate = startDt.getDayOfMonth
    val endDt = DateUtils.dateStrToDateTime(endTime, datePatern)
    while (startDt.isBefore(endDt)) {
      val hour = startDt.getHourOfDay
      var minute = startDt.getMinuteOfHour
      var tmpHour = startDt.getHourOfDay
      var tmpMinute = startDt.getMinuteOfHour
      var tmpSec = 0
      if (minute >= 30) {
        minute = 60
        tmpMinute = 0
        tmpHour = hour + 1
      } else {
        minute = 30
        tmpMinute = 30
      }
      if (tmpHour == 24) {
        tmpHour = 23
        tmpMinute = 59
        tmpSec = 59
      }
      val tmpDt = new DateTime(year, month, tmpDate, tmpHour, tmpMinute, tmpSec)
      val playTime = if (tmpDt.isBefore(endDt)) {
        tmpDt.getSecondOfDay - startDt.getSecondOfDay
      } else {
        endDt.getSecondOfDay - startDt.getSecondOfDay
      } //秒
      startDt = tmpDt
      list += ((hour, minute, playTime.toLong))
    }
    list
  }

  /**
    * 根据统计类型获取开始日期
    *
    * @param countType
    * @param date
    * @return
    */
  def getStartDateByType(countType: String, date: String): String = {
    val countFromDate =
      countType match {
        case "DAY" => date
        case "WEEK" => DateUtils.getFirstDateOfWeek(date)
        case "MONTH" => DateUtils.getFirstDateOfMonth(date)
        case "QUARTER" => DateUtils.getFirstDateOfQuarter(date)
        case "YEAR" => DateUtils.getFirstDateOfYear(date)
        case "7DAYS" => DateUtils.getDateByDays(date, 7)
        case "30DAYS" => DateUtils.getDateByDays(date, 30)
        case "1YEAR" => DateUtils.getDateByDays(date, 365)
        case _ => "-1"
      }
//    println("根据统计类型获取开始日期countFromDate:" + countFromDate)
    countFromDate
  }

  def getOnlineUserCntByRange(sqlContext:HiveContext,startDate:String,endDate:String): DataFrame={
    val sql =
      s"""
         |select terminal,regionId,case when playType = '${GatherType.one_key_timeshift}' then '${GatherType.TIME_SHIFT_NEW}' else playType end as serviceType
         |,count(distinct userid) as f_user_count
         |from ${Tables.ORC_VIDEO_PLAY} t
         |where day between $startDate and $endDate and userid <> '' and userid is not null
         |group by terminal,regionId,case when playType = '${GatherType.one_key_timeshift}' then '${GatherType.TIME_SHIFT_NEW}' else playType end
        """.stripMargin
    sqlContext.sql(sql)
  }

  /**
    * 按天各业务统计保存
    * @param date
    * @param sqlContext
    * @param partAmt
    */
  def saveBusinessArray(date:String,sqlContext:HiveContext,partAmt:Int)={
    import sqlContext.implicits._

    val sqlLive =
      s"""
         |select userId as f_user_id,deviceId as f_device_id,
         |cast(devicetype as int) as f_terminal,
         |regionId as f_region_id,
         |playtype as f_service_type,
         |playtime as f_play_time
         |from ${Tables.ORC_VIDEO_PLAY}
         |where day=$date and playtime > 0  and userid <> '' and userid is not null--and serviceId is not null and regionid is not null and devicetype is not null
      """.stripMargin
    val sourceDf = sqlContext.sql(sqlLive)
    val recordAmt = sourceDf.count()
    if(recordAmt == 0){
      System.err.println("源数据记录数为0，请检查清洗程序是否执行成功！")
      System.exit(-1)
    }else{
      println("原始记录数：" + recordAmt)
    }
    sourceDf.map(x => {
        val terminal = x.getAs[Int]("f_terminal")
        val userId = x.getAs[String]("f_user_id")
        val deviceId = x.getAs[Long]("f_device_id").toString
        val regionId = x.getAs[String]("f_region_id")
        var serviceType = x.getAs[String]("f_service_type")
        if(serviceType == "one-key-timeshift"){
          serviceType = "timeshift"
        }
        val playCount = 1L
        val playTime = x.getAs[Long]("f_play_time")
        val key = terminal + "," + regionId + "," + serviceType + "," + userId + "," +  deviceId
        //        val value = (Set(userId),playTime,playCount)
        val value = (playTime,playCount)
        (key,value)
      }).reduceByKey((x,y) => {
      val playTime = x._1 + y._1
      //      val userSet = x._1 ++ y._1
      val playCnt = x._2 + y._2
      //      (userSet,playTime,playCnt)
      (playTime,playCnt)
    }).map(x => {
      val keyArr = x._1.split(",")
      val terminal = keyArr(0).toInt
      val regionId = keyArr(1)
      val serviceType = keyArr(2)
      val userId = keyArr(3)
      val deviceId = keyArr(4)
      //      val userArray = x._2._1.toArray
      val playTime: Long = x._2._1 //秒
      val playCount = x._2._2
      //      BusinessArray(regionId,terminal,serviceType,userArray,playTime,playCount)
      val key = terminal + "," + regionId + "," + serviceType
      val userArray = Array(userId + "|" + playCount + "|" + playTime + "|" + deviceId) //保存格式为user_id|play_count|play_time|device_id
      val value = userArray
      (key,value)
    }).reduceByKey((x,y) => {
      (x ++ y)
    }).map(x => {
      val keyArr = x._1.split(",")
      val terminal = keyArr(0).toInt
      val regionId = keyArr(1)
      val serviceType = keyArr(2)
      val userArray = x._2
      //BusinessArray(f_region_id:String,f_terminal:Int,f_service_type:String,f_user_array:Array[String],f_play_time:Long,f_play_count:Long)
      BusinessArray(regionId,terminal,serviceType,userArray)
    }).repartition(partAmt/6).toDF().registerTempTable("t_bus_array_tmp")
    sqlContext.sql(
      s"""
         |insert overwrite table ${Tables.T_BUS_ARRAY_DAY} partition(date='$date')
         |select f_region_id,f_terminal,f_service_type,f_user_array
         |from t_bus_array_tmp uat
      """.stripMargin)
  }

  /**
    *
    * @param sc
    * @param sqlContext
    * @return
    */
  def getRegionDf(sc:SparkContext,sqlContext:HiveContext)={
    //项目部署地code(省或者地市)
    val regionCode = RegionUtils.getRootRegion
    val regionBC: Broadcast[String] = sc.broadcast(regionCode)
    //区域信息表
    val region =
      s"""
         |(select cast(a.area_id as char) as f_area_id,a.area_name as f_region_name,
         |cast(a.city_id as char) as f_city_id,c.city_name as f_city_name,
         |cast(c.province_id as char) as f_province_id,p.province_name as f_province_name
         |from (area a left join city c on a.city_id=c.city_id)
         |left join province p on c.province_id=p.province_id
         |where a.city_id=${regionBC.value} or c.province_id=${regionBC.value}
         |) as region
        """.stripMargin
    DBUtils.loadMysql(sqlContext, region, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
  }
/*
  /**
    * 删除历史数据
    * @param date
    */
  def deleteHist(date:String):Unit ={
    val delSql = s"delete from ${Tables.T_BUS_VISIT_HISTORY} where f_end_date < '$date'"
    DBUtils.excuteSqlPhoenix(delSql)
  }*/

  /**
    * 统计播放top N用户
    * @param sqlContext
    * @param date
    */
  def visitUserTopRank(sqlContext:HiveContext,date:String,topNum:Int = 100): Unit ={
    println("按天top用户开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    visitUserRankByCountType(sqlContext,date,"DAY",topNum)
    println("按7天top用户开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    visitUserRankByCountType(sqlContext,date,"7DAYS",topNum) //20190506 打开7天内统计
    println("按周top用户开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    visitUserRankByCountType(sqlContext,date,"WEEK",topNum)
//    println("按30天内top用户开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
//    visitUserRankByCountType(sqlContext,date,"30DAYS",topNum)
    println("按月top用户开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    visitUserRankByCountType(sqlContext,date,"MONTH",topNum)
  }
  /**
    * 统计播放全量用户
    * @param sqlContext
    * @param date
    */
  def visitUserAll(sqlContext:HiveContext,date:String): Unit ={
    println("按天业务访问用户开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    visitUserByCountType(sqlContext,date,"DAY")
    println("按7天业务访问用户开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    visitUserByCountType(sqlContext,date,"7DAYS")
    println("按周业务访问用户开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    visitUserByCountType(sqlContext,date,"WEEK")
//    println("按30天内业务访问用户开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    //    visitUserByCountType(sqlContext,date,"30DAYS")
    println("按月业务访问用户开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    visitUserByCountType(sqlContext,date,"MONTH")
  }

  /**
    * 各播放类型不活中跃用户列表
    * @param sc
    * @param sqlContext
    * @param date
    */
  def inactiveUser(sc:SparkContext,sqlContext:HiveContext,date:String)={
    println("按天各业务不活跃用户统计开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    inactiveUserByCountType(sc,sqlContext,date,"DAY")
    println("按7天各业务不活跃用户统计开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    inactiveUserByCountType(sc,sqlContext,date,"7DAYS")
    println("按周各业务不活跃用户统计开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    inactiveUserByCountType(sc,sqlContext,date,"WEEK")
//    println("按30天内各业务不活跃用户统计开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
//    inactiveUserByCountType(sc,sqlContext,date,"DAY")
    println("按月各业务不活跃用户统计开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    inactiveUserByCountType(sc,sqlContext,date,"MONTH")
  }

  /**
    * 按类型统计播放top N用户
    * @param sqlContext
    * @param endDate
    * @param countType
    * @param topNum
    */
  def visitUserRankByCountType(sqlContext:HiveContext,endDate:String,countType:String, topNum:Int = 100): Unit ={
    //日：DAY
    //一周内：7DAYS
    //周：WEEK
    //30天内：30DAYS
    //月：MONTH
    import sqlContext.implicits._
    val regionDf = UserRegion.getRegionDf(sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val deviceNumDf  = UserDeviceUtils.getDeviceNumDf(sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val startDate = countType match {
      case "DAY" => endDate
      case "7DAYS" => DateUtils.getDateByDays(endDate,7)
      case "WEEK" => DateUtils.getFirstDateOfWeek(endDate)
      case "30DAYS" => DateUtils.getDateByDays(endDate,30)
      case "MONTH" => DateUtils.getFirstDateOfMonth(endDate)
      case  _ => "unknown"
    }
    val deleteSqlStr = getDeleteStrByCountType(startDate,endDate,countType) //20190508使用统一方法获取
      /*countType match {
      case "DAY" => s" and f_date='${startDate}' and f_count_type='${countType}'"
      case "7DAYS" => s" and 0=1 and f_count_type='${countType}'" //取消删除操作
      case "WEEK" => s" and f_date='${startDate}' and f_count_type='${countType}'"
      case "30DAYS" => s" and f_count_type='${countType}'"
      case "MONTH" => s" and f_date='${startDate}' and f_count_type='${countType}'"
      case  _ => " 1=0"
    }*/
    val visitSql =
      s"""
         |select f_region_id,f_terminal,f_service_type,f_user_array
         |from ${Tables.T_BUS_ARRAY_DAY} where date >= '$startDate' and date <= '$endDate'
      """.stripMargin
    val visitDevIdDf = sqlContext.sql(visitSql).repartition(partitionAmt)
      .map(x => {
        val userListBuffer = new ListBuffer[(String,String,String,Int,String,Int,Long)] //userid,deviceid,regionid,terminal,servicetype,playcount
        val regionId = x.getAs[String]("f_region_id")
        val terminal = x.getAs[Int]("f_terminal")
        val serviceType = x.getAs[String]("f_service_type")
        val userArray = x.getAs[Seq[String]]("f_user_array").toArray
        for(usrArr <- userArray){
          val arr = usrArr.split("\\|")
          val userId = arr(0)
          val playCount= arr(1).toInt
          val playTime = arr(2).toLong
          val deviceId = try {
            arr(3)
          }catch {
            case e:Exception => "-1"
          }
            userListBuffer += ((userId,deviceId,regionId,terminal,serviceType,playCount,playTime))
        }
        userListBuffer.toList
      }).flatMap(x => x).toDF()
      .withColumnRenamed("_1","f_user_id")
      .withColumnRenamed("_2","f_device_id")
      .withColumnRenamed("_3","f_region_id")
      .withColumnRenamed("_4","f_terminal")
      .withColumnRenamed("_5","f_service_type")
      .withColumnRenamed("_6","f_play_count")
      .withColumnRenamed("_7","f_play_time")
      val visitDevNumDf = visitDevIdDf.join(deviceNumDf,visitDevIdDf("f_device_id")===deviceNumDf("device_id"),"left_outer")
        .withColumnRenamed("device_num","f_device_num")

     val visitUserDf =
       visitDevNumDf.groupBy("f_user_id","f_device_num","f_region_id","f_terminal","f_service_type")
      .agg("f_play_count" -> "sum","f_play_time" -> "sum")
        .withColumnRenamed("sum(f_play_count)","f_play_count")
        .withColumnRenamed("sum(f_play_time)","f_play_time")
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._
    val partWinSpec = Window.partitionBy("f_region_id","f_service_type","f_terminal").orderBy(desc("f_play_count"))
    val topNDf = visitUserDf.withColumn("row_num",row_number() over(partWinSpec))
      .where(s"row_num <= ${topNum}")
    val joinDf = topNDf.join(regionDf,topNDf("f_region_id")===regionDf("f_area_id"),"inner")
        /*.join(deviceNumDf,topNDf("f_device_id")===deviceNumDf("device_id"),"left_outer")
        .withColumnRenamed("device_num","f_device_num")*/
        .selectExpr(s"'${startDate}' as f_date","f_user_id","nvl(f_device_num,'-') as f_device_num","f_province_id","f_province_name","f_city_id","f_city_name"
          ,"f_region_id","f_region_name","f_terminal","f_service_type","f_play_count",s"'${countType}' as f_count_type","f_play_time")
    try{
      val deleteSql = s"delete from ${Tables.T_SERVICE_VISIT_USER_TOP_RANK} where 1=1 " + deleteSqlStr
      DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD,deleteSql)
      println("成功删除历史数据，日期：" + endDate + ",统计类型：" + countType)
      DBUtils.saveToMysql(joinDf, Tables.T_SERVICE_VISIT_USER_TOP_RANK, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD) //保存到mysql
    }catch {
      case e:Exception => {
        e.printStackTrace()
        println("数据保存失败！时间：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS) + ",统计类型：" + countType)
      }
    }
    regionDf.unpersist()
    deviceNumDf.unpersist()
  }
  /**
    * 按类型统计全量播放用户
    * @param sqlContext
    * @param endDate
    * @param countType
    */
  def visitUserByCountType(sqlContext:HiveContext,endDate:String,countType:String): Unit ={
    //日：DAY
    //一周内：7DAYS
    //周：WEEK
    //30天内：30DAYS
    //月：MONTH
    import sqlContext.implicits._
    val regionDf = UserRegion.getRegionDf(sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val deviceNumDf  = UserDeviceUtils.getDeviceNumDf(sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val personelInfDf = getUserPersonelInfo(endDate,sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER) //个人信息
    val startDate = countType match {
      case "DAY" => endDate
      case "7DAYS" => DateUtils.getDateByDays(endDate,7)
      case "WEEK" => DateUtils.getFirstDateOfWeek(endDate)
      case "30DAYS" => DateUtils.getDateByDays(endDate,30)
      case "MONTH" => DateUtils.getFirstDateOfMonth(endDate)
      case  _ => "unknown"
    }
    /*val deleteSqlStr = countType match {
      case "DAY" => s" and f_date='${startDate}' and f_count_type='${countType}'"
      case "7DAYS" => s" and f_date='${startDate}' and f_count_type='${countType}'"
      case "WEEK" => s" and f_date='${startDate}' and f_count_type='${countType}'"
      case "30DAYS" => s" and f_count_type='${countType}'"
      case "MONTH" => s" and f_date='${startDate}' and f_count_type='${countType}'"
      case  _ => " 1=0"
    }*/
    val visitSql =
      s"""
         |select f_region_id,f_terminal,f_service_type,f_user_array
         |from bigdata.${Tables.T_BUS_ARRAY_DAY} where date >= '$startDate' and date <= '$endDate'
      """.stripMargin
    val visitDevIdDf = sqlContext.sql(visitSql).repartition(partitionAmt)
      .map(x => {
        val userListBuffer = new ListBuffer[(String,String,String,Int,String,Int,Long)] //userid,deviceid,regionid,terminal,servicetype,playcount
        val regionId = x.getAs[String]("f_region_id")
        val terminal = x.getAs[Int]("f_terminal")
        val serviceType = x.getAs[String]("f_service_type")
        val userArray = x.getAs[Seq[String]]("f_user_array").toArray
        for(usrArr <- userArray){
          val arr = usrArr.split("\\|")
          val userId = arr(0)
          val playCount= arr(1).toInt
          val playTime = arr(2).toLong
          val deviceId = try {
            arr(3)
          }catch {
            case e:Exception => "-1"
          }
          userListBuffer += ((userId,deviceId,regionId,terminal,serviceType,playCount,playTime))
        }
        userListBuffer.toList
      }).flatMap(x => x).toDF()
      .withColumnRenamed("_1","f_user_id")
      .withColumnRenamed("_2","f_device_id")
      .withColumnRenamed("_3","f_region_id")
      .withColumnRenamed("_4","f_terminal")
      .withColumnRenamed("_5","f_service_type")
      .withColumnRenamed("_6","f_play_count")
      .withColumnRenamed("_7","f_play_time")
    val visitDevNumDf = visitDevIdDf.join(deviceNumDf,visitDevIdDf("f_device_id")===deviceNumDf("device_id"),"left_outer")
      .withColumnRenamed("device_num","f_device_num")

    val visitUserDf =
      visitDevNumDf.groupBy("f_user_id","f_device_num","f_region_id","f_terminal","f_service_type")
        .agg("f_play_count" -> "sum","f_play_time" -> "sum")
        .withColumnRenamed("sum(f_play_count)","f_play_count")
        .withColumnRenamed("sum(f_play_time)","f_play_time")
      .where("f_play_count > 0")
    val joinDf = visitUserDf.join(regionDf,visitUserDf("f_region_id")===regionDf("f_area_id"),"inner")
      ///20190506增加个人信息
      .join(personelInfDf,visitUserDf("f_user_id")===personelInfDf("p_user_id")&&visitUserDf("f_region_id")===personelInfDf("p_region_id"),"left_outer")
      .selectExpr(s"'${startDate}' as f_date","f_user_id","nvl(f_device_num,'-') as f_device_num","f_province_id","f_province_name","f_city_id","f_city_name"
      ,"f_region_id","f_region_name","f_terminal","f_service_type","f_play_count",s"'${countType}' as f_count_type","f_play_time","f_id_card","f_user_name","f_user_address","f_user_mobile_no")
    try{
      val deleteSql = s"delete from ${Tables.T_SERVICE_VISIT_USER} where 1=1  and f_date='${startDate}' and f_count_type='${countType}'"
//      println("delete sql: " + deleteSql)
//      DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD,deleteSql)
      DBUtils.excuteSqlPhoenix(deleteSql)
      println("成功删除历史数据，日期：" + endDate + ",统计类型：" + countType)
//      DBUtils.saveToMysql(joinDf, Tables.T_SERVICE_VISIT_USER, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD) //保存到mysql
      println("将写入t_service_visit_users记录数："+joinDf.count())
      DBUtils.saveDataFrameToPhoenixNew(joinDf, Tables.T_SERVICE_VISIT_USER) //保存到phoenix
    }catch {
      case e:Exception => {
        println("数据保存失败！时间：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS) + ",统计类型：" + countType)
      }
    }
    regionDf.unpersist()
    deviceNumDf.unpersist()
    personelInfDf.unpersist()
  }
  /*/**
    * 获取设备号信息
    * @param sqlContext
    */
  def getDeviceNumDf(sqlContext: HiveContext)={
    val deviceSql =
      s"""
         |(select dv.device_id,
         |CASE WHEN dv.device_type = 1 THEN dv.stb_id
         | WHEN dv.device_type = 2 THEN dv.cai_id
         | WHEN dv.device_type = 3 THEN dv.mobile_id
         | WHEN dv.device_type = 4 THEN dv.pad_id
         | WHEN dv.device_type = 5 THEN dv.mac_address else 'unknown' end as device_num
         |from ${Tables.T_DEVICE_INFO} dv where status = 1) as device
       """.stripMargin
    DBUtils.loadMysql(sqlContext, deviceSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
  }*/

  /**
    * 获取用户个人信息
    */

  def getUserPersonelInfo(date:String,sqlContext:HiveContext): DataFrame ={
//    person_info.identity_card 身份证
//    account_info.mobile_no 电话号码
//    account_info.nick_name 用户姓名
//    address_info.address_name 地址
    val clusterRegion = RegionUtils.getRootRegion //配置文件中所配置的地方项目区域码
    val accInfoSql =
      s"""
         |(select cast(DA as char) as da, cast(home_id as char) as home_id, nick_name, mobile_no
         |from account_info where status=1) as acc
       """.stripMargin
    val persSql =
      s"""
         |(select cast(DA as char) as f_user_id,identity_card as f_id_card
         |from person_info where status=1) as per
       """.stripMargin
    val addSql =
      s"""
         |(select address_name,cast(home_id as char) as ad_home_id,cast(region_id as char) as region_id
         |from address_info where status=1) as ad
       """.stripMargin
    //地方定制化 - 用户
    val accInfDf = clusterRegion match {
      case "340200" => { //芜湖
        UsersProcess.getEffectedUserDf(date,sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
      }
      case _ => { //默认
        DBUtils.loadMysql(sqlContext, accInfoSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).persist(StorageLevel.MEMORY_AND_DISK_SER)
      }
    }
    val perDf = DBUtils.loadMysql(sqlContext, persSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).persist(StorageLevel.MEMORY_AND_DISK_SER) //个人信息
    val addDf = DBUtils.loadMysql(sqlContext, addSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).persist(StorageLevel.MEMORY_AND_DISK_SER)  //地址信息
//    val result =
      accInfDf.join(perDf,accInfDf("da")===perDf("f_user_id"),"left_outer").join(addDf,accInfDf("home_id")===addDf("ad_home_id"),"inner")
//    result.show()
    .selectExpr("da as p_user_id","substr(region_id,1,6) as p_region_id","nick_name as f_user_name", "cast(mobile_no as string) as f_user_mobile_no","f_id_card","address_name as f_user_address")
    //f_id_card,f_user_name,f_user_address,f_user_mobile_no;
  }

  /**
    * 各业务不活跃用户
    * @param sqlContext
    * @param endDate
    * @param countType
    */
  def inactiveUserByCountType(sc:SparkContext,sqlContext:HiveContext,endDate:String,countType:String): Unit = {
    //日：DAY//一周内：7DAYS//周：WEEK//30天内：30DAYS//月：MONTH
    import sqlContext.implicits._
    val regionDf = UserRegion.getRegionDf(sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val deviceNumDf = UserDeviceUtils.getDeviceNumDf(sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val personelInfDf = getUserPersonelInfo(endDate, sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER) //个人信息
    val startDate = getStartDateByType(countType,endDate)
    val countServiceList = new ListBuffer[String]() //需要统计的不活跃用户播放业务类型
    countServiceList += ((GatherType.LIVE_NEW),(GatherType.DEMAND_NEW),(GatherType.LOOK_BACK_NEW))
    val countServiceListBC = sc.broadcast[mutable.Seq[String]](countServiceList)
    val activeUserSql =
      s"""
         |select f_region_id,f_terminal,f_service_type,f_user_array
         |from bigdata.${Tables.T_BUS_ARRAY_DAY} where date >= '$startDate' and date <= '$endDate'
      """.stripMargin
    val videoPlayUserDf = sqlContext.sql(activeUserSql)
      .map(x => {
        val userListBuffer = new ListBuffer[(String,String,String,Int,String)] //userid,deviceid,regionid,terminal,servicetype
        val regionId = x.getAs[String]("f_region_id")
        val terminal = x.getAs[Int]("f_terminal")
        val serviceType = x.getAs[String]("f_service_type")
        val userArray = x.getAs[Seq[String]]("f_user_array").toArray
        for(usrArr <- userArray){
          val arr = usrArr.split("\\|")
          val userId = arr(0)
          val deviceId = try {
            arr(3)
          }catch {
            case e:Exception => "-1"
          }
          userListBuffer += ((userId,deviceId,regionId,terminal,serviceType))
        }
        userListBuffer.toList
      }).flatMap(x => x)

    val allServiceUserDf = videoPlayUserDf.map(x => {
      ((x._1,x._2,x._3,x._4),1)
    }).reduceByKey((x,y) => { //去重
      (x)
    }).map(x => {
      val allServUser = new ListBuffer[(String,String,String,Int,String)]
      val arr = x._1
      countServiceListBC.value.foreach(serviceType => {
        allServUser += ((arr._1,arr._2,arr._3,arr._4,serviceType))
      })
      allServUser.toList
    }).flatMap(x => x).toDF("f_user_id","f_device_id","f_region_id","f_terminal","f_service_type")//.repartition(partitionAmt).persist(StorageLevel.MEMORY_AND_DISK_SER)
//    println("allServiceUserDf:" + allServiceUserDf.count())
    val activeUserDf = videoPlayUserDf.map(x => {
        ((x._1,x._2,x._3,x._4,x._5),1)
      }).reduceByKey((x,y) => { //去重
      (x)
    }).map(x => {
      val arr = x._1
      (arr._1,arr._2,arr._3,arr._4,arr._5)
    }).toDF("a_user_id","a_device_id","a_region_id","a_terminal","a_service_type")//.repartition(partitionAmt).persist(StorageLevel.MEMORY_AND_DISK_SER)
//    println("activeUserDf:" + activeUserDf.count())
    val inactiveUserDf = allServiceUserDf.join(activeUserDf,allServiceUserDf("f_user_id")===activeUserDf("a_user_id")
      && allServiceUserDf("f_device_id")===activeUserDf("a_device_id")
      && allServiceUserDf("f_region_id")===activeUserDf("a_region_id")
      && allServiceUserDf("f_terminal")===activeUserDf("a_terminal")
      && allServiceUserDf("f_service_type")===activeUserDf("a_service_type"),"left_outer")
      .where("a_user_id is null")
      .selectExpr("f_user_id","f_device_id","f_region_id","f_terminal","f_service_type")
    val resultDf = inactiveUserDf.join(deviceNumDf,inactiveUserDf("f_device_id")===deviceNumDf("device_id"),"left_outer")
      .join(regionDf,inactiveUserDf("f_region_id")===regionDf("f_area_id"),"inner")
      .join(personelInfDf,inactiveUserDf("f_user_id")===personelInfDf("p_user_id")&&inactiveUserDf("f_region_id")===personelInfDf("p_region_id"),"left_outer")
      .selectExpr(s"'${startDate}' as f_date","f_user_id","device_num as f_device_num","f_province_id","f_province_name","f_city_id","f_city_name","f_region_id"
        ,"f_region_name","f_terminal","f_service_type","f_id_card","f_user_name","f_user_address","f_user_mobile_no",s"'${countType}' as f_count_type")
    try{
      val deleteSql = s"delete from ${Tables.T_INACTIVE_USERS_BY_SERVICE} where 1=1 and f_date='${startDate}' and f_count_type='${countType}'"
      DBUtils.excuteSqlPhoenix(deleteSql)
      println("成功删除重复统计数据，日期：" + endDate + ",统计类型：" + countType)
      println("将写入t_inactive_users_by_service记录数：" + resultDf.count())
      DBUtils.saveDataFrameToPhoenixNew(resultDf, Tables.T_INACTIVE_USERS_BY_SERVICE) //保存到phoenix
    }catch {
      case e:Exception => {
        println("数据保存失败！时间：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS) + ",统计类型：" + countType)
      }
    }
    regionDf.unpersist()
    deviceNumDf.unpersist()
    personelInfDf.unpersist()
//    activeUserDf.unpersist()
//    allServiceUserDf.unpersist()
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
      case "7DAYS" => s" and f_start_date='${startDate}' and f_count_type='${countType}'"
      case "WEEK" => s" and f_date='${startDate}' and f_count_type='${countType}'"
      case "30DAYS" => s" and f_start_date='${startDate}' and f_count_type='${countType}'"
      case "MONTH" => s" and f_date='${startDate}' and f_count_type='${countType}'"
      case _ => " 1=0"
    }
  }

}
