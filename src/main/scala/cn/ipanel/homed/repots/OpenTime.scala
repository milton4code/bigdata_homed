package cn.ipanel.homed.repots
import cn.ipanel.common.{DBProperties, SparkSession, Tables}
import cn.ipanel.utils.{DBUtils, DateUtils, RegionUtils}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

case class  OpenTimeByHalfHour(f_date:String,f_hour:Int,f_range:Int,f_device_type:String,f_region_code:String,f_open_time:Float)
case class  OpenTimeByDay( f_date:String,f_device_type:String,f_region_code:String,f_open_time:Float)
case class  OpenTimeByType(f_start_date:String,f_end_date:String,f_device_type:String,f_region_code:String,f_region_name:String,f_open_time:Float)
/**
  * @author lizhy@20181028
  * 统计开机时长
  */
object OpenTime {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("OpenTimeCount")
    val sc: SparkContext = sparkSession.sparkContext
    val hiveContext: HiveContext = sparkSession.sqlContext
    var date = DateUtils.getYesterday()
    var partAmt = 200
    if(args.length != 2){
      System.err.println("请输入正确参数：统计日期,分区数")
      System.exit(-1)
    }else{
      date = args(0)
      partAmt = args(1).toInt
    }
    println("开机时长统计开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    hiveContext.sql("use bigdata")
    openTimeCount(sparkSession,sc,hiveContext,date,partAmt)
    println("开机时长统计结束："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
  }
  def openTimeCount(sparkSession: SparkSession,sc:SparkContext, sqlContext:HiveContext,date:String,partAmt: Int) = {
    //项目部署地code(省或者地市)
    val regionCode = RegionUtils.getRootRegion
    val regionBC: Broadcast[String] = sparkSession.sparkContext.broadcast(regionCode)
    //区域信息
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
    val regionDF = DBUtils.loadMysql(sqlContext, region, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    //半小时统计 //目前只统计半小时类型
    openTimeCountByHalfhour(sqlContext,date,sc,regionDF)
    /*//按天统计
    openTimeCountByDay(sqlContext,date,sc,regionDF)
    //按自然周统计
    openTimeCountByCountType(sqlContext,date,sc,"WEEK")
    //按自然月统计
    openTimeCountByCountType(sqlContext,date,sc,"MONTH")
    //按季度统计
    openTimeCountByCountType(sqlContext,date,sc,"QUARTER")
    //按自然年统计
    openTimeCountByCountType(sqlContext,date,sc,"YEAR")
    //7天内，30天内，一年内
    openTimeCountByCountType(sqlContext,date,sc,"7DAYS")
    openTimeCountByCountType(sqlContext,date,sc,"30DAYS")
    openTimeCountByCountType(sqlContext,date,sc,"1YEAR")*/
  }

  /**
    * 半小时统计
    * @param sqlContext
    * @param date
    * @param sc
    * @param regionDF
    */
  def openTimeCountByHalfhour(sqlContext:HiveContext,date:String,sc: SparkContext,regionDF:DataFrame)={
    import sqlContext.implicits._
    val daySql =
      s"""
        |select devicetype,regionid,starttime,endtime
        |from ${Tables.ORC_VIDEO_PLAY} t
        |where day = '$date'
      """.stripMargin
    val halfHourDf = sqlContext.sql(daySql)
      .map(x => {
        val openTimeBuffer = new ListBuffer[(String,Int,Int,String,String,Long)]
        val deviceType = x.getAs[String]("devicetype")
        val regionId = x.getAs[String]("regionid")
        val startTime = x.getAs[String]("starttime")
        val endTime = x.getAs[String]("endtime")
        val rangeList = getHalfHourRangeList(startTime,endTime)
        for(list <- rangeList){
          val hour = list._1
          val range = list._2
          val playTime = list._3
          openTimeBuffer += ((date,hour,range,deviceType,regionId,playTime))
        }
        openTimeBuffer.toIterator
      })
      .flatMap(x => x )
      .map(x => {
        val date = x._1
        val hour = x._2
        val range= x._3
        val deviceType = x._4
        val regionId = x._5
        val playTime = x._6
        val key = date + "," + hour + "," + range + "," + deviceType + "," + regionId
        val value = playTime
        (key,value)
      })
      .reduceByKey(_+_)
      .map(x => {
        val keyArr = x._1.split(",")
        val date = keyArr(0)
        val hour = keyArr(1).toInt
        val range = keyArr(2).toInt
        val deviceType = keyArr(3)
        val regionId = keyArr(4)
        val playTime = x._2.toFloat/3600 //小时
        OpenTimeByHalfHour(date,hour,range,deviceType,regionId,playTime)
      }).toDF()
    val joinDf = halfHourDf.join(regionDF, halfHourDf("f_region_code") === regionDF("f_area_id"), "left_outer")
      .select("f_date","f_hour","f_range","f_device_type","f_region_code","f_region_name","f_open_time")
    saveDfToDB(joinDf,"HALFHOUR")
//    DBUtils.saveDataFrameToPhoenixNew(joinDf, cn.ipanel.common.Tables.T_OPEN_TIME_HALFHOUR)
    sqlContext.clearCache()
  }

  /**
    * 按天统计
    * @param sqlContext
    * @param date
    * @param sc
    * @param regionDF
    */
    def openTimeCountByDay(sqlContext:HiveContext,date:String,sc: SparkContext,regionDF:DataFrame)={
      import sqlContext.implicits._
      val daySql =
        s"""
           |select devicetype,regionid,starttime,endtime
           |from ${Tables.ORC_VIDEO_PLAY} t
           |where day = '$date'
      """.stripMargin
      val halfHourDf = sqlContext.sql(daySql)
        .map(x => {
          val deviceType = x.getAs[String]("devicetype")
          val regionId = x.getAs[String]("regionid")
          val startTime = x.getAs[String]("starttime")
          val endTime = x.getAs[String]("endtime")
          val playTime = DateUtils.dateStrToDateTime(endTime).getSecondOfDay - DateUtils.dateStrToDateTime(startTime).getSecondOfDay //秒
          val key = date + "," +   deviceType + "," + regionId
          val value = playTime
          (key,value)
        })
        .reduceByKey(_+_)
        .map(x => {
          val keyArr = x._1.split(",")
          val date = keyArr(0)
          val deviceType = keyArr(1)
          val regionId = keyArr(2)
          val playTime = x._2.toFloat / 3600 //小时
          OpenTimeByDay(date,deviceType,regionId,playTime)
        }).toDF()
      val joinDf = halfHourDf.join(regionDF, halfHourDf("f_region_code") === regionDF("f_area_id"), "left_outer")
        .select("f_date","f_device_type","f_region_code","f_region_name","f_open_time")
//      DBUtils.saveDataFrameToPhoenixNew(joinDf, Tables.T_OPEN_TIME_DAY)
      saveDfToDB(joinDf,"DAY")
      sqlContext.clearCache()
    }
  /**
    * 按不同类型统计
    * @param sqlContext
    * @param date
    * @param sc
    * @param countType 统计类型:WEEK,MONTH,QUARTER,YEAR,7DAYS,30DAYS,1YEAR
    */
  def openTimeCountByCountType(sqlContext:HiveContext,date:String,sc: SparkContext,countType:String)={
    import sqlContext.implicits._
    val countFromDate =
      countType match{
        case "WEEK" => DateUtils.getFirstDateOfWeek(date)
        case "MONTH" => DateUtils.getFirstDateOfMonth(date)
        case "QUARTER" => DateUtils.getFirstDateOfQuarter(date)
        case "YEAR" => DateUtils.getFirstDateOfYear(date)
        case "7DAYS" => DateUtils.getDateByDays(date,7)
        case "30DAYS" => DateUtils.getDateByDays(date,30)
        case "1YEAR" => DateUtils.getDateByDays(date,365)
        case _ => "-1"
      }
    val countSql =
      s"""
         |(select f_device_type,f_region_code,f_region_name,f_open_time
         |from ${Tables.T_OPEN_TIME_DAY} t
         |where f_date between '$countFromDate' and '$date' ) as week
      """.stripMargin
    val df = DBUtils.loadDataFromPhoenix2(sqlContext, countSql)
      .map(x => {
        val deviceType = x.getAs[String]("F_DEVICE_TYPE") //需要大写
        val regionId = x.getAs[String]("F_REGION_CODE")
        val regionName = x.getAs[String]("F_REGION_NAME")
        val playTime = x.getAs[Float]("F_OPEN_TIME")
        val key = deviceType + "," +   regionId + "," + regionName
        val value = playTime
        (key,value)
      })
      .reduceByKey(_+_)
      .map(x => {
        val keyArr = x._1.split(",")
        val deviceType = keyArr(0)
        val regionId = keyArr(1)
        val regionName = keyArr(2)
        val playTime = x._2
        OpenTimeByType(countFromDate,date,deviceType,regionId,regionName,playTime)
      }).toDF()
    val joinDf = df
      .select("f_start_date","f_end_date","f_device_type","f_region_code","f_region_name","f_open_time")
    //指定类型保存到phoenix
    saveDfToDB(joinDf,countType)
    sqlContext.clearCache()
  }
  def getHalfHourRangeList(startTime:String, endTime:String, datePatern:String=DateUtils.YYYY_MM_DD_HHMMSS): ListBuffer[(Int,Int,Long)] = {
    val list = new ListBuffer[(Int,Int,Long)]
    var startDt = DateUtils.dateStrToDateTime(startTime,datePatern)
    val year = startDt.getYear
    val month =startDt.getMonthOfYear
    var tmpDate = startDt.getDayOfMonth
    val endDt = DateUtils.dateStrToDateTime(endTime,datePatern)
    while (startDt.isBefore(endDt)){
      val hour = startDt.getHourOfDay
      var minute = startDt.getMinuteOfHour
      var tmpHour = startDt.getHourOfDay
      var tmpMinute = startDt.getMinuteOfHour
      var tmpSec = 0
      if(minute >= 30){
        minute = 30
        tmpMinute = 0
        tmpHour = hour + 1
      }else{
        minute = 0
        tmpMinute = 30
      }
      if(tmpHour==24){
        tmpHour = 23
        tmpMinute = 59
        tmpSec = 59
      }
      val tmpDt = new DateTime(year,month,tmpDate,tmpHour,tmpMinute,tmpSec)
      val playTime = if(tmpDt.isBefore(endDt)){tmpDt.getSecondOfDay - startDt.getSecondOfDay}else{endDt.getSecondOfDay - startDt.getSecondOfDay} //秒
      startDt = tmpDt
      list += ((hour,minute,playTime.toLong))
    }
    list
  }
  //根据指定类型保存到phoenix不同表
  def saveDfToDB(df:DataFrame,countType:String)={
    countType match{
      case "HALFHOUR" => DBUtils.saveDataFrameToPhoenixNew(df, Tables.T_BUS_VISIT_HALFHOUR)
      case "DAY" => DBUtils.saveDataFrameToPhoenixNew(df, Tables.T_BUS_VISIT_DAY)
      case "WEEK" => DBUtils.saveDataFrameToPhoenixNew(df, Tables.T_BUS_VISIT_WEEK)
      case "MONTH" => DBUtils.saveDataFrameToPhoenixNew(df, Tables.T_BUS_VISIT_MONTH)
      case "QUARTER" => DBUtils.saveDataFrameToPhoenixNew(df, Tables.T_BUS_VISIT_QUARTER)
      case "YEAR" => DBUtils.saveDataFrameToPhoenixNew(df, Tables.T_BUS_VISIT_YEAR)
      case "7DAYS" => DBUtils.saveDataFrameToPhoenixNew(df,Tables.T_BUS_VISIT_HISTORY)
      case "30DAYS" => DBUtils.saveDataFrameToPhoenixNew(df, Tables.T_BUS_VISIT_HISTORY)
      case "1YEAR" => DBUtils.saveDataFrameToPhoenixNew(df, Tables.T_BUS_VISIT_HISTORY)
      case _ =>
    }
  }
}
