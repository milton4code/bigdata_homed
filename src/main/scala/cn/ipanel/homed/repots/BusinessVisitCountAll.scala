package cn.ipanel.homed.repots

import cn.ipanel.common.{SparkSession, Tables}
import cn.ipanel.utils.DateUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ListBuffer

/**
  * @author lizhy@20190613
  */
object BusinessVisitCountAll {
  var partitionAmt = 200
  var minVisitTime = 0
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
    println("业务访问情况(不分业务)统计开始：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    businessVisitCountAll(sparkSession, sc, hiveContext, date, partitionAmt)
    println("业务访问情况(不分业务)统计结束：" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
  }
  /**
    *
    * @param sparkSession
    * @param sc
    * @param sqlContext
    * @param date
    * @param partAmt
    */
  def businessVisitCountAll(sparkSession: SparkSession, sc: SparkContext, sqlContext: HiveContext, date: String, partAmt: Int) = {
    //区域信息
    val regionDF = BusinessVisitCount.getRegionDf(sc,sqlContext)
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
  }

  def busVisitCountByCountType(sqlContext: HiveContext, date: String, sc: SparkContext, countType: String,regionDf:DataFrame) = {
    import sqlContext.implicits._
    val countFromDate = BusinessVisitCount.getStartDateByType(countType, date)
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
//    BusinessVisitCount.saveDfToDB(regionDf,saveByServiceDf, countType) //保存分业务统计数据
    println("saveByAllServiceDf记录数：" + saveByAllServiceDf.count())
    BusinessVisitCount.saveDfToDB(regionDf,saveByAllServiceDf, countType) //此程序只保存不分业务统计数据
    sqlContext.clearCache()
  }
}
