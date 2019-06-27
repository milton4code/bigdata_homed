package cn.ipanel.homed.repots

import java.util

import cn.ipanel.common._
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

/**
  * 运营整体概况统计，直播、点播、回看和时移使用次数统计指标包括
  * 访客量
  * 访客数
  * 活跃用户数
  *
  * @author lizhy@20180926
  */
case class VisitOverview(
                          f_date:String,
                          f_region_id:String,
                          f_service_type:String,
                          f_visit_time_by_chnn: Long,
                          f_visit_time_by_show:Long,
                          f_user_ids: ArrayBuffer[String]
                        )
object VisitOverview {
  var partitionAmt = 200
  var minVisitTime = 0
  /**
    *
    * @param args
    *             args(0):统计日期
    *             args(1):分区数
    *             args(2):访问时间阈值（秒），即播放低于此值不计入访问次数
    */
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("VisitOverview")
    val sc = sparkSession.sparkContext
    val hiveContext = sparkSession.sqlContext

    var date = DateUtils.getYesterday()
    //    var partitionAmt = 1
    //    var minVisitTime = 0
    if(args.length != 3){
      System.err.println("请输入正确参数：统计日期,分区数,访问时间阈值， 例如【2018-09-27】, [50] ,[5]")
      System.exit(-1)
    }
    if (args.length == 1) {
      date = DateUtils.dateStrToDateTime(args(0),DateUtils.YYYYMMDD).toString(DateUtils.YYYY_MM_DD)
    }else if(args.length == 2){
      date = DateUtils.dateStrToDateTime(args(0),DateUtils.YYYYMMDD).toString(DateUtils.YYYY_MM_DD)
      partitionAmt = args(1).toInt
    }else if(args.length == 3){
      date = DateUtils.dateStrToDateTime(args(0),DateUtils.YYYYMMDD).toString(DateUtils.YYYY_MM_DD)
      partitionAmt = args(1).toInt
      minVisitTime = args(2).toInt
    }
    println(date)
    hiveContext.sql("use bigdata")
    println("-------------运营概况统计开始：" + DateUtils.getNowDate("yyyyMMdd hh:mm:ss") + "-------------")
    visitOverview(date,hiveContext)
    println("-------------运营概况统计结束：" + DateUtils.getNowDate("yyyyMMdd hh:mm:ss") + "-------------")
  }
  def visitOverview(date: String, hiveContext: HiveContext):Unit = {
    import hiveContext.implicits._
    val playDf = hiveContext.sql(
      s"""
         |select userid,deviceid,devicetype,regionid,playtype,serviceid,starttime,endtime,playtime
         |from ${Tables.ORC_VIDEO_PLAY} t
         |where day = regexp_replace('$date','-','')
      """.stripMargin
    )
    //    playDf.show(10)
    //计算围度下带有频道访问次数的用户列表
    val chnnTimeDf = playDf.map(x =>{
      val userId = x.getAs[String]("userid")
      //        val deviceId = x.getAs[String]("deviceid")
      //        val deviceType = x.getAs[String]("devicetype")
      val regionId = x.getAs[String]("regionid")
      var playType = x.getAs[String]("playtype")
      if(playType == GatherType.one_key_timeshift){
        playType = GatherType.TIME_SHIFT_NEW
      }
      val serviceId = x.getAs[Long]("serviceid")
      val playTime = x.getAs[Long]("playtime")
      var visitTimeByChnn = 1
      if(playTime < minVisitTime){ //如果小于阈值，则不计入访问次数
        visitTimeByChnn = 0
      }
      val key = regionId + "," + userId + "," + playType
      (key, visitTimeByChnn)
    }
    ).reduceByKey(_+_)
    //    chnnTimeDf.toDF().show(10)
    //获取节目播放列表
    val programDf = getProgramDf(date,hiveContext).registerTempTable("t_program_tmp")
    hiveContext.sql("cache table t_program_tmp")
    playDf.repartition(partitionAmt).registerTempTable("t_play_tmp")
    //    hiveContext.sql("select * from t_program_tmp").show(10)
    //计算节目访问次数
    /**
      * 需关联有交集节目
      */
    val progNChnnDf = hiveContext.sql(
      s"""
         |select pl.userid,pl.regionid,pl.playtype,pl.serviceid,pl.playtime,pt.event_id
         |from t_play_tmp pl
         |left join t_program_tmp pt
         | on pl.serviceid = pt.homed_service_id
         | and (pt.start_time between pl.starttime and pl.endtime
         | or pl.starttime between pt.start_time and pt.end_time
         | )
         |where pl.playtype = '${GatherType.LIVE_NEW}'
         |union all
         |select pl.userid,pl.regionid,pl.playtype,pl.serviceid,pl.playtime,'' as event_id
         |from t_play_tmp pl
         |where pl.playtype != '${GatherType.LIVE_NEW}'
       """.stripMargin
    )
      .map(x => {
        val userId = x.getAs[String]("userid")
        //        val deviceId = x.getAs[String]("deviceid")
        val regionId = x.getAs[String]("regionid")
        val playType = x.getAs[String]("playtype")
        //        val serviceId = x.getAs[Int]("serviceid")
        val playTime = x.getAs[Long]("playtime")
        var visitTimeByShow = 1
        if(playTime < minVisitTime){ //如果小于阈值，则不计入访问次数
          visitTimeByShow = 0
        }
        val key = regionId + "," + userId + "," + playType
        val value =  visitTimeByShow
        (key, value)
      }).reduceByKey(_+_)
      .join(chnnTimeDf)
      .map(x => {
        val keyArr = x._1.split(",")
        val regionId = keyArr(0)
        val userId = keyArr(1)
        val playType = keyArr(2)
        val visitTimeByChnn = x._2._2
        val visitTimeByShow = x._2._1
        val userList = new ArrayBuffer[String]()
        userList.append(userId + "_" + visitTimeByChnn + "_" + visitTimeByShow) //使用"_"依次连接用户ID、频道访问次数、节目访问次数
        val key = regionId + "," + playType
        val value = (userList,visitTimeByChnn.toLong,visitTimeByShow.toLong)
        (key,value)
      })
      .reduceByKey((x,y)=> {
        (x._1 ++ y._1,x._2 + y._2,x._3 + y._3)
      })
      .map(x => {
        val keyArr = x._1.split(",")
        val regionId = keyArr(0)
        var serviceType = keyArr(1)
        val value: (ArrayBuffer[String], Long, Long) = x._2
        val visitTimeByChnn = value._2
        val visitTimeByShow = value._3
        val userList = value._1
        if(serviceType == null){
          serviceType = "unknown"
        }
        new VisitOverview(date,regionId,serviceType,visitTimeByChnn,visitTimeByShow,userList)
      })
      .toDF()
    //    progNChnnDf.show(10)
    DBUtils.saveDataFrameToPhoenixNew(progNChnnDf, Tables.t_VISIT_OVERVIEW) //保存到phoenix表
  }
  def getProgramDf(date:String,sqlContext:HiveContext): DataFrame= {
    val startTime = DateUtils.getStartTimeOfDay(date.replace("-",""))
    val endDate = DateUtils.getEndTimeOfDay(date.replace("-",""))
    val sql =
      s"""
         |( select a.homed_service_id,a.event_id ,a.event_name,
         |a.f_content_type,a.start_time,
         |date_add(a.start_time,interval a.duration hour_second) as end_time,
         |time_to_sec(a.duration) as duration,b.chinese_name
         |from ${Tables.T_PROGRAM_SCHEDULE} a
         |left join
         |${Tables.T_CHANNEL_STORE} b
         |on a.homed_service_id = b.channel_id
         |where a.start_time>='$startTime' and a.start_time<='$endDate') as aa
      """.stripMargin
    val liveProgramDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    liveProgramDF
  }
}
