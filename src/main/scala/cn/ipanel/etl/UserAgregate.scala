package cn.ipanel.etl

import cn.ipanel.common._
import cn.ipanel.utils.LogUtils.divideTime
import cn.ipanel.utils.{DateUtils, DeviceUtils, LogUtils, RegionUtils, UserUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable.ListBuffer

/**
  * 用户聚合处理
  * 1.将用户上报,推流日志,NGINX,websoket日志中的用户数据进行聚合气起来
  * 2.按照默认时间刻度将数据保存
  * 3.按照唯一标示,步骤2中的数据去重,保存起来
  * create  liujjy
  * time    2019-04-30 0030 15:16
  */

object UserAgregate {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("请输入有效日期,分区数 例如[20180401] [200] ")
      System.exit(-1)
    }
    val session = SparkSession("UserAgregate")
    val hiveContext = session.sqlContext

    val day = args(0)
    val partitions = args(1).toInt

    process(day,partitions,hiveContext)

  }

  /**
    * 用户聚合功能,主要根据提取所有有效用户
    * @param day   yyyyMMdd
    * @param partitions  分区数
    * @param hiveContext hiveContext
    * @return
    */
  def process(day:String, partitions :Int=10, hiveContext: HiveContext):Unit = {
    val time = new LogUtils.Stopwatch("UserAgregate 用户聚合处理 ")

    val nextDay = DateUtils.getAfterDay(day, targetFormat = DateUtils.YYYY_MM_DD)
    var resources = CluserProperties.LOG_SOURCE.trim.split(",").toBuffer

    println("===日志源=====" + CluserProperties.LOG_SOURCE)
    var listBuffer = new ListBuffer[DataFrame]()
    for (index <- resources.indices) {
      resources(index).trim match {
        case SourceType.USER_REPORT =>
          val df = processUserBehaviorLog(day,SourceType.USER_REPORT ,hiveContext)
          val count = df.count()
          println("processReportPlayLog===> " + count)
          if (count > 0)
            listBuffer.append(df)


        case SourceType.RUN_LOG =>
          val df = processRunPlayLog(day, hiveContext)
          val df1 = processUserBehaviorLog(day,SourceType.RUN_LOG,hiveContext)
          val count = df1.count()
          println("processRunPlayLog===> " + count)
          if (count == 0){
            listBuffer.append(df)
          }else{
            listBuffer.append( df.unionAll(df1).distinct())
          }


        case SourceType.NGINX =>
          val df = processNginxLog(day, hiveContext)
          val count = df.count()
          println("processNginxLog===> " + count)
          if (count > 0)
            listBuffer.append(df)

        case SourceType.WEBSOCKET =>
          val df = processWebsoket(day, hiveContext)
          val count = df.selectExpr("device_id").distinct().count()
          //          val count = df.count()
          println("processWebsoket=device==> " + count)
          if (count > 0)
            listBuffer.append(df)

        case _ =>
      }
    }

    println("listBuffer=====size=====>" + listBuffer.size)
    var _unionDF: DataFrame = null
    if (listBuffer.size == 1) {
      _unionDF = listBuffer.head
    } else if(listBuffer.size > 1){
      _unionDF = listBuffer.reduce((x1, x2) => x1.unionAll(x2))
    }
    val unionDF = _unionDF.dropDuplicates(Seq("user_id", "device_id", "device_type", "hour", "range", "source", "action", "region_id"))

    CluserProperties.REGION_CODE match {
      case Constant.HUNAN_CODE =>  //湖南定制,根据上报301过滤用户
        val day = DateUtils.dateStrToDateTime(nextDay, DateUtils.YYYY_MM_DD).toString(DateUtils.YYYYMMDD)
        val df = hiveContext.sql(s"select user_id ,device_id from user.t_user where day <='$day'").distinct()

        unionDF.join(df, Seq("user_id", "device_id")).registerTempTable("t1")

      case _ => unionDF.registerTempTable("t1")
    }


    val allLogDF = hiveContext.sql(
      """
        |select min(report_time) report_time,user_id,device_id,device_type,region_id,hour,range, uniq_id,
        |concat_ws('|',collect_set(source))  as source,
        |concat_ws('|',collect_set(action))  as action
        |from t1
        |group by user_id, device_id, device_type, region_id,hour, range,uniq_id
      """.stripMargin)


    val homedUserDF = UserUtils.getHomedUserDevice(hiveContext, nextDay).selectExpr("user_id", "device_id")


    //区域信息
    val regionDF = RegionUtils.getRegions(hiveContext)
    //    设备及唯一ID 对应关系
    val deviceCaInfo = DeviceUtils.getDeviceCaInfo(hiveContext, nextDay).selectExpr("device_id", "uniq_id")

    val userDetailsDF = allLogDF.alias("a").join(regionDF.alias("b"), Seq("region_id"))
      .join(deviceCaInfo.alias("c"), Seq("device_id"), "left")
      .selectExpr("report_time", "user_id", "device_id", "device_type"
        , "province_id", "province_name", "city_id", "city_name", "region_id", "region_name"
        , "hour", "range", "c.uniq_id", "source", "action", s" '$day' as day")
      .na.fill("")
      .filter("device_id != 0")

    val detailsDF = userDetailsDF.join(homedUserDF, Seq("device_id", "user_id"), "inner")
      .selectExpr("report_time", "user_id", "device_id", "device_type"
        , "province_id", "province_name", "city_id", "city_name", "region_id", "region_name"
        , "hour", "range", "uniq_id", "source", "action", s" '$day' as day")
    detailsDF.persist().registerTempTable("t2")


    hiveContext.udf.register("merge", (source: String) => {
      if (source.nonEmpty) {
        source.split("\\|").toSet.mkString("|")
      } else ""
    })


    val distinctUserDF = hiveContext.sql(
      s"""
         |select min(report_time) report_time,user_id,device_id,device_type,
         |province_id,province_name,city_id,city_name,region_id,region_name,uniq_id,
         | concat_ws('|',collect_set(source))  as source,
         | concat_ws('|',collect_set(action))  as action,
         |'$day' as day
         |from t2
         |group by user_id, device_id, device_type, province_id,province_name,city_id,city_name,region_id,region_name,uniq_id
      """.stripMargin)
      .selectExpr("report_time", "user_id", "device_id", "device_type"
        , "province_id", "province_name", "city_id", "city_name", "region_id", "region_name"
        , "uniq_id", "merge(source) as source", " merge(action) as action", s" '$day' as day")



    //-----------------测试代码


    //    detailsDF.show(false)
    //    unionDF.selectExpr("count(distinct(device_id)) unionDF ").show()

    //    val data0E = DateUtils.getNowDate("yyyyMMdd-HHmmss")


    //     t_user_details.repartition(10)
    //      .write.mode(SaveMode.Overwrite)
    //      .saveAsTable("t_user_details")

    //val t_user_details = unionDF.dropDuplicates(Seq("user_id","device_id"))
    //    t_user_details.map(_.toString().replace("[","").replace("]",""))
    //        .repartition(1).saveAsTextFile(s"/tmp/details/$data0E")


    //    homedUserDF.selectExpr("count(distinct(device_id)) homedUserDF ").show()
    //    userDetailsDF.selectExpr("count(distinct(device_id)) userDetailsDF ").show()
    //    detailsDF.selectExpr("count(distinct(device_id)) detailsDF ").show()

    //    val t_user_diff = userDetailsDF.selectExpr("user_id", "device_id").distinct()
    //      .except(
    //        homedUserDF.selectExpr("user_id", "device_id").distinct()
    //      ).join(userDetailsDF, Seq("user_id", "device_id"))

    //
    //    t_user_diff.repartition(10).write.mode(SaveMode.Overwrite)
    //      .saveAsTable("t_user_diff")
    //
    //    t_user_diff.map(_.toString().replace("[", "").replace("]", ""))
    //      .repartition(1).saveAsTextFile(s"/tmp/t_user_diff/$data0E")

    //----------------------------


    hiveContext.sql("use bigdata")
    detailsDF.repartition(partitions).write.format("orc").mode(SaveMode.Overwrite).partitionBy("day")
      .insertInto("orc_user_details")
    distinctUserDF.repartition(partitions).write.format("orc").mode(SaveMode.Overwrite).partitionBy("day")
      .insertInto("orc_user")

    detailsDF.unpersist()

    println(time)
  }

  def processWebsoket(day: String, hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    val _date = DateUtils.dateStrToDateTime(day, DateUtils.YYYYMMDD)
    val date = _date.toString(DateUtils.YYYY_MM_DD_HHMMSS)
    val date2 = _date.toString(DateUtils.YYYY_MM_DD)
    hiveContext.sql(
      s"""
         |SELECT f_user_id,f_device_id,f_device_type,f_region_id,
         |if(cast(f_login_time as string) >= '$date', cast(f_login_time as string),'$date') f_login_time ,
         |cast(f_logout_time as string) f_logout_time
         |from bigdata.t_user_online_history
         |where day='$day'
       """.stripMargin)
      .flatMap(row => {
        val userId = row.getAs[Long]("f_user_id").toString
        val deviceId = row.getAs[Long]("f_device_id")
        val regionId = row.getAs[String]("f_region_id")
        val deviceType = row.getAs[Long]("f_device_type").toString
        val starttime = row.getAs[String]("f_login_time")
        val endtime = row.getAs[String]("f_logout_time")
        val uniq_id = userId + "_" + regionId
        val times = divideTime(date2, starttime, endtime)
        times.map(x => {
          Log(userId, deviceId, deviceType, regionId, starttime, x._1, x._2, SourceType.WEBSOCKET, "", uniq_id)
        })
      }).toDF()

  }


  /**
    * 用户上报中播放行为数据
    */
  def processReportPlayLog(day: String, hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    val day2 = DateUtils.transformDateStr(day)

    val filter = CluserProperties.REGION_CODE match {
      case Constant.HUNAN_CODE => s" playtype='${GatherType.SYSTEM_OPEN}'"
      case _ => " 1=1 "
    }

    hiveContext.sql(
      s"""
         |select userid,deviceid,devicetype,regionid,starttime,endtime ,playtype from bigdata.orc_video_report where day='$day'
      """.stripMargin)
      .filter(filter)
      .flatMap(row => {
        val userId = row.getAs[String]("userid")
        val deviceId = row.getAs[Long]("deviceid")
        val regionId = row.getAs[String]("regionid")
        val deviceType = row.getAs[String]("devicetype")
        val starttime = row.getAs[String]("starttime")
        val endtime = row.getAs[String]("endtime")
        val playtype = row.getAs[String]("playtype") + s"_${SourceType.USER_REPORT}"
        val uniq_id = userId + "_" + regionId
        val times = divideTime(day2, starttime, endtime)
        times.map(x => {
          Log(userId, deviceId, deviceType, regionId, starttime, x._1, x._2, SourceType.USER_REPORT, playtype, uniq_id)
        })
      }).toDF()
  }


  //select userid,deviceid,devicetype,regionid,starttime,endtime from orc_video_play where day=20190111
  /**
    * run日志中的播放行为日志
    */
  def processRunPlayLog(day: String, hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    val day2 = DateUtils.transformDateStr(day)
    hiveContext.sql(
      s"""
         |select userid,deviceid,devicetype,regionid,starttime,endtime ,playtype from bigdata.orc_video_play_tmp where day='$day'
      """.stripMargin)
      .flatMap(row => {
        val userId = row.getAs[String]("userid")
        val deviceId = row.getAs[Long]("deviceid")
        val regionId = row.getAs[String]("regionid")
        val deviceType = row.getAs[String]("devicetype")
        val starttime = row.getAs[String]("starttime")
        val endtime = row.getAs[String]("endtime")
        val playtype = row.getAs[String]("playtype")
        val uniq_id = userId + "_" + regionId
        val times = divideTime(day2, starttime, endtime)
        times.map(x => Log(userId, deviceId, deviceType, regionId, starttime, x._1, x._2, SourceType.RUN_LOG, playtype, uniq_id)
        )
      }).toDF()

  }


  //处理nginx日志
  def processNginxLog(day: String, hiveContext: HiveContext):DataFrame = {
    import hiveContext.implicits._
    //读取nginx日志
    hiveContext.sql(
      s"""
         |select report_time,params['accesstoken'],key_word  token from bigdata.orc_nginx_log where day='$day'
      """.stripMargin)
      .map(row => {
//        val listBuffer = new ListBuffer[Log]()
//        while (it.hasNext) {
//          val row = it.next()
          val reportTime = row.getString(0)
          val (hour, range) = bombTime(reportTime)
          val user: User2 = TokenParser.parserAsUser(row.getString(1))
          val key_word = row.getString(2)
          val uniq_id = user.DA + "_" + user.region_id
        Log(user.DA, user.device_id, user.device_type, user.region_id, reportTime, hour, range,
          SourceType.NGINX, key_word, uniq_id)
//          listBuffer.append(Log(user.DA, user.device_id, user.device_type, user.region_id, reportTime, hour, range,
//            SourceType.NGINX, key_word, uniq_id))
//        }
//        listBuffer.iterator
      }).toDF()
      .filter("user_id <> '' ")
  }


  /**
    * 用户行为日志 包含用户上报
    */
  def processUserBehaviorLog(day: String,sourceType: String, hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._

    var queryTable = ""
    var filter = ""

    sourceType match{
      case SourceType.USER_REPORT =>
          queryTable = "bigdata.orc_report_behavior"
          filter = s" reporttype in ('${GatherType.SYSTEM_OPEN}','${GatherType.SYSTEM_HEARTBEAT}' )"

      case _ =>
         queryTable = "bigdata.orc_user_behavior"
         filter = "1=1"


    }

    println("queryTable==========>" + queryTable)
    val tmp = hiveContext.sql(
      s"""
         |select userid,deviceid,regionid, devicetype, reporttime,exts['logsouce'] logsource,reporttype ,reporttype ,ca
         |from $queryTable
         |where day='$day'
      """.stripMargin)

    val df = tmp.filter(filter)
      .distinct()
      .map(row => {
        val userId = row.getAs[String]("userid")
        val deviceId = row.getAs[Long]("deviceid")
        val regionId = row.getAs[String]("regionid")
        val deviceType = row.getAs[String]("devicetype")
        val reportTime = row.getAs[String]("reporttime")
        val reporttype = row.getAs[String]("reporttype")
        val logSource = row.getAs[String]("logsource")
        val (hour, range) = bombTime(reportTime)
        //        val uniq_id = userId + "_" + regionId
        val uniq_id = row.getAs[String]("ca")
        Log(userId, deviceId, deviceType, regionId, reportTime, hour, range, logSource, reporttype, uniq_id)
      }).toDF()
      .dropDuplicates(Seq("user_id", "device_id", "hour", "range", "source", "action", "uniq_id"))
      .repartition()

     if( SourceType.USER_REPORT.equals(sourceType)) {
        val caDF = hiveContext.sql(s"select user_id,device_id from  user.t_user where day <= '$day'").distinct()

//        //------------------验证代码
//        val regionDF = RegionUtils.getRegions(hiveContext).selectExpr("city_name","region_name","region_id")
//
//        regionDF.show()
//
//        // private case class Log(user_id: String, device_id: Long, device_type: String, region_id: String,
//        //                         report_time: String, hour: Int, range: Int, source: String, action: String, uniq_id: String)
//        val _701DF = tmp.filter("reporttype='701'")
//          .selectExpr("deviceid as device_id", "ca uniq_id","regionid region_id","reporttime report_time")
//          .dropDuplicates(Seq("uniq_id"))
//        val data0E = DateUtils.getNowDate("yyyyMMdd-HHmm")
//        val _301DF = tmp.filter("reporttype='301'")
//          .selectExpr("deviceid as device_id", "ca uniq_id","regionid region_id","reporttime report_time")
//            .dropDuplicates(Seq("uniq_id"))
//
//        _701DF.alias("a").except(_301DF.alias("b")).join(caDF, Seq("device_id"))
//          .selectExpr("a.*")
//          .join(regionDF,Seq("region_id"))
//          .map(_.toString().replace("[", "").replace("]", ""))
//          .repartition(1)
//          .saveAsTextFile(s"/tmp/t_701_diff/$data0E")
//
//
//          _301DF.distinct()
//           .map(_.toString().replace("[", "").replace("]", ""))
//          .repartition(1)
//          .saveAsTextFile(s"/tmp/t_301_diff/$data0E")
////        //------------------验证代码结束

        df.alias("a").join(caDF.alias("b"), Seq("user_id", "device_id")).selectExpr("a.*")
    }else{
       df
    }

  }

  //时间分片
  private def bombTime(time: String) = {
    val dateTime = DateUtils.dateStrToDateTime(time)
    val hour = dateTime.getHourOfDay
    val minute = dateTime.getMinuteOfHour
    val range = if (minute >= 0 && minute < 30) 30 else 60
    (hour, range)
  }

  private case class Log(user_id: String, device_id: Long, device_type: String, region_id: String,
                         report_time: String, hour: Int, range: Int, source: String, action: String, uniq_id: String)

}
