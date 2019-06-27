package cn.ipanel.etl

import java.util.regex.Pattern

import cn.ipanel.common.{CluserProperties, Constant, GatherType, SparkSession}
import cn.ipanel.customization.hunan.etl.HNLogParser
import cn.ipanel.utils._
import jodd.util.StringUtil
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * 用户开机时长补丁程序
  * 主要统计
  * create  liujjy
  * time    2019-05-10 0010 14:30
  */
@Deprecated
object OnlineTimePatch {
  val SPLIT = ","

  //湖南日志上报中 用户id 和设备id 分别上报的是 ca 和
  def main(args: Array[String]): Unit = {
    val time = new LogUtils.Stopwatch("OnlineTimePatch 用户开机时长补丁程序 ")
    if (args.length != 3) {
      println(
        """
          |请输入参数 日期,分区数, [日志上报DA字段是否需要转换] :例如
          |20180919 30 ,ture
          |-----------------------------------------------------------
          |--日期 20180919
          |--分区数 30
          |--日志上报DA字段是否需要转换
          |   -true 表示用户上报日志中用户id字段为CA,需要进行转换
          |   -false 表示用户上报日志正常,则不需要转换
        """.stripMargin)
      sys.exit(-1)
    }

    val day = args(0).toString
    val partionNums = args(1).toInt
    val switch = args(2).toBoolean
    val nextDay = DateUtils.getAfterDay(day, targetFormat = DateUtils.YYYY_MM_DD)

    val session = new SparkSession("OnlineTimePatch")
    val sc = session.sparkContext
    val sqlContext = session.sqlContext

    val path = LogConstant.HDFS_ARATE_LOG + day + "/*"
//        val path = "file:///D:\\TMP\\hn\\12.log"
    //        val path = "file:///D:\\TMP\\ks\\ks.log"

    val textFile = sc.textFile(path)
    sqlContext.udf.register("deviceType", DeviceUtils.deviceIdMapToDeviceType _)

    import sqlContext.implicits._
    //<?><[0701,1556899132202,8746002235604418,-1,0780010018500000564]>
    val base = textFile.filter(x => x.startsWith("<?>") && StringUtil.isNotEmpty(x))
      .map(log => {
        val tmp = log.substring(3).replace("<", "").replace(">", "")
          .replace("[", "").replace("]", "")
          .replace("(", "").replace(")", "")
        trimSpace(tmp)
      })
      .map(x => {
        val log = x.split("\\|")
        tranform(log(0), day)
      }).filter(x => {
      StringUtil.isNotEmpty(x.userid) && !x.userid.equalsIgnoreCase("null") && !GatherType.SYSTEM_STANDBY.equals(x.service.toString) &&
        x.deviceid != ""
    }) toDF()

    val userDeviceDF = UserUtils.getHomedUserDevice(sqlContext, nextDay)
    userDeviceDF.persist()

    val defaultRegion = RegionUtils.getDefaultRegion()

    if (switch) {
      CluserProperties.REGION_CODE match {
        case Constant.HUNAN_CODE =>
           HNLogParser.getVaidateTimerDF(userDeviceDF,base,sqlContext,defaultRegion) .registerTempTable("t_log")
        case _ =>
           tranformCaDA(base,userDeviceDF,sqlContext,defaultRegion)
      }

    } else {
      base.selectExpr("service", "timehep", "userid", s"if(length(region)<6, $defaultRegion,region) region",
        "deviceType(deviceid) terminal", "deviceid")
        .registerTempTable("t_log")
    }

    val df = sqlContext.sql(
      s"""
         |select
         |  service1 as servicetype,
         |  from_unixtime (timehep1/1000) as starttime,
         |  case when t2.service2 = ${GatherType.SYSTEM_OPEN} then from_unixtime (timehep1/1000)
         |    else from_unixtime (timehep2/1000) end as endtime,
         |  case when t2.service2 = ${GatherType.SYSTEM_OPEN} then 0 else (t2.timehep2-t2.timehep1)/1000 end as playtime,
         |  t2.userid,t2.region as   regionid,t2.terminal as terminal,t2.deviceid
         |from
         |(
         |   SELECT
         |     t1.service1,t1.service2,
         |     t1.timehep AS timehep1,
         |     lead (t1.timehep, 1, t1.timehep) over (PARTITION BY t1.deviceid 	ORDER BY t1.timehep ) AS timehep2,
         |     t1.first_time,t1.userid,t1.region,t1.terminal,t1.deviceid
         |   from
         |   (
         |      select service as service1,
         |        lead(service,1,"0") over(partition by deviceid order by timehep) as service2, timehep,
         |        first_value(timehep) over(partition by userid,deviceid order by timehep) as first_time,
         |        userid,region,terminal,deviceid
         |      from t_log
         |   )t1
         |   where
         |    (t1.service1 = ${GatherType.SYSTEM_HEARTBEAT} and t1.timehep=first_time )
         |    or (t1.service1 = ${GatherType.SYSTEM_HEARTBEAT} and t1.service2="0" )
         |    or (t1.service1 = ${GatherType.SYSTEM_HEARTBEAT} and t1.service2 = ${GatherType.SYSTEM_OPEN})
         |) t2
         |where (t2.service1 <> ${GatherType.SYSTEM_HEARTBEAT}  )  or t2.timehep1 = t2.first_time
      """.stripMargin)


    //再次关联区域,防止某些地市区域上报错误
    val homedUserDF = userDeviceDF.selectExpr("user_id as userid", "device_id deviceid", "region_id regionid"
                      , "device_type terminal")

    df.alias("a").join(homedUserDF.alias("t"), Seq("userid"))
      .selectExpr("a.userid", "a.servicetype", "nvl(t.deviceid,a.deviceid) deviceid",
        "nvl(t.terminal,a.terminal) terminal", "t.regionid as regionid",
        "starttime", "endtime", "playtime", s" '$day' as day")
      .filter("playtime > 0 ")
      .distinct()
      .repartition(partionNums)
      .write.format("orc").mode(SaveMode.Overwrite).partitionBy("day")
      .insertInto("bigdata.orc_online_time")

    userDeviceDF.unpersist()
    println(time)
  }


  private def tranformCaDA(base:DataFrame,userDeviceDF:DataFrame,hiveContext: SQLContext,defaultRegion:String): Unit ={
    import hiveContext.implicits._
    val userRegionDF = userDeviceDF.selectExpr("user_id", "region_id", "device_id", "device_type", "uniq_id")

    base.alias("a")
      .join(userRegionDF.alias("c"), $"a.userid" === $"c.uniq_id")
      .selectExpr("a.service", "a.timehep", "c.user_id userid", s"nvl(c.region_id,$defaultRegion) region",
        "nvl(c.device_type ,deviceType(a.deviceid)) terminal", "c.device_id deviceid")
      .registerTempTable("t_log")
  }

  private case class Log(service: Int, timehep: Long = 0L, userid: String = "", region: String, terminal: String = "", deviceid: String = "")

  /*
     1.处理日志中的时间上报问题
     2.过滤上报日志格式错误数据
   */
  private def tranform(data: String, day: String): Log = {
    var log = Log(0, 0L, "", "")
    val datas = data.split(SPLIT)
    if (datas.length == 5) {
      if (!"undefined".equals(datas(1)) && !"undefined".equals(datas(0))) {
        val reprotTimeStamp = getTime(datas(1), day)
        log = Log(datas(0).toInt, reprotTimeStamp, datas(2), datas(3), "", datas(4)).copy()
      }
    } else if (datas.length == 4) {
      if (datas(1) != "undefined" && datas(0) != "undefined") {
        val reprotTimeStamp = getTime(datas(1), day)
        log = Log(datas(0).toInt, reprotTimeStamp, datas(2), datas(3)).copy()
      }
    }
    log
  }

  //处理上报时间异常问题，
  // 对上报时间小于指定日期开始时间,则赋默认值指定日期开始时间 ；或者大于指定日期结束时间 则赋默认值指定日期结束时间
  private def getTime(timeStamp: String, day: String): Long = {
    val defaultDateTime = DateUtils.dateStrToDateTime(day, DateUtils.YYYYMMDD)
    val startTimpstamp = defaultDateTime.getMillis
    val endTimpstamp = defaultDateTime.plusDays(1).plusSeconds(-1).getMillis
    var reportTimestamp = startTimpstamp
    try {
      reportTimestamp = timeStamp.toLong
      if (reportTimestamp <= startTimpstamp) {
        reportTimestamp = startTimpstamp
      } else if (reportTimestamp >= endTimpstamp) {
        reportTimestamp = endTimpstamp
      }
    } catch {
      case ex: Exception => ex.printStackTrace()
    }

    reportTimestamp
  }


  //去空格
  private def trimSpace(str: String): String = {
    var dest = ""
    if (StringUtil.isNotEmpty(str)) {
      val p = Pattern.compile("\\s*|\t|\r|\n")
      val m = p.matcher(str)
      dest = m.replaceAll("")
    }
    dest
  }


}
