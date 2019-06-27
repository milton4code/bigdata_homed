package cn.ipanel.etl

import java.util.regex.Pattern

import cn.ipanel.common._
import cn.ipanel.customization.hunan.etl.HNLogParser
import cn.ipanel.customization.wuhu.etl.LogProcess
import cn.ipanel.etl.LogParser.getUserRegion
import cn.ipanel.utils.{DBUtils, DateUtils, LogUtils, RegionUtils}
import jodd.util.StringUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable.{HashMap => scalaHashMap}

//指定日期数
//上报日志
/**
  * 上报日志解析
  *
  */
object ReportLog {
  private lazy val SPLIT = ","
  private lazy val BYTE_SIZE = 1024
  private lazy val LOGGER = LoggerFactory.getLogger(ReportLog.getClass)
  val clusterRegion = RegionUtils.getRootRegion //配置文件中所配置的地方项目区域码 added @20190421

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println(
        """
          |请输入参数 日期,分区数, [ca da转换标识] ,[定制化版本标示]:例如
          |20180919 30 ,ture, false
          |-----------------------------------------------------------
          |ca da转换标识
          | -true 表示用户上报日志中用户id字段为CA,需要进行转换
          | -false 表示用户上报日志正常,则不需要转换
          |定制化版本标示
          | -true  代表湖南专用
          | -false 其他地方
          |
        """.stripMargin)
      sys.exit(-1)
    }

    val day = args(0)
    val partionNums = args(1).toInt
    //true  日志版本 用户id为CA卡号
    //false 日志版本 用户id为正常DA
    val version = args(2).toBoolean
    //true 日志版本   上报格式  播放10秒后上报
    //false 日志版本  播放日志 点击一条 播放一条
    val version1 = args(3).toBoolean
    val session = new SparkSession("ReportLog")
    val sc = session.sparkContext
    val sqlContext = session.sqlContext
    var regionCode = RegionUtils.getRootRegion
    val defaultCode = RegionUtils.getRootRegion
    //默认区域码
    var regionPro = "" //省标识
    if (regionCode.endsWith("0000")) {
      regionCode = (regionCode.toInt + 101).toString //默认市的默认区
      regionPro = regionCode.substring(0, 2)
    } else {
      regionCode = (regionCode.toInt + 1).toString //默认区
      regionPro = regionCode.substring(0, 4)
    }
    val path = LogConstant.HDFS_ARATE_LOG + day + "/*"
    //val path = "file:///C:\\Users\\Administrator\\Desktop\\aaaa"
    val rCode = session.sparkContext.broadcast(regionCode)
    val textFile = sc.textFile(path)
    //直播中点赞，分享等行为码
    //val liveFilterBr  =
    // sc.broadcast(Array("2","3","4","11","12","13","14","18","19","28","29"))
    //<?><[0101,1537951414577,50310979,0,1005358791]><|><(T,live)><&><(P,http)><&><(ID,4200851466)><&><(VC,1005358791)><&><(CL,100)><&><(A,1)><&><(NO,0)><&><(NA,山东卫视)><&><(PA,0)>
    import sqlContext.implicits._
    val ds1 = textFile.filter(x => x.startsWith("<?>") && StringUtil.isNotEmpty(x))
      .map(log => {
        // val tmp = log.substring(3).replace("<", "").replace(">", "").replace("[", "").replace("]", "").replace("(", "").replace(")", "")
        val tmp = log.substring(3) //.replace("<", "").replace(">", "").replace("[", "").replace("]", "").replace("(", "").replace(")", "")
        trimSpace(tmp)
      })
    CaGetDa.fromCaGetDa(sqlContext: HiveContext,partionNums)
    if (version) {
      val ds2 = ds1.map(line => {
        var log = Log(0, 0L, "", "", "", "", new scalaHashMap[String, String]())
        if (line.contains("|")) {
          //有拓展内容
          val spilts = line.split("\\|", 2) //设备相关数据|拓展数据
          if (spilts.length > 1) { //修复日志中出现 "|" 拓展数据为空的 出现角标越界异常
            val base = spilts(0).replace("<", "").replace(">", "")
              .replace("[", "").replace("]", "")
              .replace("(", "").replace(")", "")
            val userDeviceInfo = tranform(base, version, day)
            if (!userDeviceInfo.terminal.equals("unknown")) {
              val ext = spilts(1).replace("<", "").replace(">", "")
                .replace("[", "").replace("]", "")
                .replace("(", "").replace(")", "")
              log = userDeviceInfo.copy(paras = LogUtils.str_to_map_new(ext, "&", ",", SourceType.USER_REPORT))
            }
          }
        } else { //务拓展内容
          log = tranform(line.replace("<", "").replace(">", "").replace("[", "").replace("]", "").replace("(", "").replace(")", ""), version, day)
        }
        log
      }).filter(x => x.userid != "0" && x.userid != "null" && !GatherType.SYSTEM_STANDBY.equals(x.service.toString)
        && x.userid != "" && x.userid != "NULL")
        .toDF()
      sqlContext.sql("use bigdata")
      val cadf = sqlContext.sql(
        """
          |select deviceid as device_id,DA,ca_id from t_ca_device
        """.stripMargin)

      val ds4: DataFrame = defaultCode match {
        case Constant.HUNAN_CODE =>
          //湖南需要过滤掉凤凰高清盒子用户，判断依据就是第三个字段上报的是DA
          HNLogParser.fiterHdUser(sqlContext, ds2, cadf)
        case _ =>
          ds2
      }
      val user = ds4.filter(
          ds4.col("userid") >= "50000000" && ds4.col("userid") <= "70000000"
      )
      val userdf = user.repartition(partionNums)
        .join(cadf, cadf("DA") === user("userid"))
        .select("service", "timehep", "userid", "device_id", "paras", "ca_id", "terminal")
      val ds3 = ds4.except(user).repartition(partionNums)
        .join(cadf, cadf("ca_id") === ds2("userid"))
        .select("service", "timehep", "DA", "device_id", "paras", "ca_id", "terminal")
      val ds = ds3.unionAll(userdf)
      println("ds.count:" + ds.count())
      val region = getUserRegion(regionCode, sqlContext)
      val resultdf = ds.join(region, ds("DA") === region("userId"), "left_outer").drop("region")
      val result = resultdf.map(x => {
        val service = x.getAs[Int]("service")
        val timehep = x.getAs[Long]("timehep")
        val userid = x.getAs[String]("DA")
        val deviceid = x.getAs[String]("device_id")
        val terminal = deviceIdMapToDeviceType(deviceid)
        val paras = x.getAs[Map[String, String]]("paras")
        val f_region_id = x.getAs[String]("regionId")
        val ca_id = x.getAs[String]("ca_id")
        LogNew(service, timehep, userid, f_region_id, terminal, deviceid, paras, ca_id)
      }).toDF()
      result.repartition(partionNums)
        .registerTempTable("t_report_tmp")
    }
    else {
      val ds = ds1.map(line => {
        var log = Log(0, 0L, "", "", "", "", new scalaHashMap[String, String]())
        if (line.contains("|")) {
          //有拓展内容
          val spilts = line.split("\\|", 2) //设备相关数据|拓展数据
          if (spilts.length > 1) { //修复日志中出现 "|" 拓展数据为空的 出现角标越界异常
            val data = spilts(0).replace("<", "").replace(">", "")
              .replace("[", "").replace("]", "")
              .replace("(", "").replace(")", "")
            val userDeviceInfo = tranform(data, version, day)
            if (!userDeviceInfo.terminal.equals("unknown")) {
              val ext = spilts(1).replace("<", "").replace(">", "")
                .replace("[", "").replace("]", "")
                .replace("(", "").replace(")", "")
              log = userDeviceInfo.copy(paras = LogUtils.str_to_map_new(ext, "&", ",", SourceType.USER_REPORT))
            }
          }
        } else { //务拓展内容
          log = tranform(line.replace("<", "").replace(">", "").replace("[", "").replace("]", "").replace("(", "").replace(")", ""), version, day)
        }
        log
      })
        .filter(x =>
          x.userid != "0" && x.userid != "null"
            && !GatherType.SYSTEM_STANDBY.equals(x.service.toString)
            && x.userid != ""
        )
        .toDF()
      val region = getUserRegion(regionCode, sqlContext)
      val resultdf = ds.join(region, Seq("userid"), "left_outer").drop("region")
      val cadf = fromDeviceIdGetCa(sqlContext)
      val result = resultdf.repartition(partionNums).join(cadf, Seq("deviceid"), "left")
      result.repartition(partionNums).registerTempTable("t_report_tmp")
    }
    //除了播放行为的其他行为写入orc_report_behavior
    saveBehaviorToHive(sqlContext, day, rCode.value)
    println("用户行为处理完成")
    if (version1) {
      sqlContext.sql(
        s"""
           |select
           |service1 as servicetype,
           |from_unixtime (timehep1/1000) as starttime,
           |case when t2.service2 = ${GatherType.SYSTEM_OPEN} then from_unixtime (timehep1/1000)
           |  else from_unixtime (timehep2/1000) end as endtime,
           |case when t2.service2 = ${GatherType.SYSTEM_OPEN} then 0 else (t2.timehep2-t2.timehep1)/1000 end as playtime,
           |t2.userid,t2.regionid,t2.terminal as deviceType, t2.paras,t2.deviceid
           |    from
           |(select
           |t1.service1 ,t1.service2 ,
           |t1.timehep as timehep1 ,
           |lead(t1.timehep,1,t1.timehep) over(partition by t1.deviceid order by t1.timehep) as timehep2,
           | t1.first_time, t1.userid, t1.regionid, t1.terminal,  t1.paras, t1.deviceid
           |from (
           |select t.service as service1,
           |lead(t.service,1,"0") over(partition by t.deviceid order by t.timehep) as service2,
           |timehep,
           |first_value(timehep) over(partition by deviceid order by timehep) as first_time,
           | userid,
           |if(f_region_id is null,${rCode.value},f_region_id) as regionid,
           |terminal, paras,deviceid
           |from
           |(
           |select service,timehep,userid,terminal,deviceid,paras,regionid as f_region_id
           |from t_report_tmp
           |where
           |(     (service =${GatherType.LIVE} and (paras["A"]=1 or paras["A"]=18 or paras["A"]=19) and (paras["S"] =0 or paras["S"] = 3 or  paras["S"] = 4 or  paras["S"] = 5 or  paras["S"] = 6 or  paras["S"] = 7 or  paras["S"] = 15)) or
           |      (service =${GatherType.TIME_SHIFT} and (paras["A"]=1 or paras["A"]=18 or paras["A"]=19) and (paras["S"] =0 or paras["S"] = 3 or  paras["S"] = 4 or  paras["S"] = 5 or  paras["S"] = 6 or  paras["S"] = 7 or  paras["S"] = 15)) or
           |      (service =${GatherType.LOOK_BACK} and (paras["A"]=1 or paras["A"]=18 or paras["A"]=19) and (paras["S"] =0 or paras["S"] = 3 or  paras["S"] = 4 or  paras["S"] = 5 or  paras["S"] = 6 or  paras["S"] = 7 or  paras["S"] = 15) ) or
           |      (service =${GatherType.DEMAND} and (paras["A"]=1 or paras["A"]=18 or paras["A"]=19) and (paras["S"] =0 or paras["S"] = 3 or  paras["S"] = 4 or  paras["S"] = 5 or  paras["S"] = 6 or  paras["S"] = 7 or  paras["S"] = 15))
           |      or (service = ${GatherType.SYSTEM_HEARTBEAT}
           |      and (paras["S"]=${GatherType.BREAK_SCEN_LIVE} or paras["S"]=${GatherType.BREAK_SCEN_TIEMSFT}  or paras["S"]=${GatherType.BREAK_SCEN_LOOKBK} or paras["S"]=${GatherType.BREAK_SCEN_DEMAND})
           |       or service = ${GatherType.SYSTEM_OPEN} ))
           |       ) t
           | ) t1
           | where (t1.service1 <> ${GatherType.SYSTEM_HEARTBEAT} )
           | or (t1.service1 = ${GatherType.SYSTEM_HEARTBEAT} and t1.timehep=first_time )
           | or (t1.service1 = ${GatherType.SYSTEM_HEARTBEAT} and t1.service2="0" )
           | or (t1.service1 = ${GatherType.SYSTEM_HEARTBEAT} and t1.service2 = ${GatherType.SYSTEM_OPEN})
           |) t2
           |  where t2.service1 <> ${GatherType.SYSTEM_HEARTBEAT} or t2.timehep1 = t2.first_time
                                                      """.stripMargin)
        .repartition(partionNums).registerTempTable("t_saved")
    } else {
      //播放行为处理
      sqlContext.sql(
        s"""
           |select
           |service1 as servicetype,
           |from_unixtime (timehep1/1000) as starttime,
           |case when t2.service2 = ${GatherType.SYSTEM_OPEN} then from_unixtime (timehep1/1000)
           |  else from_unixtime (timehep2/1000) end as endtime,
           |case when t2.service2 = ${GatherType.SYSTEM_OPEN} then 0 else (t2.timehep2-t2.timehep1)/1000 end as playtime,
           |t2.userid,t2.regionid,t2.terminal as deviceType, t2.paras,t2.deviceid
           |    from
           |(select
           |t1.service1 ,t1.service2 ,
           |t1.timehep as timehep1 ,
           |lead(t1.timehep,1,t1.timehep) over(partition by t1.deviceid order by t1.timehep) as timehep2,
           | t1.first_time, t1.userid, t1.regionid, t1.terminal,  t1.paras, t1.deviceid
           |from (
           |select t.service as service1,
           |lead(t.service,1,"0") over(partition by t.deviceid order by t.timehep) as service2,
           |timehep,
           |first_value(timehep) over(partition by deviceid order by timehep) as first_time,
           | userid,
           |if(f_region_id is null,${rCode.value},f_region_id) as regionid,
           |terminal, paras,deviceid
           |from
           |(
           |select service,timehep,userid,terminal,deviceid,paras,regionid as f_region_id
           |from t_report_tmp
           |where
           |(     (service =${GatherType.LIVE} and (paras["A"]=1 or paras["A"]=18 or paras["A"]=19) ) or
           |      (service =${GatherType.TIME_SHIFT} and (paras["A"]=1 or paras["A"]=18 or paras["A"]=19)) or
           |      (service =${GatherType.LOOK_BACK} and (paras["A"]=1 or paras["A"]=18 or paras["A"]=19)  ) or
           |      (service =${GatherType.DEMAND} and (paras["A"]=1 or paras["A"]=18 or paras["A"]=19) )
           |      or (service = ${GatherType.SYSTEM_HEARTBEAT}
           |      and ((paras["S"]=${GatherType.BREAK_SCEN_LIVE} or paras["S"]=${GatherType.BREAK_SCEN_TIEMSFT}  or paras["S"]=${GatherType.BREAK_SCEN_LOOKBK} or paras["S"]=${GatherType.BREAK_SCEN_DEMAND}))
           |       or service = ${GatherType.SYSTEM_OPEN} ))
           |       ) t
           | ) t1
           | where (t1.service1 <> ${GatherType.SYSTEM_HEARTBEAT} )
           | or (t1.service1 = ${GatherType.SYSTEM_HEARTBEAT} and t1.timehep=first_time )
           | or (t1.service1 = ${GatherType.SYSTEM_HEARTBEAT} and t1.service2="0" )
           | or (t1.service1 = ${GatherType.SYSTEM_HEARTBEAT} and t1.service2 = ${GatherType.SYSTEM_OPEN})
           |) t2
           |  where t2.service1 <> ${GatherType.SYSTEM_HEARTBEAT} or t2.timehep1 = t2.first_time
                                                      """.stripMargin)
        .repartition(partionNums).registerTempTable("t_saved")
    }
    saveToOrcPlayRecommend(day, sqlContext)
    println("上报日志处理完成")
  }


  def fromDeviceIdGetCa(sqlContext: HiveContext) = {
    val dc_sql =
      """( select device_id as deviceid,
        |(case device_type when 1 then stb_id
        |when 2 then cai_id
        |when 3 then mobile_id
        |when 4 then pad_id
        |when 5 then mac_address end) as ca_id
        |from homed_iusm.device_info where status=1 ) as d""".stripMargin
    val dc_df = DBUtils.loadMysql(sqlContext, dc_sql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()
    dc_df
  }

  def saveBehaviorToHive(sqlContext: HiveContext, day: String, rCode: String) = {
    sqlContext.sql("use bigdata")
    sqlContext.sql(s"alter table  orc_report_behavior drop IF EXISTS PARTITION(day='$day')")
    //sqlContext.sql(s"alter table  bigdata.orc_report_behavior drop IF EXISTS PARTITION(day='$day')")
    //地方定制化 - user behavior
    val registTmpName = clusterRegion match {
      case "340200" => { //芜湖
        val bhfDf = sqlContext.sql("select * from t_report_tmp")
        LogProcess.saveReportLogBehavior(day, bhfDf: DataFrame, sqlContext: SQLContext)
      }
      case _ => { //默认
        "t_report_tmp"
      }
    }
    sqlContext.sql(
      s"""
         |insert  overwrite table bigdata.orc_report_behavior  partition(day='$day')
         |select userid,
         |if(regionid  is null,${rCode},regionid) as regionid,
         |deviceid,terminal as devicetype,
         |service as reporttype,
         |from_unixtime (timehep/1000) as reporttime,
         |paras as exts,
         |ca_id as ca
         | from
         |$registTmpName
            """.stripMargin)
  }

  def saveToOrcPlayRecommend(day: String, sqlContext: HiveContext) = {
    sqlContext.sql("use bigdata")
    sqlContext.sql(s"alter table  orc_video_report  drop IF EXISTS PARTITION(day='$day')")
//    sqlContext.sql(s"alter table  bigdata.orc_video_report  drop IF EXISTS PARTITION(day='$day')")
    sqlContext.sql(
      s"""
         |insert overwrite  table bigdata.orc_video_report  partition(day='$day')
         |select
         |userid,deviceId,deviceType,regionId,
         |(case when ((servicetype=${GatherType.LIVE}) or ((servicetype=${GatherType.SYSTEM_HEARTBEAT}) and (paras ['S'] =${GatherType.BREAK_SCEN_LIVE}))) then  'live'
         | when ((servicetype=${GatherType.TIME_SHIFT}) or ((servicetype=${GatherType.SYSTEM_HEARTBEAT}) and (paras ['S'] = ${GatherType.BREAK_SCEN_TIEMSFT}))) then  'timeshift'
         | when ((servicetype=${GatherType.LOOK_BACK}) or ((servicetype=${GatherType.SYSTEM_HEARTBEAT}) and (paras ['S'] = ${GatherType.BREAK_SCEN_LOOKBK}))) then 'lookback'
         |  else  'demand' end) as
         |servicetype,
         |if((paras ['ID'] is  not null),paras ['ID'],paras ['I'])  AS  serviceId,starttime,endTime,playTime,
         |paras
         |from t_saved
         |where ((((servicetype=${GatherType.LIVE})  or (servicetype=${GatherType.TIME_SHIFT}) or (servicetype=${GatherType.LOOK_BACK}) or (servicetype=${GatherType.DEMAND})) and (paras ['A']=1))
         |or (servicetype=${GatherType.SYSTEM_HEARTBEAT}) )
         |and ((paras ['ID'] is  not null) or (paras ['I'] is not null))
        """.stripMargin)
  }


  case class Log(service: Int, timehep: Long, userid: String = "0", region: String, terminal: String, deviceid: String, paras: scalaHashMap[String, String])

  case class LogNew(service: Int, timehep: Long, userid: String = "0", regionid: String, terminal: String, deviceid: String, paras: Map[String, String], ca_id: String)

  /**
    * 转换
    * 将设备ID转换为对应的设备类型
    *
    * @param data 包含service,event_time,userid,region,deviceid格式
    */
  //[0101,1537951414577,50310979,0,1005358791]
  private def tranform(data: String, version: Boolean, day: String): Log = {
    var log = Log(0, 0L, "", "", "", "", new scalaHashMap[String, String]())
    val datas = data.split(SPLIT)
    if (datas.length == 5) {
      var deviceType = LogConstant.TERMINAL
      var deviceId = datas(4)
      if (version) {
        deviceType = LogConstant.TERMINAL
      }
      val regex ="""^\d+$""".r
      if (regex.findFirstMatchIn(datas(4)) != None) {
        deviceType = deviceIdMapToDeviceType(datas(4))
      }
      else {
        deviceId = "0"
      }
      try {
        if (datas(1) != "undefined" && datas(0) != "undefined") {
          val reprotTimeStamp = getTime(datas(1), day)
          log = Log(datas(0).toInt, reprotTimeStamp, datas(2), datas(3), deviceType, deviceId, new scalaHashMap[String, String]()).copy()
        }
      } catch {
        case ex: Exception => ex.printStackTrace()
      }
    } else if (datas.length == 4 /*&& Constant.HUNAN_CODE.equals(CluserProperties.REGION_CODE)*/ ) { // 湖南定制,兼容缺少deviceId字段情况
      if (datas(1) != "undefined" && datas(0) != "undefined") {
        val reprotTimeStamp = getTime(datas(1), day)
        log = Log(datas(0).toInt, reprotTimeStamp, datas(2), datas(3), LogConstant.TERMINAL, "", new scalaHashMap[String, String]()).copy()
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

  /**
    * 设备ID映射为设备类型
    *
    * @param deviceId 　设备ID
    * @return 设备类型
    */
  private def deviceIdMapToDeviceType(deviceId: String): String = {
    //  var deviceType = "0"
    //终端（0 其他, 1stb,2 CA卡,3mobile,4 pad, 5pc）
    var deviceType = LogConstant.TERMINAL
    try {
      val device = deviceId.toLong
      if (device >= 1000000000l && device <= 1199999999l) {
        deviceType = "1"
      } else if (device >= 1400000000l && device <= 1599999999l) {
        deviceType = "2"
      } else if (device >= 1800000000l && device < 1899999999l) {
        deviceType = "4"
      } else if (device >= 2000000000l && device <= 2999999999l) {
        deviceType = "3"
      } else if (device >= 3000000000l && device < 3999999999l) {
        deviceType = "5"
      }
    } catch {
      case ex: Exception => LOGGER.error(ex.getMessage); ex.printStackTrace()
    }
    deviceType
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
