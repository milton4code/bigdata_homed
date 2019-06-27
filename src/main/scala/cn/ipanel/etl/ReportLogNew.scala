package cn.ipanel.etl

import java.util.regex.Pattern

import cn.ipanel.common._
import cn.ipanel.utils.{DBUtils, DateUtils, LogUtils, RegionUtils}
import jodd.util.StringUtil
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import scala.collection.mutable.{HashMap => scalaHashMap}

//初始化数据清洗
object ReportLogNew {
  private lazy val SPLIT = ","
  private lazy val BYTE_SIZE = 1024
  private lazy val LOGGER = LoggerFactory.getLogger(ReportLogParser.getClass)


  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("请输入参数 日期,分区数:例如 20180919 30")
      sys.exit(-1)
    }

    val day = args(0)
    val partionNums = args(1).toInt
    val path = LogConstant.HDFS_ARATE_LOG + day + "/*"
    // val path = "file:///C:\\Users\\Administrator\\Desktop\\qq"
    val session = new SparkSession("ReportLogNew")
    val sc = session.sparkContext
    val sqlContext = session.sqlContext

    var regionCode = RegionUtils.getRootRegion
    if (regionCode.isEmpty) {
      regionCode = args(1)
    }

    if (regionCode.endsWith("0000")) {
      regionCode = (regionCode.toInt + 101).toString
    } else {
      regionCode = (regionCode.toInt + 1).toString
    }
    val rCode = session.sparkContext.broadcast(regionCode)
    val textFile = sc.textFile(path)
    //直播中点赞，分享等行为码
    //val liveFilterBr  = sc.broadcast(Array("2","3","4","11","12","13","14","18","19","28","29"))
    //<?><[0101,1537951414577,50310979,0,1005358791]><|><(T,live)><&><(P,http)><&><(ID,4200851466)><&><(VC,1005358791)><&><(CL,100)><&><(A,1)><&><(NO,0)><&><(NA,山东卫视)><&><(PA,0)>
    import sqlContext.implicits._
    val ds = textFile.filter(x => x.startsWith("<?>") && StringUtil.isNotEmpty(x))
      .map(log => {
        val tmp = log.substring(3).replace("<", "").replace(">", "").replace("[", "").replace("]", "").replace("(", "").replace(")", "")
        trimSpace(tmp)
      })
      .filter(x => { //过滤非当天的数据
        val line = x.split(SPLIT)
        line.nonEmpty
      })
      .map(line => {
        var log = Log(0, 0L, "", "", "", "", new scalaHashMap[String, String]())
        if (line.contains("|")) {
          //有拓展内容
          val spilts = line.split("\\|") //设备相关数据|拓展数据
          if (spilts.length > 1) { //修复日志中出现 "|" 拓展数据为空的 出现角标越界异常
            val userDeviceInfo = tranform(spilts(0))
            if (!userDeviceInfo.terminal.equals("unknown")) {
              log = userDeviceInfo.copy(paras = LogUtils.str_to_map(spilts(1), "&", ","))
            }
          }
        }
        log
      })
      .filter(x => x.userid != 0 && x.deviceid != null && x.deviceid != "null" && x.deviceid != "" && x.terminal != "0" && !GatherType.SYSTEM_STANDBY.equals(x.service.toString))
      .toDF()

    val accountInfoSql =
      """
        |(select  DA as f_user_id,home_id as f_home_id
        |from account_info) as account
      """.stripMargin
    val addressInfoSql =
      s"""
         |(select home_id,if(length(cast(region_id as char))=6 and region_id not like '%00',cast(region_id as char),${rCode.value}) as f_region_id
         |from address_info ) as address
          """.stripMargin
    val accountDF = DBUtils.loadMysql(sqlContext, accountInfoSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val addressDF = DBUtils.loadMysql(sqlContext, addressInfoSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)

    val region = accountDF.join(addressDF, accountDF("f_home_id") === addressDF("home_id")).drop("f_home_id").drop("home_id")
    val result = ds.join(region, ds("userid") === region("f_user_id"), "left_outer").drop("region")
    result.repartition(partionNums).registerTempTable("t_report_tmp")
    //除了播放行为的其他行为写入hive
    // saveBehaviorToHive(sqlContext, day,rCode.value)
    //播放行为处理
    sqlContext.sql(
      s"""
         |select t.service1 as servicetype,t.service2,
         |(t.timehep2-t.timehep1)/1000   as playTime,substr(from_unixtime (timehep1/1000,'yyyyMMdd HH:mm:ss'),0,8) as day,
         |from_unixtime (t.timehep1/1000) as starttime,
         |from_unixtime (t.timehep2/1000) as endTime,t.terminal as deviceType,
         |t.userid,t.regionid, t.paras,t.deviceid
         |from
         |(select service as service1,terminal,
         |lead(service,1,service) over(partition by userid order by timehep) as service2,
         |timehep as timehep1,
         |lead(timehep,1,timehep) over(partition by userid order by timehep) as timehep2,
         | userid,
         |if((f_region_id is null),${rCode.value},f_region_id) as regionid,
         |terminal, paras,deviceid
         |from
         |t_report_tmp ) t
          """.stripMargin)//.show(1000,false)
     .registerTempTable("t_saved")
    saveToOrcPlayRecommend(day, sqlContext)
  }


  def saveToOrcPlayRecommend(day: String, sqlContext: HiveContext) = {
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlContext.sql(
      s"""
         |insert overwrite  table bigdata.orc_video_play_recommend  partition(day)
         |select
         |userid,deviceId,deviceType,regionId,
         |(case when ((servicetype=101) or ((servicetype=701) and (paras ['S'] = 1))) then  'live'
         |when ((servicetype=102) or ((servicetype=701) and (paras ['S'] = 2))) then  'timeshift'
         |when ((servicetype=103) or ((servicetype=701) and (paras ['S'] = 3))) then 'lookback'
         |else  'demand' end) as
         |servicetype,
         |if((paras ['ID'] is  not null),paras ['ID'],paras ['I'])  AS  serviceId,
         |starttime,
         |endTime,
         |playTime,
         |paras,day
         |from t_saved
         |where ((((servicetype=101)  or (servicetype=102) or (servicetype=103) or (servicetype=104)) and (paras ['A']=1))
         |or (servicetype=701) ) and
         |((paras ['ID'] is  not null) or (paras ['I'] is not null))
        """.stripMargin)
  }


  case class Log(service: Int, timehep: Long, userid: String = "0", region: String, terminal: String, deviceid: String, paras: scalaHashMap[String, String])

  /**
    * 转换
    * 将设备ID转换为对应的设备类型
    *
    * @param data 包含service,event_time,userid,region,deviceid格式
    */
  //[0101,1537951414577,50310979,0,1005358791]
  private def tranform(data: String): Log = {
    var log = Log(0, 0L, "", "", "", "", new scalaHashMap[String, String]())
    val datas = data.split(SPLIT)
    if (datas.length == 5) {
      var deviceType = "0"
      if (datas(4) != null && datas(4) != "null" && datas(4) != "undefined") {
        deviceType = deviceIdMapToDeviceType(datas(4))
      }
      try {
        if (datas(1) != "undefined" && datas(0) != "undefined") {
          log = Log(datas(0).toInt, datas(1).toLong, datas(2), datas(3), deviceType, datas(4), new scalaHashMap[String, String]()).copy()
        }
      } catch {
        case ex: Exception => ex.printStackTrace()
      }
    }
    log
  }

  /**
    * 设备ID映射为设备类型
    *
    * @param deviceId 　设备ID
    * @return 设备类型
    */
  private def deviceIdMapToDeviceType(deviceId: String): String = {
    //    var deviceType = "unknown"
    //终端（0 其他, 1stb,2 CA卡,3mobile,4 pad, 5pc）
    var deviceType = "0"
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

  //时间检验
  private def validateTime(timeStampString: String, day: String): Boolean = {
    validateTimeLength(timeStampString) && valiateTimeRange(timeStampString, day)
  }

  //校验时间范围，排除日志中不是指定day日期内数据
  private def valiateTimeRange(timeStampString: String, day: String): Boolean = {
    val startTime = DateTime.parse(day, DateTimeFormat.forPattern(DateUtils.YYYYMMDD))
    val endTime = new DateTime(timeStampString.toLong)
    if (startTime.getDayOfYear - endTime.getDayOfYear == 0) true else false
  }

  //校验时间长度
  private def validateTimeLength(timeStampString: String): Boolean = {
    timeStampString.length == 13 && !timeStampString.contains("&")
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
