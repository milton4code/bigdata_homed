package cn.ipanel.ocn.etl

import cn.ipanel.common._
import cn.ipanel.etl.LogConstant
import cn.ipanel.ocn.utils.OcnLogUtils
import cn.ipanel.utils._
import jodd.util.StringUtil
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.{HashMap => scalaHashMap}

/**
  *
  * OCN项目日志解析代码
  *
  * @author liujjy  
  * @date 2018/05/26 16:53
  */

private[ocn] object OcnLogParser {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("参数错误,请输入日期 ,分区数 [20180528 30]")
      sys.exit(-1)
    }


    val day = args(0)
    val nums = args(1).toInt
    val session = new SparkSession("OcnLogParser","")
    process(session,day,nums)
  }


  def process (session:SparkSession,day:String,nums:Int=30)={
    val sc = session.sparkContext
    lazy val sqlContext = session.sqlContext
    val path = LogConstant.HDFS_ARATE_LOG + day + "/*/*"
    deleteDuplicateData(day, sqlContext)
    import sqlContext.implicits._
    val ds = sc.textFile(path)
      .filter(line => filterOCNLog(line))
      .filter(line => line.startsWith("<?>") && StringUtil.isNotEmpty(line))
      .map(_.substring(3).replaceAll("(<|>|\\[|\\]|\\(|\\))", "").replaceAll("\\s*|\t|\r|\n", ""))
      .filter(x => {
        x.split(OcnConstant.SPLIT).nonEmpty && validateTime(x.split(OcnConstant.SPLIT)(1), day)
      }).map(line => {
      var log = Log(params = new scalaHashMap[String, String]())
      if (line.contains("|")) {
        //有拓展内容
        //val spilts = line.split("\\|") //设备相关数据|拓展数据
        val userAndDeviceStr = line.substring(0, line.indexOf("|")) //设备相关数据
        val extendsStr = line.substring(line.indexOf("|") + 1)
        val userDeviceInfo = OcnLogUtils.tranform(userAndDeviceStr)
        if (!userDeviceInfo.device_type.equals("unknown")) {
          log = userDeviceInfo.copy(params = LogUtils.str_to_map(extendsStr, "&", ","))
        }
      } else { //拓展内容
        log = OcnLogUtils.tranform(line)
      }
      log
    }).filter(log => log.service_type.equals(OcnConstant.RECOMMEND) || log.service_type.equals(OcnConstant.SEARCH))
      .toDF().repartition(nums)

    ds.registerTempTable("t_ocn_log")

    sqlContext.sql(s"use bigdata")

    //    ds.show()

    sqlContext.sql(
      s"""
         |insert overwrite table bigdata.orc_ocn_reported partition(day='$day')
         |select service_type,report_time,user_id,region_code,device_id,device_type,params
         |from t_ocn_log
         |where params is NOT NULL and size(params)>0
         |""".stripMargin)

  }
  /**
    * 过滤OCN 类型日志
    */
  private def filterOCNLog(line: String): Boolean = {
    line.contains(OcnConstant.SEARCH) || line.contains(OcnConstant.RECOMMEND)
  }


  //时间检验
  private def validateTime(timeStampString: String, day: String): Boolean = {
    validateTimeLength(timeStampString) && valiateTimeRange(timeStampString, day)

  }

  //校验时间范围，排除日志中不是指定day日期内数据
  private def valiateTimeRange(timeStampString: String, day: String): Boolean = {
    val startTime = DateTime.parse(day, DateTimeFormat.forPattern(DateUtils.YYYYMMDD))
    val endTime = new DateTime(timeStampString.toLong)
    if (startTime.getDayOfMonth - endTime.getDayOfMonth == 0) true else false
  }

  //校验时间长度
  private def validateTimeLength(timeStampString: String): Boolean = {
    timeStampString.length == 13 && !timeStampString.contains("&")
  }

  def deleteDuplicateData(day: String, sqlContext: SQLContext) = {
    val deleteSql = s"DELETE  FROM t_ocn_search WHERE f_date=$day"
    DBUtils.executeSql(DBProperties.OCN_JDBC_URL, DBProperties.OCN_USER, DBProperties.OCN_PASSWORD, deleteSql)
  }


  /**
    * 将拓展内容转成map
    *
    * @param extendInfo 将拓展内容
    * @return
    */
  private def extendInfoToMap(extendInfo: String) = {
    var paras = new scalaHashMap[String, String]()
    extendInfo.split("&").foreach(ext => {
      val exts = ext.split(OcnConstant.SPLIT)
      if (exts.length > 1) {
        paras.put(exts(0), exts(1))
      } else {
        paras.put(exts(0), null)
      }
    })
    paras
  }

  private[ocn] case class Log(service_type: String = "", report_time: String = "", user_id: String = "", region_code: String = "", device_id: String = "",
                              device_type: String = "", params: scalaHashMap[String, String])

}
