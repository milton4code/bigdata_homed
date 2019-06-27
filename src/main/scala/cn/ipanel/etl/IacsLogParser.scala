package cn.ipanel.etl

import cn.ipanel.common.{Constant, SparkSession, Tables}
import cn.ipanel.etl.RunLogParser._
import cn.ipanel.utils.MultilistUtils.getHomedVersion
import cn.ipanel.utils.{DateUtils, LogUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SaveMode}

import scala.collection.mutable

/**
  *
  * iacs日志解析程序
  * Author :liujjy  
  * Date : 2019-02-24 0024 10:10
  * version: 1.0
  * Describle: 
  **/
object IacsLogParser {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("请输入有效日期,分区数, 例如【20180401】, [200] ")
      System.exit(-1)
    }
    val day = args(0)
    val partitions = args(1).toInt
    val session = new SparkSession("IacsLogParser")
    try {
      val homedVersion = getHomedVersion()
      //homed版本大于1.4 才有iacs 用户离线日志
      if(  homedVersion > Constant.OLDER_HOMED_OVERION  ){
        process(day, partitions, session)
      }
      MysqlToHive.process(day, partitions, session.sqlContext)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()

      }
    }


  }

  /**
    * 解析iacs日志,并存入到
    */
  private def process(day: String, partitions: Int, session: SparkSession) = {
    val sparkContext = session.sparkContext
    val sqlContext = session.sqlContext

    val logPathDate: String = DateUtils.transformDateStr(day, DateUtils.YYYYMMDD, DateUtils.YYYY_MM_DD)
    import sqlContext.implicits._
    val iacs_log_path = LogConstant.IACS_LOG_PATH + "/" + logPathDate + "/*"
    val textRDD = sparkContext.textFile(iacs_log_path).filter(x => filterLog(x))
    val df = textRDD.filter(!_.contains("===="))
      .filter(_.split("\\[INFO\\]").length == 2) // 修复角标越界问题
      .map(line => {
      val splits = line.split("\\[INFO\\]") //日志分成时间 和 内容
      val reportTime = getReportTime(splits(0))
      val logInfo: String = splits(1)
      val keyWord: String = getKeyWord(logInfo)
      val logMap: mutable.HashMap[String, String] = LogUtils.str_to_map(logInfo.substring(logInfo.indexOf(Constant.SPLIT) + 1), ",", " ")
      (reportTime, keyWord, logMap, day)
    }).toDF("report_time", "key_word", "exts", "day")

    sqlContext.sql("use bigdata")
    df.repartition(partitions).write.mode(SaveMode.Overwrite).format("orc").partitionBy("day").insertInto(Tables.ORC_IACS)
  }

  private def filterLog(log: String) = {
    log.contains("UserOnline") || log.contains("UserOffline")
  }
}
