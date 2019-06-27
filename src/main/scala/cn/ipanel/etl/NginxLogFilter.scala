package cn.ipanel.etl

import cn.ipanel.common.{Constant, SparkSession, Tables}
import cn.ipanel.utils.{DateUtils, LogUtils}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Nginxr日志过滤器
  * 注意对Nginx做预处理,减少数据传输量
  */
case class Log(report_time: String, key_word: String, params: mutable.HashMap[String, String], body: mutable.HashMap[String, String])

case class Log2(report_time: String, key_word: String, params: String, body: String)

object NginxLogFilter {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("=========请输入有日期,分区数: [20180518] , [20]")
      System.exit(-1)
    }
    val day = args(0)
    val nums  = args(1).toInt
    val sparkSession = new SparkSession("NginxLogFilter", "")
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    sc.getConf.registerKryoClasses(Array(classOf[Log]))
    sqlContext.sql(s"use ${Constant.HIVE_DB}")

    val jsonDF: DataFrame = sqlContext.read.json(LogConstant.NGINX_JSON_LOG_PATH + day + "/*")
    import sqlContext.implicits._
    val df = filterLog(jsonDF, sqlContext)

    df.selectExpr("request", "timestamp as reportTime", "body")
      .mapPartitions(it => {
         val listBuffer = new ListBuffer[Log]()
         while (it.hasNext) {
           val log = it.next()
           val request = log.getString(0)
           val post_body = log.getString(2).replace("\\x22", "").replaceAll("\\{|\\}|\"", "")
           val report_time = DateUtils.nginxTimeFormate(log.getString(1))
           val body = LogUtils.str_to_map(post_body)
            if(request.indexOf("?") > -1){ //homed url get请求一般都会带参数
              val request_filter = request.substring(0, request.indexOf("?"))
              val params = LogUtils.str_to_map(request.substring(request_filter.length + 1), "&", "=")
              listBuffer.+=(Log(report_time, request_filter, params, body))
            }else{
              listBuffer.+=(Log(report_time, request, null, body))
            }
          }
          listBuffer.iterator
      }).toDF()
      .repartition(nums)
      .registerTempTable("t_log")

    sqlContext.sql(
      s"""
         |insert overwrite   table ${Tables.ORC_NGINX_LOG} partition(day='$day')
         |select report_time,key_word,params, body from t_log
      """.stripMargin)

    sc.stop()
  }


  def filterLog(json: DataFrame, sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    json.filter(
      $"request".contains("/filter/filter") //筛选
        || $"request".contains("search/search_by_keyword") //搜索
        || $"request".contains("media/series/get_info")
//        || $"request".contains("media/event/get_info")
        || $"request".contains("media/video/get_info")
        || $"request".contains("channel/get_list") //栏目关联
        || $"request".contains("event/get_list")
        || $"request".contains("event/get_info")
        || $"request".contains("program/get_list")
        || $"request".contains("programtype/get_list")//
        || $"request".contains("video/get_info")
        || $"request".contains("ad/get_list")     //广告
        || $"request".contains("baike/get_info")   //明星
        || $"request".contains("share/index.html")//分享点击量
        || $"request".contains("/login")//登录
    )

  }

}
