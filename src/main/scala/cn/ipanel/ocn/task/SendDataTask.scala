package cn.ipanel.ocn.task

import java.util

import cn.ipanel.common.{DBProperties, SparkSession}
import cn.ipanel.ocn.common.Properties
import cn.ipanel.utils.{DBUtils, DateUtils, PushTools}
import com.alibaba.fastjson.JSONObject
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory


/**
  * SendDataTask<br> 
  * 将热搜词推送给推荐
  *
  * @author liujjy
  * @create 2018/6/6
  * @since 1.0.0
  */
object SendDataTask {
  private lazy val LOGGER = LoggerFactory.getLogger(SendDataTask.getClass)
  def main(args: Array[String]): Unit = {

    if(args.length != 3){
      System.err.println(s"""
                            |Usage: SendDataTask <ranks> <day> <days>
                            |  <ranks> 表示限定排名总条数,默认500
                            |  <endTime> 表示统计热搜词的截至时间 默认昨天 ，格式 例如 20180501
                            |  <days> 表示需要加载几天前数据，默认7天
        """.stripMargin)
      sys.exit(-1)
    }


    val (ranks,endTime,days) = (args(0).toInt,args(1),args(2).toInt)
    val sparkSession = new SparkSession("SendDataTask","")
    //特意减少日志日志输出
    sparkSession.sparkContext.setLogLevel("WARN")

    val sqlContext = sparkSession.sqlContext


   val sendDataDF =  getSendData(ranks, endTime, days, sqlContext)

    val data = getSendData(sendDataDF)

    val result = PushTools.sendPostRequest(Properties.POST_URL,data)

    LOGGER.error("result=====" + result)
  }

  private def getSendData(sendDataDF: DataFrame): String = {
    val listBuffer = new util.ArrayList[util.HashMap[String, Any]]()
    sendDataDF.collect().toList.foreach(r => {
      val map = new util.HashMap[String, Any]()
      map.put("hotkeyword", r.getString(0))
      map.put("value", r.getDecimal(1))
      listBuffer.add(map)
    })

    val json = new JSONObject()
    json.put("list", listBuffer)
    json.toString
  }

  private def getSendData(ranks: Int, endTime: String, days: Int, sqlContext: SQLContext): DataFrame = {
    val dateTime = DateUtils.dateStrToDateTime(endTime, DateUtils.YYYYMMDD)
    val startTime = dateTime.plusDays(-days).toString(DateUtils.YYYYMMDD)

    val sql =
      s"""
         |(select f_key_word hotkeyword ,SUM(f_pv) value from t_ocn_search
         |where f_date >= '$startTime' and f_date <= '$endTime'
         |and f_word_type=2  GROUP BY f_key_word
         |LIMIT $ranks) as t
       """.stripMargin
     DBUtils.loadMysql(sqlContext, sql, DBProperties.OCN_JDBC_URL, DBProperties.OCN_USER, DBProperties.OCN_PASSWORD)
  }
}
