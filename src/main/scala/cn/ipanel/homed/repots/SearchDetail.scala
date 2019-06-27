package cn.ipanel.homed.repots

import java.net.URLDecoder

import cn.ipanel.common.{CluserProperties, SparkSession, _}
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Sorting

/**
  * 热搜词报表
  * author Liu gaohui
  * date 2018/05/07
  **/
object SearchDetail {
  def main(args: Array[String]): Unit = {
    var day = DateUtils.getYesterday()
    if (args.size == 1) {
      day = args(0)
    }

    val sparkSession = SparkSession("SearchDetail", CluserProperties.SPARK_MASTER_URL)
    val sparkContext = sparkSession.sparkContext
    val hiveContext = sparkSession.sqlContext
    import sparkSession.sqlContext.implicits._
    hiveContext.sql(s"use ${Constant.HIVE_DB}")
    //读orc_nginx_log中的数据，过滤不含搜索的数据
    val lines_base =
      s"""
         |select report_time,key_word,params['accesstoken'] as accesstoken,
         |params['keyword'] as keyword
         |from orc_nginx_log
         |where day='$day' and
         |(key_word like '%/search/search_by_keyword%'
         |or key_word like '%/media/video/get_info%' or key_word like '%/media/event/get_info%')
            """.stripMargin
    val logRdd = hiveContext.sql(lines_base).rdd
    val reduceRdd = logRdd.mapPartitions(partition => {
      val list = new ListBuffer[(String, Array[String])]
      val dupMap = new mutable.HashMap[String, String]()
      partition.foreach(x => {
        val report_time = x.getAs[String]("report_time")
        val keyword = x.getAs[String]("keyword")
        val accesstoken = x.getAs[String]("accesstoken")
        val request = x.getAs[String]("key_word")
        var userIdAndDeviceId = ""
        try {
          userIdAndDeviceId = TokenParser.parser(accesstoken)
        } catch {
          case e: Exception =>
        }
        val key = request + "," + accesstoken + "," + keyword
        if (accesstoken != "" && accesstoken != null && userIdAndDeviceId != "" && !dupMap.contains(key)) {
          //          list += ((userIdAndDeviceId + hour, Array(time + "-->" + key)))

          list += ((userIdAndDeviceId, Array(report_time + "-->" + key)))
          dupMap += (key -> "")
        }
      })
      list.toIterator
    }).reduceByKey((a, b) => sort(a, b))
    //      .filter(x=>x._1=="55315352_1005361582").toDF().show(false)
    //通过时间排序，当搜索后，点击进入内容后算搜索一次
    val result = reduceRdd.mapPartitions(partition => {
      val listBuffer = ListBuffer[keyWord]()
      partition.foreach(x => {
        val userIdAndDeviceId = x._1
        val arr = x._2
        var Word = ""
        var indx = 1
        for (x <- arr) {
          var keyword = ""
          var accesstoken = ""
          var request = ""
          val report_time = x.split("-->")(0)
          val keyarr = x.split("-->")(1).split(",")
          if (keyarr.length == 3) {
            request = keyarr(0)
            accesstoken = keyarr(1)
            keyword = keyarr(2)
          }
          if (request.contains("/search/search_by_keyword")) {
            Word = keyword
            indx = 0
          } else {
            if (Word != "" && Word != null && Word != "null" && indx == 0) {
              listBuffer += keyWord(report_time, accesstoken, Word)
              indx = 1
            }
          }
        }

      })
      listBuffer.toList.toIterator
    }).distinct().map(x => {
      val time = x.time.split(" ")
      val date = time(0)
      val hour = time(1).split(":")(0)
      val f_timerange = getTimeRangeByMinute(time(1).split(":")(1).toInt)
      val token = x.accesstoken
      val keyword = URLDecoder.decode(x.keyword, "utf-8")
      val key = date + "," + hour + "," + f_timerange + "," + token + "," + keyword
      (key, 1)
    }).reduceByKey(_ + _).map(x => {
      val keyarr = x._1.split(",")
      val f_date = keyarr(0).replace("-", "")
      val f_hour = keyarr(1)
      val f_timerange = keyarr(2)
      val arr = TokenParser.parserAsUser(keyarr(3))
      val f_userid = arr.DA.toString
      val deviceId = arr.device_id
      val f_terminal = arr.device_type
      val f_province_id = arr.province_id
      val f_city_id = arr.city_id
      val f_region_id = arr.region_id
      val f_keyword = keyarr(4)
      val f_count = x._2
      WordCount(f_date, f_hour, f_timerange, f_userid, f_terminal, f_province_id, f_city_id, f_region_id, f_keyword, f_count)
    }).filter(x => x.f_keyword != "null" && x.f_userid != "0").toDF()
    SearchDetail(result, sparkSession)
  }

  /**
    * 通过区域id,城市id，省份id找到用户的地址信息
    * @param detailsDF
    * @param sparkSession
    */
  def SearchDetail(detailsDF: DataFrame, sparkSession: SparkSession): Unit = {
    val sqlContext = sparkSession.sqlContext
    val provincesql =
      s"""
         |(SELECT province_id as f_province_id,province_name as f_province_name
         |from province
          ) as aa
     """.stripMargin
    val citysql =
      s"""
         |(SELECT city_id as f_city_id,city_name as f_city_name
         |from city
          ) as aa
     """.stripMargin
    val regionsql =
      s"""
         |(SELECT area_id as f_region_id,area_name as f_region_name
         |from area
          ) as aa
     """.stripMargin
    val provinceDF = DBUtils.loadMysql(sqlContext, provincesql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val cityDF = DBUtils.loadMysql(sqlContext, citysql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val regionDF = DBUtils.loadMysql(sqlContext, regionsql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val resultDF = detailsDF.join(provinceDF, Seq("f_province_id"))
      .join(cityDF, Seq("f_city_id"))
      .join(regionDF, Seq("f_region_id"))
    DBUtils.saveToHomedData_2(resultDF, Tables.mysql_user_search_keyword)
  }

  /**
    * 半个小时一个区间
    * @param minute
    * @return
    */
  def getTimeRangeByMinute(minute: Int): String = {
    var timeRange = "00-30"
    minute match {
      case _ if (minute >= 30) => timeRange = "60"
      case _ => timeRange = "30"
    }
    timeRange
  }

  /**
    * 通过时间来进行排序
    * @param arr1
    * @param arr2
    * @return
    */
  def sort(arr1: Array[String], arr2: Array[String]): Array[String] = {
    val buffer = arr1.toBuffer
    buffer ++= arr2
    val result = buffer.toArray
    Sorting.quickSort(result)
    result
  }

  /**
    * 构造map
    * @param request
    * @return
    */
  def stringToMap(request: String): mutable.Map[String, String] = {
    val map = new mutable.HashMap[String, String]()
    val tokens = request.substring(request.indexOf("?") + 1).split("&")
    for (token <- tokens) {
      val splits = token.split("=")
      if (splits.length == 2) {
        map += (splits(0) -> splits(1))
      }
    }
    map
  }
}

