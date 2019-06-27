package cn.ipanel.etl

import cn.ipanel.common.{SparkSession, TokenParser}
import cn.ipanel.utils.{DBUtils, DateUtils}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Sorting


/**
  * Nginx日志栏目分析
  *
  * @author ZouBo
  * @date 2018/4/10 0010 
  */

object NginxLogParseForLabel {

  def parseHiveNginxLog(sparkSession: SparkSession, day: String) = {
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    sqlContext.sql("use bigdata")
    val hiveSql =
      s"""
         | select report_time,key_word,params['accesstoken'] as token,params['label'] as label,
         | params['chnlid'] as chnlid,params['videoid'] as videoid,params['eventid'] as eventid
         | from orc_nginx_log
         | where day='$day'
         | and (key_word like '%channel/get_list%' or key_word like '%event/get_list%'
         | or key_word like '%event/get_info%' or key_word like '%program/get_list%' or key_word like '%video/get_info%'
         | or key_word like '%search/search_by_keyword%'
         |  or key_word like '%ad/get_list%')
    """.stripMargin
    val logRdd = sqlContext.sql(hiveSql).rdd

    val reduceRdd = logRdd.mapPartitions(partition => {
      val list = new ListBuffer[(String, Array[String])]
      val dupMap = new mutable.HashMap[String, String]()
      partition.foreach(row => {
        val time = row.getAs[String]("report_time")
        val keyWord = row.getAs[String]("key_word")
        val token = row.getAs[String]("token")
        val label = if (row.getAs[String]("label") == null) ""
        else getLabel(row.getAs[String]("label"))
        val chnlid = row.getAs[String]("chnlid")
        val videoid = row.getAs[String]("videoid")
        val eventid = row.getAs[String]("eventid")
        val hour = if (time.length >= 13) {
          time.substring(0, 13)
        } else {
          time
        }
        var userIdAndDeviceId = ""
        try {
          userIdAndDeviceId = TokenParser.parser(token)
        } catch {
          case e: Exception =>
        }
        val key = keyWord + "," + token + "," + label + "," + chnlid + "," + videoid + "," + eventid
        if (token != "" && token != null && userIdAndDeviceId != "" && !dupMap.contains(key)) {
          list += ((userIdAndDeviceId, Array(time + "-->" + key)))
          dupMap += (key -> "")
        }
      })
      list.toIterator
    }).reduceByKey((a, b) => sort(a, b))
    val dupMap = new mutable.HashMap[String, String]()
    val result = reduceRdd.mapPartitions(par => {
      val list = new ListBuffer[(String, String)]
      par.foreach(x => {
        val arrs = x._2
        var labelNew = ""
        for (request <- arrs) {
          val key = request.split("-->")(1).split(",")
          var keyWord = ""
          var token = ""
          var label = ""
          var chnlid = ""
          var videoid = ""
          var eventid = ""
          if (key.length == 6) {
            keyWord = key(0)
            token = key(1)
            label = key(2)
            chnlid = key(3)
            videoid = key(4)
            eventid = key(5)
          }
          if (keyWord.contains("ad/get_list") || keyWord.contains("channel/get_list") || keyWord.contains("program/get_list") || keyWord.contains("search/search_by_keyword")) {
            labelNew = label
          }
          else {
            if (labelNew != "" && labelNew != null && labelNew != "null" && labelNew != "0") {
              if (chnlid != null && chnlid != "" && chnlid != "null" && !dupMap.contains(chnlid)) {
                //list += (LabelInfo(chnlid, labelNew))
                list += ((chnlid, labelNew))
                dupMap.put(chnlid, "")
              }
              if (videoid != null && videoid != "" && videoid != "null" && !dupMap.contains(videoid)) {
                //list += (LabelInfo(videoid, labelNew))
                list += ((videoid, labelNew))
                dupMap.put(videoid, "")
              }
              if (eventid != null && eventid != "" && eventid != "null" && !dupMap.contains(eventid)) {
                //list += (LabelInfo(eventid, labelNew))
                list += ((eventid, labelNew))
                dupMap.put(eventid, "")
              }
            }
          }
        }
      })
      list.toIterator
    })
    //.toDF("f_video_id", "f_column_id").show()
    val resultMap = new mutable.HashMap[String, String]()
    result.collect.foreach(tuple2 => {
      if (tuple2._1 != null) {
        val serviceId = tuple2._1
        val labelId = tuple2._2
        resultMap.put(serviceId, labelId)
      }
    })
    sparkSession.sparkContext.broadcast(resultMap)
    //result
  }

  def getLabel(lab: String) = {
    var label =  lab
    if (lab.contains("\\|")) {
      label = lab.split("|")(0)
    }
    label
  }

  def parseNginxLog(sparkSession: SparkSession, filePath: String) = {
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    val reduceRdd = sqlContext.read.json(filePath)
      .selectExpr("timestamp as reportTime", "request")
      .filter($"request".contains("accesstoken=") && ($"request".contains("channel/get_list") || $"request".contains("event/get_list")
        || $"request".contains("event/get_info") || $"request".contains("program/get_list")
        || $"request".contains("video/get_info")))
      .mapPartitions(partition => {
        val list = new ListBuffer[(String, Array[String])]
        val dupMap = new mutable.HashMap[String, String]()
        partition.foreach(row => {
          val time = DateUtils.nginxTimeFormate(row.getString(0))
          val hour = if (time.length >= 13) {
            time.substring(0, 13)
          } else {
            time
          }
          val request = row.getString(1)
          val map = stringToMap(request)
          val token = map.getOrElse("accesstoken", "")
          var userIdAndDeviceId = ""
          try {
            userIdAndDeviceId = TokenParser.parser(token)
          } catch {
            case e: Exception =>
          }
          val timeAndRequest = Array(time + "-->" + request)
          if (token != "" && userIdAndDeviceId != "" && !dupMap.contains(request)) {
            list += ((userIdAndDeviceId + hour, timeAndRequest))
            //            list += ((userIdAndDeviceId, timeAndRequest))
            dupMap += (request -> "")
          }
        })
        list.toIterator
      }).reduceByKey((a, b) => sort(a, b), 112)

    val result = reduceRdd.mapPartitions(partition => {
      val list = new ListBuffer[(String, String)]
      val dupMap = new mutable.HashMap[String, String]()
      partition.foreach(x => {
        val tokenArray = x._2
        var labelNew = ""
        for (token <- tokenArray) {
          val map = stringToMap(token.split("-->")(1))
          if (token.contains("channel/get_list") || token.contains("program/get_list")) {
            labelNew = map.getOrElse("label", "")
          } else {
            val chanelId = map.getOrElse("chnlid", "")
            val videoId = map.getOrElse("videoid", "")
            val eventId = map.getOrElse("eventid", "")
            if (labelNew != "") {
              if (chanelId != "" && !dupMap.contains(chanelId)) {
                list += ((chanelId, labelNew))
                dupMap += (chanelId -> "")
              }
              if (videoId != "" && !dupMap.contains(videoId)) {
                list += ((videoId, labelNew))
                dupMap += (videoId -> "")
              }
              if (eventId != "" && !dupMap.contains(eventId)) {
                list += ((eventId, labelNew))
                dupMap += (eventId -> "")
              }
            }
          }
        }
      })
      list.toIterator
    })

    val resultMap = new mutable.HashMap[String, String]()
    result.collect.foreach(tuple2 => {
      val serviceId = tuple2._1
      val labelId = tuple2._2
      resultMap.put(serviceId, labelId)
    })
    sparkSession.sparkContext.broadcast(resultMap)
  }

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

  /**
    * 同一个用户同一台设备数据放到数组中，支持升序排序
    *
    * @return
    */
  def sort(arr1: Array[String], arr2: Array[String]): Array[String] = {
    val buffer = arr1.toBuffer
    buffer ++= arr2
    val result = buffer.toArray
    Sorting.quickSort(result)
    result
  }

  def main(args: Array[String]): Unit = {
    //    val sparkSession = new SparkSession("NginxLogParseForLabel", "")
    //    val day = args(0)
    //        val filePath = LogConstant.NGINX_JSON_LOG_PATH + day + "/*"
    //    val filePath = "/tmp/kuming/nginx_log"
    //         val filePath = "file:///E:\\test\\log\\access.log"
    //  val tmp = parseNginxLog(sparkSession, filePath)
    //        val tmp = parseHiveNginxLog(sparkSession, day)
    //    val map = tmp.value
    //    println(map)
    //    val sparkSession = SparkSession("DemandReport")
    //    val sc = sparkSession.sparkContext
    //    val hiveContext = sparkSession.sqlContext
    //    val time = args(0)
    // parseHiveNginxLog(sparkSession: SparkSession, time: String)

  }


}

