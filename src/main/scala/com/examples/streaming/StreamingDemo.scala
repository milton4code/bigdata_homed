/*
package cn.ipanel.homed.realtime

import java.util

import cn.ipanel.common._
import cn.ipanel.homed.realtime.TokenParser.User
import cn.ipanel.utils.{DBUtils, DateUtils, PropertiUtils, RedisClient}
import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.serializer.StringDecoder
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import redis.clients.jedis.{Response, ScanParams, ScanResult}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * 频道在线用户分析
  *
  * 用户缓存到redis中结构为map second_user|5000001:频道Id|节目ID|结束时间戳
  * spark streaming参数调优 http://blog.csdn.net/u010454030/article/details/54629049
  * http://blog.csdn.net/xiao_jun_0820/article/details/50715623
  *
  * @author liujjy
  *         date 2018/01/13 14:01
  */


object StreamingDemo extends App {
  private lazy val sqlPro = PropertiUtils.init("sql/t_channel_useronline_analytics.properties")

  //-----------------------------------------------------
  //按秒维度存放的用户状态 前缀
  private lazy val SECOND_USER = "second_user"
  /** 全省在线人数 前缀 */
  private lazy val PROVINCE_ONLINE_USER = "province_online"
  /** 全市在线人数 前缀 */
  private lazy val CITY_ONLINE_USER = "city_online"

  //-----------------注册的公共临时表--------------------------
  private lazy val T_PROGRAM = "t_program"
  private lazy val T_LOG = "t_log"
  private lazy val T_USER_REGION = "t_user_region"
  private lazy val T_USER_ALL = "t_user_all"
  private lazy val T_USER_CACHE = "t_user_cache"

  Logger.getLogger(ChannelUserOnlineAnalytics.getClass).setLevel(Level.ERROR)
  //  val topic = args(0)
  //  val duration: Long = args(1).toLong
  val run_log_topic = "run_debug_log_topic"
  val nginx_log_topic = "nginx_debug_log_topic"
  val duration: Long = "3".toLong
  val sparkSession = SparkSession("ChannelLiveAnalytics", "")
  val sparkContext = sparkSession.sparkContext

  //  val zkPath = "/bigdata/channel_live/offset"
  //  val zkHosts= "192.168.18.60:2181,192.168.18.61:2181,192.168.18.63:2181/kafka"
  //  val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
  //  val zkClient = new ZkClient(zkHosts, 30000, 30000)


  //确保在kill任务时，能够处理完最后一批数据，再关闭程序，不会发生强制kill导致数据处理中断，没处理完的数据丢失
  //    sparkContext.getConf.set("spark.streaming.stopGracefullyOnShutdown","true")
  //开启后spark自动根据系统负载选择最优消费速率
  //    sparkContext.getConf.set("spark.streaming.backpressure.enabled","true")


  // Kafka configurations
  val ssc = new StreamingContext(sparkSession.sparkContext, Durations.seconds(duration))
  val run_log_topics = Set(run_log_topic)
  val nginx_log_topics = Set(nginx_log_topic)
  val union_topics = Set(run_log_topic)

  val brokers = "192.168.18.60:9092,192.168.18.61:9092,192.168.18.63:9092"
  val group_id = "channel_live_group_1"
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
    "serializer.class" -> "kafka.serializer.StringEncoder", "group.id" -> group_id, "auto.offset.reset" -> "largest")


  val runlogSream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, run_log_topics)
  val nginxlogSream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, nginx_log_topics)
  val unionStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, union_topics)

  val nginxLogDStream: DStream[User] = filterNignxLog(nginxlogSream)
  val runLogDStream: DStream[Log] = filterLiveLog(runlogSream)


    foreachRdd(nginxLogDStream,runLogDStream)


  //    ssc.remember(Durations.seconds(duration * 3))
  //  ssc.checkpoint("/sparkchkp/newtvguide")
  ssc.start()
  ssc.awaitTermination()

  //
  private[this] def filterNignxLog(nginxlogSream: InputDStream[(String, String)]): DStream[(Int,User)] = {

    nginxlogSream.filter(data => {
      data._2.contains("accesstoken")
    }).map(x => {
      val json = JSON.parseObject(x._2)
      val request = json.getString("request")
      val token = StringUtils.substringBetween(request, "accesstoken=", "&")
      println( "token ==>" + token)
      (new Random().nextInt(10),TokenParser.parser(token))
    })/*.filter( _ .DA != 0 )*/ //过滤无效用户
  }


  private[this] def foreachRdd( nginxLogDStream: DStream[(Int,User)],runLogDSream: DStream[(Int,Log)]) = {
    val sqlContext = sparkSession.sqlContext
   val joinDF = nginxLogDStream.join(runLogDSream)
   joinDF.foreachRDD(r =>{

   })

  }

  //过滤直播类型日志
  def filterLiveLog(kafkaStream: InputDStream[(String, String)]): DStream[(Int,Log)] = {
    kafkaStream.transform(rdd => {
      //      KafkaOffsetManager.saveOffsets(zkClient,zkHosts, zkPath, rdd)
      rdd.flatMap(line => {
        try {
          val data = JSON.parseObject(line._2)
          data.put("receive_time", System.currentTimeMillis() / 1000)
          Some(data)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
            Some(new JSONObject())
          }
        }
      }).filter(x => {
        //过滤无效日志
        x.containsKey("keyword") && x.containsKey("PlayType")
      }) .filter(x => { //过滤直播的日志
        val keyword = x.getString("keyword")
        val playtype = x.getString("PlayType")
        playtype.equalsIgnoreCase("live") /*&& (
            keyword.equalsIgnoreCase("StatisticsVidPlySuccess") || keyword.equalsIgnoreCase("StatisticsVidPlyFinished")
            )*/
      }).map(x => {
        println("=========x==============" + x.getString("DA") + " == " + x.getString("timeunix"))
        (new Random().nextInt(10),Log(x.getIntValue("DA"), x.getLongValue("ProgramId"),
          x.getString("DeviceType"), x.getString("DeviceId"),
          x.getLongValue("timeunix"), x.getLongValue("receive_time"),
          x.getString("keyword")))
      })
    })
  }



  /**
    * 加载用户在线状态
    * @return DA|device_id|device_type|ipaddress|login_time|province_id|city_id|region_id
    */
  def loadUserOnlineStatus(sqlContext: SQLContext): DataFrame = {
    val userStatusTable =
      s"""
         |( SELECT f_user_id as DA,f_device_id as device_id,case f_device_type
         |WHEN 1 then "stb" when 2 then "smartcard"
         |when 3 then "mobile" when 4 then "pad"
         |when 5 then "pc"
         |ELSE "unknown" end device_type,
         |CONCAT(SUBSTR(f_region_id,1,2),"0000") province_id,
         |CONCAT(SUBSTR(f_region_id,1,4),"00") city_id,
         |SUBSTR(f_region_id,1,6) region_id
         | from t_user_online_statistics  ) as user
         """.stripMargin

    val prop = new java.util.Properties
    prop.setProperty("user", DBProperties.USER_IACS)
    prop.setProperty("password", DBProperties.PASSWORD_IACS)

    sqlContext.read.jdbc(DBProperties.JDBC_URL_IACS, userStatusTable, "DA", 1, 99999999, 10, prop)
  }


  /**
    * 加载频道、节目信息
    * 返回字段：channel_id|program_id|chinese_name|english_name|program_name|start_time|end_time
    */
  def loadChannelProgram(sqlContext: SQLContext): DataFrame = {
    val now = DateUtils.getNowDate(DateUtils.YYYY_MM_DD)
    val programTable =
      s"""( SELECT  hes.homed_service_id as channel_id ,cs.chinese_name,cs.english_name,
         |hes.event_id program_id,hes.event_name as program_name ,
         |UNIX_TIMESTAMP(hes.start_time) as start_time,
         |UNIX_TIMESTAMP(DATE_ADD( hes.start_time,INTERVAL duration DAY_SECOND))  as end_time
         |FROM homed_eit_schedule hes
         |INNER JOIN channel_store cs
         |ON hes.homed_service_id = cs.channel_id
         |WHERE start_time >= "$now 00:00:00" and start_time <= "$now 23:59:59" ) as program"""
        .stripMargin

    val programDF = DBUtils.loadMysql(sqlContext, programTable, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    programDF
  }

}
*/
