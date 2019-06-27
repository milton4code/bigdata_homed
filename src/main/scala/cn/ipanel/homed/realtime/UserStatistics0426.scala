//package cn.ipanel.homed.realtime
//
//import java.util
//
//import cn.ipanel.common.{Constant, DBProperties, SparkSession}
//import cn.ipanel.etl.LogConstant
//import cn.ipanel.utils.{PropertiUtils, RedisClient}
//import com.alibaba.fastjson.{JSON, JSONObject}
//import kafka.serializer.StringDecoder
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.{DataFrame, SQLContext}
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Durations, StreamingContext}
//import redis.clients.jedis.{Response, ScanParams, ScanResult}
//
//import scala.collection.mutable.ListBuffer
//
///**
//  * 用户统计
//  * 主要统计开机用户、直播、点播、回看在线用户数
//  */
//object UserStatistics0426 {
//  private lazy val sqlPro = PropertiUtils.init("sql/user_statistics.properties")
//
//  //省市县|终端类型|观看类型
//  val run_log_topic = "run_log_topic"
//  val duration: Long = "20".toLong
//
//  val AREA_TYPE = "city"
//  /** 过期时间 */
//  val EXPIRE_TIME = 30
//
//  val REDIS_INDEX = 5
//
//  //---------------redis 前缀--------------------------------------
//  //按秒维度存放的用户状态 前缀
//  private lazy val SECOND_USER = "second_user"
//
//
//  //------------------临时表---------------
//  // 用户在线临时表
//  private lazy val T_USER_ONLINE = "t_user_online"
//  //redis缓存临时表
//  private lazy val T_CACHE = "t_cache"
//
//
//  def main(args: Array[String]): Unit = {
//
//    val sparkSession = SparkSession("UserStatistics", "")
//    Logger.getLogger(UserStatistics0426.getClass).setLevel(Level.ERROR)
//
//    val ssc = new StreamingContext(sparkSession.sparkContext, Durations.seconds(duration))
//    val run_log_topics = Set(run_log_topic)
//
//    val brokers = "192.168.18.60:9092,192.168.18.61:9092,192.168.18.63:9092"
//    val group_id = "channel_live_group_1"
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
//      "serializer.class" -> "kafka.serializer.StringEncoder", "group.id" -> group_id, "auto.offset.reset" -> "largest")
//
//    val sparkContext = sparkSession.sparkContext
//    //确保在kill任务时，能够处理完最后一批数据，再关闭程序，不会发生强制kill导致数据处理中断，没处理完的数据丢失
//    sparkContext.getConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
//    //开启后spark自动根据系统负载选择最优消费速率
//    sparkContext.getConf.set("spark.streaming.backpressure.enabled", "true")
//    sparkContext.getConf.registerKryoClasses(Array(classOf[CachUser], classOf[Log]))
//
//    val runlogSream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, run_log_topics)
//    val runLogDStream: DStream[Log] = filterLog(runlogSream)
//    foreachRdd(runLogDStream)
//
//    ssc.start()
//    ssc.awaitTermination()
//
//  }
//
//
//  def foreachRdd(runLogDStream: DStream[Log]): Unit = {
//    runLogDStream.foreachRDD(event => {
//
//      val sqlContext: SQLContext = SQLContext.getOrCreate(event.sparkContext)
//      import sqlContext.implicits._
//      val logDF = event.toDF().dropDuplicates(Seq("device_id", "DA", "play_type"))
//      logDF.registerTempTable("t_log")
//      logDF.persist()
//      //      logDF.selectExpr("device_id as log_device_id", "DA", "play_type").show(100, false)
//
//      val userOnlineDF: DataFrame = loadUserOnlineStatus(sqlContext)
//      userOnlineDF.persist()
//      userOnlineDF.registerTempTable(T_USER_ONLINE)
//
//      //加载缓存数据
//      val preCacheUsersDF: DataFrame = loadPreviousUserStatus(sqlContext)
//      preCacheUsersDF.registerTempTable(T_CACHE)
//
//      if (preCacheUsersDF.rdd.isEmpty()) { //缓存数据为空，则说明程序刚启动
//        process(logDF, sqlContext)
//
//        cacheUserToRedis(logDF)
//      } else {
//        val activeUserDF = sqlContext.sql(sqlPro.getProperty("active_user"))
//        if (!activeUserDF.rdd.isEmpty()) {
//          activeUserDF.dropDuplicates(Seq("device_id", "DA", "play_type"))
//          sqlContext.setConf("spark.sql.shuffle.partitions", "400")
//
//          process(activeUserDF, sqlContext)
//
//          val df = activeUserDF.selectExpr("device_id", "device_type", "DA", "province_id",
//            "city_id", "region_id", "play_type", "report_time", "unix_timestamp() as receive_time")
//          cacheUserToRedis(df)
//        }
//
//      }
//      userOnlineDF.unpersist()
//      logDF.unpersist()
//    })
//
//  }
//
//  def process(df: DataFrame, sqlContext: SQLContext) = {
//    df.registerTempTable("t_tmp")
//    //    df.show(100, false)
//    AREA_TYPE.toLowerCase() match {
//      case Constant.PROVINCE => statisticsByProvice(df, sqlContext)
//      case Constant.CITY => statisticsByCity(df, sqlContext)
//    }
//  }
//
//
//  /**
//    * 缓存数据到redis中
//    * redis结构为second_user|设备ID|DA|playType  x._2 结构为 device_type|DA|province_id|city_id|region_id|report_time|receive_time
//    */
//  private[this] def cacheUserToRedis(cacheDF: DataFrame): Unit = {
//    cacheDF.selectExpr("device_type", "DA", "play_type", "from_unixtime(report_time) as report_time", "from_unixtime(receive_time) as receive_time").show(30, false)
//    println("cacheUserToRedis==========" + cacheDF.rdd.isEmpty())
//    if (!cacheDF.rdd.isEmpty()) {
//      val start = System.currentTimeMillis()
//      cacheDF.foreachPartition(it => {
//        val jedis = RedisClient.getRedis()
//        val pipeline = jedis.pipelined()
//        pipeline.select(REDIS_INDEX)
//        val value = new StringBuilder()
//        while (it.hasNext) {
//          val log = it.next()
//          val key = SECOND_USER + Constant.REDIS_SPILT + log.getAs[Long]("device_id") +
//            Constant.REDIS_SPILT + log.getAs[Long]("DA") +
//            Constant.REDIS_SPILT + log.getAs[String]("play_type")
//
//          value.append(log.getAs[String]("device_type")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[String]("province_id")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[String]("city_id")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[String]("region_id")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[Long]("report_time")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[Long]("receive_time"))
//
//          pipeline.set(key, value.toString())
//          pipeline.expire(key, EXPIRE_TIME)
//          value.clear()
//        }
//        pipeline.sync()
//        RedisClient.release(jedis)
//      })
//      val end = System.currentTimeMillis()
//    }
//  }
//
//
//  /**
//    * 统计省网在线用户
//    */
//  def statisticsByProvice(eventDF: DataFrame, sqlContext: SQLContext) = {
//    import sqlContext.sql
//    //1、按省 终端类型|收看类型 统计用户数量
//    val provice_singlg_deviceTypeDF = sql(sqlPro.getProperty("provice_single_deviceType"))
//    //2、按省 市 终端类型|收看类型（单）
//    val provice_city_single_deviceTypeDF = sql(sqlPro.getProperty("provice_city_single_deviceType"))
//    //3、按省 全部终端类型|收看类型（总）
//    val all_provice_all_deviceTypeDF = sql(sqlPro.getProperty("all_provice_all_deviceType"))
//    //4、按省 市 全部终端类型|收看类型（总）
//    val provice_city_all_deviceTypeDF = sql(sqlPro.getProperty("provice_city_all_deviceType"))
//
//    val resultDF = provice_singlg_deviceTypeDF.unionAll(provice_city_single_deviceTypeDF)
//      .unionAll(all_provice_all_deviceTypeDF).unionAll(provice_city_all_deviceTypeDF)
//    resultDF.show()
//  }
//
//  /**
//    * 统计市网在线用户
//    */
//  def statisticsByCity(eventDF: DataFrame, sqlContext: SQLContext) = {
//    import sqlContext.sql
//
//    // 5、按市 终端类型|收看类型（单）
//    val city_single_deviceTypeDF = sql(sqlPro.getProperty("city_single_deviceType"))
//    // 6、按市|区 终端类型|收看类型（单）
//    val city_region_single_deviceTypeDF = sql(sqlPro.getProperty("city_region_single_deviceType"))
//    //7、按市 全部终端类型|收看类型（总）
//    val city_all_deviceTypeDF = sql(sqlPro.getProperty("city_all_deviceType"))
//    //8、按市 区 全部终端类型|收看类型（总）
//    val city_region_all_deviceTypeDF = sql(sqlPro.getProperty("city_region_all_deviceType"))
//    //10、按市　全部终端　全部收看类型　汇总
//    val city_allDF = sql(sqlPro.getProperty("city_all"))
//
//    val resultDF = city_single_deviceTypeDF.unionAll(city_region_single_deviceTypeDF)
//      .unionAll(city_all_deviceTypeDF).unionAll(city_region_all_deviceTypeDF)
//      .unionAll(city_allDF)
//    resultDF.show()
//  }
//
//  //过滤有效日志
//  def filterLog(kafkaStream: InputDStream[(String, String)]): DStream[Log] = {
//    kafkaStream.transform(rdd => {
//      //      KafkaOffsetManager.saveOffsets(zkClient,zkHosts, zkPath, rdd)
//      rdd.flatMap(line => {
//        try {
//          val data = JSON.parseObject(line._2)
//          data.put("receive_time", System.currentTimeMillis() / 1000)
//          Some(data)
//        } catch {
//          case ex: Exception =>
//            ex.printStackTrace()
//            Some(new JSONObject())
//        }
//      }).filter(x => { //过滤直播的日志
//        val keyword = x.getString("keyword")
//        keyword.equalsIgnoreCase(LogConstant.videoSuccess) || keyword.equalsIgnoreCase(LogConstant.videFinished)
//      }).map(x => {
//        //        println("data===filter===========" + x)
//        val regionIdStr = TokenParser.getRegionString(x.getString("accesstoken"))
//        val proviceId = regionIdStr.substring(0, 2) + "0000"
//        val cityId = regionIdStr.substring(0, 4) + "00"
//        val regionId = regionIdStr.substring(0, 6)
//
//        Log(x.getLongValue("DA"), 0L,
//          x.getString("DeviceType"), x.getString("DeviceID"),
//          x.getLongValue("timeunix"), x.getLongValue("receive_time"),
//          "", proviceId, cityId, regionId, x.getString("playtype"))
//      }) /*.filter( "000000" != _.province_id )*/
//    })
//  }
//
//
//  /**
//    * 加载前一次用户状态
//    *
//    * @return device_id|device_type|DA|province_id|city_id|region_id|report_time
//    */
//  private def loadPreviousUserStatus(sqlContext: SQLContext): DataFrame = {
//    val redisResponse = loadUserStatusFromReids()
//    val userStatusDF = redisToDataFrame(redisResponse, sqlContext)
//    userStatusDF
//  }
//
//  /**
//    * redis数据转成DataFrame
//    *
//    * @return device_id|device_type|DA|province_id|city_id|region_id|play_type|report_time|receive_time
//    */
//  private def redisToDataFrame(redisResponse: Map[String, Response[String]], sqlContext: SQLContext): DataFrame = {
//    //    println("========redisToDataFrame======size=======" + redisResponse.size)
//    val buffer = new ListBuffer[CachUser]
//    redisResponse.foreach(x => {
//      val device_id = x._1.split("\\" + Constant.REDIS_SPILT)(1).toLong
//      val DA = x._1.split("\\" + Constant.REDIS_SPILT)(2).toLong
//      val playType = x._1.split("\\" + Constant.REDIS_SPILT)(3)
//      val paras = x._2.get.split("\\" + Constant.REDIS_SPILT)
//      buffer.append(CachUser(device_id, DA, playType, paras(0), paras(1), paras(2), paras(3), paras(4).toLong, paras(5).toLong))
//    })
//
//    val userStatusDF = sqlContext.createDataFrame(buffer)
//    userStatusDF
//  }
//
//
//  /**
//    * 从redis中加载用户状态
//    */
//  private def loadUserStatusFromReids(): Map[String, Response[String]] = {
//    var responses = Map[String, Response[String]]()
//    val jedis = RedisClient.getRedis()
//    jedis.select(REDIS_INDEX)
//
//    val pipeline = jedis.pipelined()
//    var scanCursor = "0"
//    var scanResult: java.util.List[String] = new util.ArrayList[String]()
//
//    // scala遍历java集合时需要导入此类
//    import scala.collection.JavaConversions._
//    do {
//      val scan: ScanResult[String] = jedis.scan(scanCursor, new ScanParams().`match`(SECOND_USER + Constant.REDIS_SPILT + "*"))
//      scanCursor = scan.getStringCursor
//      for (key: String <- scan.getResult) {
//        responses += (key -> pipeline.get(key))
//      }
//    } while (!"0".equals(scanCursor) && !scanResult.isEmpty)
//    pipeline.sync()
//    RedisClient.release(jedis)
//    responses
//  }
//
//  /**
//    * 加载用户在线状态
//    *
//    * @return DA|device_id|device_type|ipaddress|login_time|province_id|city_id|region_id
//    */
//  private def loadUserOnlineStatus(sqlContext: SQLContext): DataFrame = {
//    val userStatusTable =
//      s"""
//         |( SELECT f_user_id as DA,f_device_id as device_id,case f_device_type
//         |WHEN 1 then "stb" when 2 then "smartcard"
//         |when 3 then "mobile" when 4 then "pad"
//         |when 5 then "pc"
//         |ELSE "unknown" end device_type,
//         |CONCAT(SUBSTR(f_region_id,1,2),"0000") province_id,
//         |CONCAT(SUBSTR(f_region_id,1,4),"00") city_id,
//         |SUBSTR(f_region_id,1,6) region_id
//         |from t_user_online_statistics  ) as user
//         """.stripMargin
//
//    val prop = new java.util.Properties
//    prop.setProperty("user", DBProperties.USER_IACS)
//    prop.setProperty("password", DBProperties.PASSWORD_IACS)
//
//    sqlContext.read.jdbc(DBProperties.JDBC_URL_IACS, userStatusTable, "DA", 1, 99999999, 10, prop)
//  }
//
//}
//
