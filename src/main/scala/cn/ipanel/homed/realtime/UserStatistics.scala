//package cn.ipanel.homed.realtime
//
//import java.util
//
//import cn.ipanel.common.{Constant, DBProperties, SparkSession, Tables}
//import cn.ipanel.etl.{Log2, LogConstant}
//import cn.ipanel.utils.{DBUtils, DateUtils, PropertiUtils, RedisClient}
//import com.alibaba.fastjson.{JSON, JSONObject}
//import com.examples.Log4jPrintStream
//import kafka.serializer.StringDecoder
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.types.StructType
//import org.apache.spark.sql.{DataFrame, Row, SQLContext}
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Durations, StreamingContext}
//import redis.clients.jedis.ScanParams.SCAN_POINTER_START
//import redis.clients.jedis.{Jedis, ScanParams, ScanResult}
//
///**
//  * 用户统计
//  * 主要统计开机用户、直播、点播、回看在线用户数
//  */
//
//private case class CachUser(device_id: Long = 0L, DA: Long = 0L, play_type: String = "", device_type: String = "", province_id: String = "", city_id: String = "", region_id: String = "",
//                            report_time: Long = 0L, receive_time: Long = 0L)
//
//object UserStatistics {
//  private lazy val sqlPro = PropertiUtils.init("sql/user_statistics.properties")
//  private lazy val checkpointPath = "/spark/checkpoint/userstatistics"
//
//  //省市县|终端类型|观看类型
//  //  val run_log_topic = "run_log_topic"
//
//
//  /** 过期时间 */
//  val EXPIRE_TIME = 30
//  /** redis 库索引 */
//  val DB_INDEX = 13
//
//  //---------------redis 前缀--------------------------------------
//  //按秒维度存放的用户状态 前缀
//  private lazy val SECOND_USER = "second_user"
//
//
//  //------------------临时表---------------
//  // 用户在线临时表
//  private lazy val T_USER_ONLINE = "t_tmp_user_online"
//  //redis缓存临时表
//  private lazy val T_CACHE = "t_cache"
//
//
//  def main(args: Array[String]): Unit = {
//    Log4jPrintStream.redirectSystemOut()
//
//
//    lazy val run_log_topic: String = args(0)
//    lazy val group_id: String = args(1)
//    lazy val duration: Long = args(2).toLong
//    lazy val threads: Int = args(3).toInt
//    lazy val area_type: String = args(4)
//
//    val sparkSession = SparkSession("UserStatistics", "")
//    val sparkContext = sparkSession.sparkContext
//
//
//    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
//    Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)
//    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
//    Logger.getLogger("org.apache.hive").setLevel(Level.WARN)
//
//
//    val run_log_topics = Set(run_log_topic)
//    val brokers = "192.168.18.60:9092,192.168.18.61:9092,192.168.18.63:9092"
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
//      "serializer.class" -> "kafka.serializer.StringEncoder", "group.id" -> group_id, "auto.offset.reset" -> "largest")
//
//    //确保在kill任务时，能够处理完最后一批数据，再关闭程序，不会发生强制kill导致数据处理中断，没处理完的数据丢失
//    sparkContext.getConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
//    //开启后spark自动根据系统负载选择最优消费速率
//    sparkContext.getConf.set("spark.streaming.backpressure.enabled", "true")
//    //注册序列化类
//    sparkContext.getConf.registerKryoClasses(Array(classOf[CachUser2], classOf[Log2]))
//
//    val ssc = new StreamingContext(sparkSession.sparkContext, Durations.seconds(duration))
//    val runlogSream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, run_log_topics)
//    val runLogDStream: DStream[Log2] = filterLog(runlogSream)
//
//    foreachRdd(runLogDStream, threads, area_type)
//
//
//    ssc.start()
//    ssc.awaitTermination()
//
//  }
//
//
//  def foreachRdd(runLogDStream: DStream[Log2], threads: Int, area_type: String): Unit = {
//    runLogDStream.foreachRDD(event => {
//      val sqlContext: SQLContext = SQLContext.getOrCreate(event.sparkContext)
//      import sqlContext.implicits._
//
//      val logDF = event.toDF()
//      logDF.registerTempTable("t_log")
//      logDF.persist()
//
//      val userOnlineDF: DataFrame = loadUserOnlineStatus(sqlContext)
//      userOnlineDF.persist()
//      userOnlineDF.registerTempTable(T_USER_ONLINE)
//
//      //加载缓存数据
//      val preCacheUsersDF: DataFrame = loadPreviousUserStatus(sqlContext)
//      preCacheUsersDF.registerTempTable(T_CACHE)
//
//      val userLastLogDF = sqlContext.sql(sqlPro.getProperty("active_user"))
//
//      //      logDF.selectExpr("da as logDF_da", "status", "play_type", "from_unixtime(report_time) as report_time", "from_unixtime(receive_time) as receive_time").show(1000, false)
//      //      userLastLogDF.selectExpr("da as lastLog_da", "start_status", "end_status", "play_type", "from_unixtime(report_time) as report_time", "from_unixtime(receive_time) as receive_time").show(1000, false)
//
//
//
//      if (preCacheUsersDF.rdd.isEmpty()) { //缓存数据为空，则说明程序刚启动
//        val _userLastLogDF =  userLastLogDF
//        process(_userLastLogDF, sqlContext, threads, area_type)
//
//        println("====缓存数据为空==" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
//        cacheUserToRedis(userLastLogDF)
//        //        cacheToMysql(logDF)
//      } else {
//
//        preCacheUsersDF.selectExpr("DA as redis_Da", "play_type", "device_type", "from_unixtime(report_time) as report_time", "from_unixtime(receive_time) as receive_time")
//          .show(1000, false)
//
//        val activeUserDF = sqlContext.sql(sqlPro.getProperty("active_user")).dropDuplicates(Seq("device_id", "DA", "play_type"))
//        process(activeUserDF, sqlContext, threads, area_type)
//        val df = activeUserDF.selectExpr("device_id", "device_type", "DA", "province_id"
//          , "city_id", "region_id", "play_type", "report_time", " receive_time") //.filter($"status" === LogConstant.SUCCESS) //只统计正在播放的用户
//        cacheUserToRedis(df)
//        //        cacheToMysql(df)
//      }
//      userOnlineDF.unpersist()
//      logDF.unpersist()
//    })
//
//  }
//
//  def process(df: DataFrame, sqlContext: SQLContext, threads: Int, area_type: String) = {
//    df.registerTempTable("t_tmp")
//    area_type.toLowerCase() match {
//      case Constant.PROVINCE => statisticsByProvice(df, sqlContext, threads)
//      case Constant.CITY => statisticsByCity(df, sqlContext, threads)
//    }
//  }
//
//
//  private[this] def cacheToMysql(cacheDF: DataFrame) {
//    val cache = cacheDF.selectExpr(s"'${DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS)}' as time", " device_id", "DA", "play_type")
//    //    DBUtils.saveDataFrameToPhoenixNew(cache,"t_cache_user")
//    DBUtils.saveToHomedData_2(cache, "t_cache_user")
//  }
//
//  /**
//    * 缓存数据到redis中
//    * redis结构为second_user|设备ID|DA  x._2 结构为 playType|device_type|DA|province_id|city_id|region_id|report_time|receive_time
//    */
//  private[this] def cacheUserToRedis(cacheDF: DataFrame): Unit = {
//
//    if (!cacheDF.rdd.isEmpty()) {
//      val start = System.currentTimeMillis()
//
//      cacheDF.foreachPartition(it => {
//        val jedis = RedisClient.getRedis()
//        val pipeline = jedis.pipelined()
//        pipeline.select(DB_INDEX)
//
//        val redisValue = new StringBuilder()
//        while (it.hasNext) {
//          val log = it.next()
//          val key = SECOND_USER + Constant.REDIS_SPILT + log.getAs[Long]("device_id") +
//            Constant.REDIS_SPILT + log.getAs[Long]("DA")
//
//          redisValue
//            .append(log.getAs[String]("play_type")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[String]("device_type")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[String]("province_id")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[String]("city_id")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[String]("region_id")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[Long]("report_time")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[Long]("receive_time"))
//          /*    .append(Constant.REDIS_SPILT)
//             .append(log.getAs[Int]("status"))*/
//
//
//          //          println("redis :: " + key + "===" + redisValue)
//
//          pipeline.zadd(key, log.getAs[Long]("receive_time") + log.getAs[Long]("report_time"), redisValue.toString())
//          pipeline.expire(key, EXPIRE_TIME)
//          redisValue.clear()
//        }
//        pipeline.sync()
//        RedisClient.release(jedis)
//
//      })
//      val end = System.currentTimeMillis()
//      println("cacheUserToRedis =-=-=-=--cost=" + (end - start) + " ms")
//    }
//  }
//
//
//  /**
//    * 统计省网在线用户
//    */
//  def statisticsByProvice(eventDF: DataFrame, sqlContext: SQLContext, threads: Int) = {
//
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
//  }
//
//
//  /**
//    * 统计市网在线用户
//    */
//  def statisticsByCity(df: DataFrame, sqlContext: SQLContext, threads: Int) = {
//    println("eventDF=========isEmpty===" + df.rdd.isEmpty())
//    import sqlContext.implicits._
//    //    eventDF.selectExpr("da as event_da", "Da", "play_type").show()
//    val result = df.mapPartitions(x => {
//      //      var res = ListBuffer[(String, Int)]()
//      var res = List[(String, Int)]()
//      while (x.hasNext) {
//        val row = x.next()
//        //        val device_id = row.getAs[Long]("device_id")
//        val device_type = row.getAs[String]("device_type")
//        //        val DA = row.getAs[Long]("DA")
//        val play_type = row.getAs[String]("play_type")
//
//        val province_id = row.getAs[String]("province_id")
//        val city_id = row.getAs[String]("city_id")
//        val region_id = row.getAs[String]("region_id")
//
//        //6 按市 终端类型|收看类型（单）
//        res = res.::(("c1," + province_id + "," + city_id + "," + device_type + "," + play_type, 1))
//        //7、按市|区 终端类型|收看类型（单）
//        res = res.::(("c2," + province_id + "," + city_id + "," + region_id + "," + device_type + "," + play_type, 1))
//        //8、按市 全部终端类型|收看类型（总）
//        res = res.::(("c3," + province_id + "," + city_id + "," + play_type, 1))
//        //9、按市 区 全部终端类型|收看类型（总）
//        res = res.::(("c4," + province_id + "," + city_id + "," + region_id + "," + play_type, 1))
//        //10、按市　全部终端　全部收看类型 总
//        res = res.::(("c5," + province_id + "," + city_id, 1))
//
//      }
//      res.toIterator
//    }).reduceByKey((v1: Int, v2: Int) => v1 + v2, threads)
//      .map { case (group, count) =>
//        var reuslt = Result()
//        val group_fileds = group.split(",")
//        group_fileds(0) match {
//          case "c1" =>
//            reuslt = reuslt.copy(group_fileds(1), group_fileds(2), "all", group_fileds(3), group_fileds(4), count)
//          case "c2" =>
//            reuslt = reuslt.copy(group_fileds(1), group_fileds(2), group_fileds(3), group_fileds(4), group_fileds(5), count)
//          case "c3" =>
//            reuslt = reuslt.copy(group_fileds(1), group_fileds(2), "all", "all", group_fileds(3), count)
//          case "c4" =>
//            reuslt = reuslt.copy(group_fileds(1), group_fileds(2), group_fileds(3), "all", group_fileds(4), count)
//          case "c5" =>
//            reuslt = reuslt.copy(group_fileds(1), group_fileds(2), "all", "all", "all", count)
//          case _ =>
//        }
//        reuslt
//      }.toDF().filter($"province_id" !== "")
//
//    save(result)
//  }
//
//
//  def save(result: DataFrame): Unit = {
//    if (!result.rdd.isEmpty()) {
//      val resultDF = result.selectExpr(s"'${DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS)}' as time",
//        "province_id", "city_id", "region_id", "device_type", "device_type", "play_type", "user_count")
//      resultDF.show(100000, false)
//      DBUtils.saveDataFrameToPhoenixNew(resultDF, Tables.T_USER_ONLINE)
//    }
//  }
//
//  //过滤有效日志
//  def filterLog(kafkaStream: InputDStream[(String, String)]): DStream[Log2] = {
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
//        val statuts = if (LogConstant.videoSuccess.equals(x.getString("keyword"))) LogConstant.SUCCESS else LogConstant.FINISH
//
//        Log2(x.getLongValue("DA"), 0L,
//          x.getString("DeviceType"), x.getString("DeviceID"),
//          x.getLongValue("timeunix"), x.getLongValue("receive_time"),
//          "", proviceId, cityId, regionId, x.getString("playtype"), statuts)
//      }) /*.filter( "000000" != _.province_id )*/
//      //              .filter(_.DA == 55313240)
//    })
//  }
//
//
//  /**
//    * 加载前一次用户状态
//    *
//    * @return device_id|DA|playType|device_type|DA|province_id|city_id|region_id|report_time|receive_time
//    */
//  private def loadPreviousUserStatus(sqlContext: SQLContext): DataFrame = {
//    val start = System.currentTimeMillis()
//    val redisResponse = loadUserStatusFromReids()
//    val end = System.currentTimeMillis()
//    println("====load from redis cost " + (end - start) + "ms")
//    redisToDataFrame(redisResponse, sqlContext)
//  }
//
//  /**
//    * redis数据转成DataFrame
//    *
//    * @return device_id|DA|playType|device_type|DA|province_id|city_id|region_id|report_time|receive_time
//    */
//  private def redisToDataFrame(redisResponse: Map[String, String], sqlContext: SQLContext): DataFrame = {
//    val buffer = new util.ArrayList[Row]
//    println("redisResponse==isEmpty=" + redisResponse.isEmpty)
//
//    redisResponse.foreach(x => {
//      val device_id = x._1.split("\\" + Constant.REDIS_SPILT)(1).toLong
//      val DA = x._1.split("\\" + Constant.REDIS_SPILT)(2).toLong
//      val paras = x._2.split("\\" + Constant.REDIS_SPILT)
//      //      buffer.add(Row(device_id, DA, paras(0), paras(1), paras(2), paras(3), paras(4), paras(5).toLong, paras(6).toLong,paras(7).toInt))
//      buffer.add(Row(device_id, DA, paras(0), paras(1), paras(2), paras(3), paras(4), paras(5).toLong, paras(6).toLong))
//    })
//    println("redisToDataFrame buffer is :" + buffer.isEmpty)
//    sqlContext.createDataFrame(buffer, getCacheUserSchema())
//  }
//
//  def getCacheUserSchema(): StructType = {
//    import org.apache.spark.sql.types._
//    StructType(List(
//      StructField("device_id", LongType, nullable = false),
//      StructField("DA", LongType, nullable = false),
//      StructField("play_type", StringType, nullable = false),
//      StructField("device_type", StringType, nullable = true),
//      StructField("province_id", StringType, nullable = true),
//      StructField("city_id", StringType, nullable = true),
//      StructField("region_id", StringType, nullable = true),
//      StructField("report_time", LongType, nullable = true),
//      StructField("receive_time", LongType, nullable = true)
//      //      ,StructField("receive_time", LongType, nullable = true)
//    ))
//  }
//
//  /**
//    * 从redis中加载用户状态
//    */
//  private def loadUserStatusFromReids(): Map[String, String] = {
//    var responses = Map[String, String]()
//    val jedis = RedisClient.getRedis()
//    jedis.select(DB_INDEX)
//
//    val scanParams: ScanParams = new ScanParams().count(1000).`match`("*")
//    var cur: String = SCAN_POINTER_START
//
//    // scala遍历java集合时需要导入此类
//    import scala.collection.JavaConversions._
//    do {
//      val scanResult: ScanResult[String] = jedis.scan(cur, scanParams)
//      for (key: String <- scanResult.getResult) {
//        val values: Seq[String] = jedis.zrevrange(key, 0, -1).toSeq
//        if (values.size > 1) { // 至少保留一条记录
//          jedis.zrem(key, values.tail: _*) // : _*  scala传递可变参特殊用法
//        }
//        responses += (key -> values.head)
//      }
//      cur = scanResult.getStringCursor
//    } while (!(cur == SCAN_POINTER_START))
//    RedisClient.release(jedis)
//
//    responses
//  }
//
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
//    sqlContext.read.jdbc(DBProperties.JDBC_URL_IACS, userStatusTable, "DA", 1, 99999999, 5, prop)
//  }
//
//
//}
////
