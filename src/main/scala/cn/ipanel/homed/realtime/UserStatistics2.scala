//package cn.ipanel.homed.realtime
//
//import java.util
//
//import cn.ipanel.common._
//import cn.ipanel.etl.LogConstant
//import cn.ipanel.utils.DateUtils.dateToUnixtime
//import cn.ipanel.utils.{DBUtils, DateUtils, PropertiUtils, RedisClient}
//import com.alibaba.fastjson.{JSON, JSONObject}
//import kafka.serializer.StringDecoder
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.sql.types.StructType
//import org.apache.spark.sql.{DataFrame, Row, SQLContext}
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Durations, StreamingContext}
//import redis.clients.jedis.ScanParams.SCAN_POINTER_START
//import redis.clients.jedis.{ScanParams, ScanResult}
//
//import scala.collection.mutable.{ListBuffer, HashMap => scalaMap}
//import scala.collection.{Set => scalaSet}
//
///**
//  * 用户统计
//  * 主要统计开机用户、直播、点播、回看在线用户数
//  */
//
//case class Log3(DA: Long, channel_id: Long, device_type: String, device_id: String,
//                report_time: Long, receive_time: Long, keyword: String,
//                province_id: String, city_id: String, region_id: String, play_type: String,
//                status: Boolean, play_time: Long,time:String)
//
//private[this] case class CachUser2(device_id: Long = 0L, DA: Long = 0L, play_type: String = "", device_type: String = "", province_id: String = "", city_id: String = "", region_id: String = "",
//                                   report_time: Long = 0L, receive_time: Long = 0L)
//
//private[this] case class Cache(DA: Long = 0L, play_type: String = "", device_id: String = "", device_type: String = "", province_id: String = "", city_id: String = "", region_id: String = "",
//                               status: Boolean, report_time: Long,time:String)
//
//
//object UserStatistics2 {
//  private lazy val sqlPro = PropertiUtils.init("sql/user_statistics.properties")
//  private lazy val checkpointPath = "/spark/checkpoint/userstatistics"
//  private lazy val logger = Logger.getLogger(this.getClass())
//
//  //省市县|终端类型|观看类型
//  //  val run_log_topic = "run_log_topic"
//  //------------------临时表---------------
//  // 用户在线临时表
//  private lazy val T_USER_ONLINE = "t_tmp_user_online"
//  //redis缓存临时表
//  private lazy val T_CACHE = "t_cache"
//  /** 过期时间 */
//  val EXPIRE_TIME = 30
//  /** redis 库索引 */
//  val DB_INDEX = 13
//
//
//  val filterFields = Seq("DA", "device_id", "play_type")
//  val fields = Array("DA", "play_type", "device_id")
//  val fields2 = Array("DA", "play_type", "device_id", "device_type", "province_id", "city_id", "region_id", "status")
//
//  def main(args: Array[String]): Unit = {
//    //    Log4jPrintStream.redirectSystemOut()
//    //run_log_topic my_group_id112 city 20 4  30 0
//
//    val run_log_topic: String = args(0)
//    val group_id: String = args(1)
//    val area_type: String = args(2)
//    val duration: Long = args(3).toLong
//    val threads: Int = args(4).toInt
//    val expire_time: Int = args(5).toInt
//    val play_time: Int = args(6).toInt
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
//    val run_log_topics = Set(run_log_topic)
//    val brokers = "192.168.18.60:9092,192.168.18.61:9092,192.168.18.63:9092"
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
//      "serializer.class" -> "kafka.serializer.StringEncoder", "group.id" -> group_id, "auto.offset.reset" -> "largest")
//
//    val regionCode = UserRegion.getRegionCode()
//    val regionCodeBr = sparkContext.broadcast(regionCode)
//
//
//    //确保在kill任务时，能够处理完最后一批数据，再关闭程序，不会发生强制kill导致数据处理中断，没处理完的数据丢失
//    //    sparkContext.getConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
//    //开启后spark自动根据系统负载选择最优消费速率,
//    //    sparkContext.getConf.set("spark.streaming.backpressure.enabled", "true")
//    //注册序列化类
//    sparkContext.getConf.registerKryoClasses(Array(classOf[CachUser2], classOf[Log3], classOf[Cache]))
//
//    val ssc = new StreamingContext(sparkSession.sparkContext, Durations.seconds(duration))
//    val runlogSream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, run_log_topics)
//    val runLogDStream: DStream[Log3] = filterLog(runlogSream, regionCodeBr)
//
//    foreachRdd(runLogDStream, threads, area_type, play_time, expire_time)
//
//
//    ssc.start()
//    //延长数据在程序中的生命周期
//    ssc.remember(Durations.seconds(duration * 5))
//    ssc.awaitTermination()
//
//  }
//
//
//  def foreachRdd(runLogDStream: DStream[Log3], threads: Int, area_type: String, play_time: Int, expire_time: Int): Unit = {
//    runLogDStream.foreachRDD(foreachFunc = event => {
//      val sqlContext: SQLContext = SQLContext.getOrCreate(event.sparkContext)
//      import sqlContext.implicits._
//
//      val logDF = event.toDF()
//      logDF.registerTempTable("t_log")
//
//      logDF.orderBy($"report_time".asc)
//        .selectExpr("DA as eventDa", "play_type", " status", "time as start").show(10000,false)
//
//      //加载缓存数据
//      val preCacheUsersDF: DataFrame = loadPreviousUserStatus(sqlContext)
//      preCacheUsersDF.registerTempTable(T_CACHE)
//
//      //根据用户上报数据分组,得到用户播放类型的开始和结束状态,
//      //主要有3种情况 1.开始1 结束0 ,则说明用户结束了该次播放 ;2.开始1,结束空,则说明用户正在播放; 3.开始空,结束0,则说明用户结束上次播放
//      val s1 = System.currentTimeMillis()
//      val _logGroupDF = transform(logDF, sqlContext) //.drop("report_time")
//
//      _logGroupDF.persist(StorageLevel.MEMORY_ONLY_SER)
//
//
//      _logGroupDF.orderBy($"report_time".asc)
//        .selectExpr("DA as _logGroupDA", "play_type", " status", "time").show(10000,false)
//
//
//      val e1 = System.currentTimeMillis()
//
//
//      if (preCacheUsersDF.rdd.isEmpty) {
//        //        println("=============1=========")
//        processOnlineUser(_logGroupDF, sqlContext, threads, area_type)
//        cacheUserToRedis(_logGroupDF, expire_time, 1)
//
//        //        cacheToMysql(_logGroupDF)
//      } else {
//        //        println("=============2=========")
//        //加载在线用户数据
//        val userOnlineDF: DataFrame = loadUserOnlineStatus(sqlContext)
//        userOnlineDF.registerTempTable(T_USER_ONLINE)
//
//        println("====_logGroupDF==isEmpty=====" + _logGroupDF.rdd.isEmpty())
//        //在线且缓存的用户
//        val cacheDF = preCacheUsersDF.alias("cacheUser")
//          .join(userOnlineDF.alias("onlineUser"), Seq("DA", "device_id"))
//          .selectExpr("DA", "cacheUser.play_type", "device_id", "cacheUser.device_type",
//            "cacheUser.province_id", "cacheUser.city_id", "cacheUser.region_id", "cacheUser.status",
//            "cacheUser.report_time")
//         // .filter("status = true")
//
//        val logMap: scalaMap[String, String] = dataFrameToMap(_logGroupDF)
//        val cacheMap: scalaMap[String, String] = dataFrameToMap(cacheDF)
//
//        val logMapKeys: scalaSet[String] = logMap.keySet
//        val cacheMapKeys: scalaSet[String] = cacheMap.keySet
//
//        //取日志缓存中都有,则说明用户在播放,且有操作
//        val bothInKeys: scalaSet[String] = logMapKeys & cacheMapKeys
//
//        // 日志中有,缓存中没有,(新加入的用户播放或用户有其他操作)
//        val inLogNotCacheKeys: scalaSet[String] = logMapKeys &~ cacheMapKeys
//        //得到只包含开始的播放(新的播放行为)
//        val onlySuccessKeys: scalaSet[String] = inLogNotCacheKeys.filter(_.contains(LogConstant.SUCCESS.toString))
//        //得到已经结束播放操作的记录
//        val onlyFinishedKeys: scalaSet[String] = inLogNotCacheKeys.filter(_.contains(LogConstant.FINISH.toString))
//
//        //取缓存中有,日志中没有,同时需要过滤状态为false的数据,(说明用户一直播放,没有其他操作)
//        val inCacheNotLogKeys: scalaSet[String] = cacheMapKeys &~ logMapKeys
//
//        //通过差集,可以排除在同一用户同一播放行为 在缓存中状态为true,在日志中状态为false的记录
//        // 比如缓存记录为:用户1-点播-true , 日志中记录为:用户-点播-false ,可以通过用户-点播 作为条件 取差集
//        val activedKeys: scalaSet[String] = inCacheNotLogKeys.map(x => x.substring(0, x.lastIndexOf(","))) //取DA device_id play_type
//          .&~(onlyFinishedKeys.map(x => x.substring(0, x.lastIndexOf(","))))
//          .map(_.concat(s",${LogConstant.SUCCESS}"))
//
//        val allMaps = logMap ++ cacheMap
//        val allKeys = bothInKeys.union(onlySuccessKeys).union(activedKeys)
//
//        val resutMap = new scalaMap[String, String]()
//        allKeys.map(key => {
//          resutMap += key -> allMaps(key)
//        })
//
//        val _df = mapToDataFrame(resutMap, sqlContext)
//
//        //        processOnlineUser(_df, sqlContext, threads, area_type)
//        //        processBootUser(userOnlineDF, sqlContext, threads, area_type)
//        cacheUserToRedis(_df, expire_time, 2)
//
//        cacheToMysql(_df)
//      }
//
//      logDF.unpersist()
//      //      logGroupDF.unpersist()
//    })
//
//  }
//
//  /**
//    *
//    * @param  resultMap DA,play_type,device_id,status -> province_id, city_id, region_id, status,report_time
//    * @return
//    */
//  def mapToDataFrame(resultMap: scalaMap[String, String], sqlContest: SQLContext): DataFrame = {
//    import sqlContest.implicits._
//    resultMap.map(m => {
//      val key = m._1.split(",")
//      val value = m._2.split(",")
//      //      println(key.length+" ==key==="+ key.mkString("|"))
//      //      println(value.length  +"==value==="+ value.mkString("|"))
//
//      Cache(key(0).toLong, key(1), key(2), value(0), value(1), value(2), value(3), key(3).toBoolean, value(4).toLong,"")
//    }).toSeq.toDF()
//  }
//
//  /**
//    * dataFrame 转为map
//    *
//    * @param cacheDF dataFrame 结构为 DA, play_type, device_id, device_type, province_id, city_id, region_id, status,report_time
//    * @return 返回map 结构为  DA,play_type,device_id,status -> device_type,province_id,city_id,region_id|report_time
//    */
//  def dataFrameToMap(cacheDF: DataFrame): scalaMap[String, String] = {
//    val map = new scalaMap[String, String]()
//    val key = new StringBuilder()
//    val value = new StringBuilder()
//    cacheDF.map(it => {
//      it.mkString(",")
//    }).collect().foreach(x => {
//      val spilts = x.split(",")
//      key.append(spilts(0)).append(",") //DA
//        .append(spilts(1)).append(",") //play_type
//        .append(spilts(2)).append(",") // device_id
//        .append(spilts(7)) //status
//
//      value.append(spilts(3)).append(",") //device_type
//        .append(spilts(4)).append(",") // province_id
//        .append(spilts(5)).append(",") //city_id
//        .append(spilts(6)).append(",") //region_id
//        .append(spilts(8)) //report_time
//
//      map += key.toString() -> value.toString()
//      key.clear()
//      value.clear()
//    })
//    map
//  }
//
//  private[this] def cacheToMysql(cacheDF: DataFrame) {
//    val cache = cacheDF.selectExpr(s"'${DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS)}' as time", " device_id", "DA", "play_type")
//    DBUtils.saveToHomedData_2(cache, "t_cache_user")
//  }
//
//
//  /**
//    * 将多字段dataFrame转为两个字段的dataFrame
//    * 保留用户最后一次播放
//    *
//    * @return dataFrame结构为 (DA,play_type,device_id,device_type,province_id,city_id) (status,report_time)
//    */
//  private def transform(dataFrame: DataFrame, sqlContext: SQLContext): DataFrame = {
//    val start = System.currentTimeMillis()
//    import sqlContext.implicits._
//    val df = dataFrame.sortWithinPartitions($"report_time".asc) //按上报时间升序排序,以确定put到map中顺序
//      .mapPartitions(it => {
//      val map = new scalaMap[String, String]()
//      val key = new StringBuilder()
//      val value = new StringBuilder()
//      while (it.hasNext) {
//        val log = it.next()
//        key.append(log.getAs[Long]("DA")).append(",")
//        key.append(log.getAs[String]("play_type")).append(",")
//        key.append(log.getAs[String]("device_id")).append(",")
//        key.append(log.getAs[String]("device_type")).append(",")
//        key.append(log.getAs[String]("province_id")).append(",")
//        key.append(log.getAs[String]("city_id")).append(",")
//        key.append(log.getAs[String]("region_id"))
//
//        value.append(log.getAs[Boolean]("status")).append(",")
//          .append(log.getAs[Long]("report_time")).append(",")
//          .append(log.getAs[String]("time"))
//
//        map += key.toString() -> value.toString()
//        key.clear()
//        value.clear()
//      }
//      map.iterator
//    }).map(x => {
//      val log = x._1.split(",")
//      val status = x._2.split(",")
//      Cache(log(0).toLong, log(1), log(2), log(3), log(4), log(5), log(6), status(0).toBoolean, status(1).toLong,status(2))
//    }).toDF()
//
//    //    println("transform===================" + (System.currentTimeMillis() - start) + " ms")
//
//    df
//  }
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
//    //    println("===========load from redis cost " + (end - start) + "ms")
//    redisToDataFrame(redisResponse, sqlContext)
//  }
//
//  /**
//    * redis数据转成DataFrame
//    * @return device_id|DA|playType|device_type|DA|province_id|city_id|region_id|status|report_time
//    */
//  private def redisToDataFrame(redisResponse: Map[String, String], sqlContext: SQLContext): DataFrame = {
//    val buffer = new util.ArrayList[Row]
//    redisResponse.foreach(x => {
//      val keys = x._1.split("\\" + Constant.REDIS_SPILT)
//      val paras = x._2.split("\\" + Constant.REDIS_SPILT)
//      buffer.add(Row(keys(0).toLong, keys(1), paras(0), paras(1), paras(2), paras(3), paras(4), paras(5).toBoolean, paras(6).toLong))
//    })
//    println("redisToDataFrame buffer is :" + buffer.isEmpty)
//    sqlContext.createDataFrame(buffer, getSchema())
//  }
//
//  def getSchema(): StructType = {
//    import org.apache.spark.sql.types._
//    StructType(List(
//      StructField("DA", LongType, nullable = false),
//      StructField("device_id", StringType, nullable = false),
//      StructField("play_type", StringType, nullable = false),
//      StructField("device_type", StringType, nullable = true),
//      StructField("province_id", StringType, nullable = true),
//      StructField("city_id", StringType, nullable = true),
//      StructField("region_id", StringType, nullable = true),
//      StructField("status", BooleanType, nullable = true),
//      StructField("report_time", LongType, nullable = true)
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
//    val scanParams: ScanParams = new ScanParams().count(10000).`match`("*")
//    var cur: String = SCAN_POINTER_START
//
//    // scala遍历java集合时需要导入此类
//    import scala.collection.JavaConversions._
//    do {
//      val scanResult: ScanResult[String] = jedis.scan(cur, scanParams)
//      for (key: String <- scanResult.getResult) {
//        var values: Seq[String] = jedis.zrevrange(key, 0, -1).toSeq
//        responses += (key -> values.head)
//        if (values.size > 1) { // 至少保留一条记录
//          jedis.zrem(key, values.tail: _*)
//        }
//      }
//      cur = scanResult.getStringCursor
//    } while (!(cur == SCAN_POINTER_START))
//    RedisClient.release(jedis)
//
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
//    sqlContext.read.jdbc(DBProperties.JDBC_URL_IACS, userStatusTable, "DA", 1, 99999999, 5, prop)
//  }
//
//
//  //处理在线用户
//  def processOnlineUser(df: DataFrame, sqlContext: SQLContext, threads: Int, area_type: String) = {
//    val start = System.currentTimeMillis()
//    df.registerTempTable("t_tmp")
//    area_type.toLowerCase() match {
//      case Constant.PROVINCE => statisticsByProvice(df, sqlContext, threads)
//      case Constant.CITY =>  statisticsByCity(df, sqlContext, threads)
//
//    }
//    //    println("process===================" + (System.currentTimeMillis() - start) + " ms")
//  }
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
//  /**
//    * 统计市网在线用户
//    */
//  def statisticsByCity(df: DataFrame, sqlContext: SQLContext, threads: Int) = {
//
//    import sqlContext.implicits._
//    val result = df.mapPartitions(x => {
//      val res = ListBuffer[(String, Int)]()
//      while (x.hasNext) {
//        val row = x.next()
//        val device_type = row.getAs[String]("device_type")
//        val play_type = row.getAs[String]("play_type")
//        val province_id = row.getAs[String]("province_id")
//        val city_id = row.getAs[String]("city_id")
//        val region_id = row.getAs[String]("region_id")
//
//        //6 按市 终端类型|收看类型（单）
//        res += (("c1," + province_id + "," + city_id + "," + device_type + "," + play_type, 1))
//        //7、按市|区 终端类型|收看类型（单）
//        res += (("c2," + province_id + "," + city_id + "," + region_id + "," + device_type + "," + play_type, 1))
//        //8、按市 全部终端类型|收看类型（总）
//        res += (("c3," + province_id + "," + city_id + "," + play_type, 1))
//        //9、按市 区 全部终端类型|收看类型（总）
//        res += (("c4," + province_id + "," + city_id + "," + region_id + "," + play_type, 1))
//        //10、按市　全部终端　全部收看类型 总
//        res += (("c5," + province_id + "," + city_id, 1))
//
//      }
//      res.toIterator
//    }).reduceByKey((v1: Int, v2: Int) => v1 + v2 /*, threads*/)
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
//
//  }
//
//  def save(result: DataFrame): Unit = {
//    //    val s1 = System.currentTimeMillis()
//    if (result.rdd.partitions.nonEmpty) {
//      val resultDF = result.selectExpr(s"'${DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS)}' as time",
//        "province_id", "city_id", "region_id", "device_type", "device_type", "play_type", "user_count")
//      DBUtils.saveDataFrameToPhoenixNew(resultDF, Tables.T_USER_ONLINE)
//    }
//    //    val e1 = System.currentTimeMillis()
//    //    println("save==============" + (e1 - s1) + " ms")
//  }
//
//  /**
//    * 缓存数据到redis中
//    * redis结构为 DA|设备ID    report_time  playType|device_type|province_id|city_id|region_id|status|report_time
//    */
//  private[this] def cacheUserToRedis(cacheDF: DataFrame, expire_time: Int, tag: Int): Unit = {
//    //    println("===cacheDF======isEmpty===" + cacheDF.rdd.isEmpty())
//    //    println("===tag=========" + tag)
//    //    cacheDF.printSchema()
//    val start = System.currentTimeMillis()
//    flushDB()
//    if (!cacheDF.rdd.isEmpty()) {
//      //      println("==========cacheDF=====" + cacheDF.rdd.isEmpty())
//      cacheDF.foreachPartition(it => {
//        val jedis = RedisClient.getRedis()
//        val pipeline = jedis.pipelined()
//        pipeline.select(DB_INDEX)
//
//        val redisValue = new StringBuilder()
//        val key = new StringBuilder()
//        while (it.hasNext) {
//          val log = it.next()
//          key
//            .append(log.getAs[Long]("DA")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[String]("device_id"))
//
//          redisValue
//            .append(log.getAs[String]("play_type")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[String]("device_type")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[String]("province_id")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[String]("city_id")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[String]("region_id")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[Boolean]("status")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[Long]("report_time"))
//
//
//          //          pipeline.set(key.toString, redisValue.toString)
//          pipeline.zadd(key.toString(), log.getAs[Long]("report_time"), redisValue.toString())
//          pipeline.expire(key.toString, expire_time)
//
//          key.clear()
//          redisValue.clear()
//        }
//        pipeline.sync()
//        RedisClient.release(jedis)
//
//      })
//    }
//    //    println(" cacheUserToRedis=============" + (System.currentTimeMillis() - start) + " ms ")
//  }
//
//  def flushDB() {
//    //    println("========flushDB=============")
//    val jedis = RedisClient.getRedis()
//    jedis.select(DB_INDEX)
//    jedis.flushDB()
//    RedisClient.release(jedis)
//  }
//
//  //过滤有效日志
//  def filterLog(kafkaStream: InputDStream[(String, String)], regionCodeBr: Broadcast[String]): DStream[Log3] = {
//    kafkaStream.transform(rdd => {
//      val regionCode = regionCodeBr.value
//
//      rdd.flatMap(line => {
//        try {
//          val data = JSON.parseObject(line._2)
//          data.put("receive_time", System.currentTimeMillis() / 1000)
//          Some(data)
//        } catch {
//          case ex: Exception =>
//            Some(new JSONObject())
//        }
//      }).filter(x => { //过滤直播的日志
//        val keyword = x.getString("keyword")
//        keyword.equalsIgnoreCase(LogConstant.videoSuccess) || keyword.equalsIgnoreCase(LogConstant.videoFinished)
//      }).map(x => {
//        //        println("data===filter===========" + x)
//        var regionIdStr = TokenParser.getRegionString(x.getString("accesstoken"))
//        if (Constant.EMPTY_CODE.equals(regionIdStr)) {
//          regionIdStr = regionCode
//        }
//
//        val proviceId = regionIdStr.substring(0, 2) + "0000"
//        val cityId = regionIdStr.substring(0, 4) + "00"
//        val regionId = regionIdStr.substring(0, 6)
//        val statuts = if (LogConstant.videoSuccess.equals(x.getString("keyword"))) LogConstant.SUCCESS else LogConstant.FINISH
//        val time = DateUtils.dateToTimestamp(x.getString("time"),"yyyy-MM-dd HH:mm:ss:SSS")
//        Log3(x.getLongValue("DA"), 0L,
//          x.getString("DeviceType"), x.getString("DeviceID"),
//          time, x.getLongValue("receive_time"),
//          "", proviceId, cityId, regionId, x.getString("playtype"),
//          statuts, x.getString("PlayS").toLong,x.getString("time"))
//      }) .filter(_.DA.toString.startsWith("50311"))
//    })
//  }
//
//     //处理开机用户
//  def processBootUser(onlineUserDF: DataFrame, sqlContext: SQLContext, threads: Int, area_type: String) = {
//    val start = System.currentTimeMillis()
//    area_type.toLowerCase() match {
//      case Constant.CITY => statisticsBootUser(onlineUserDF, sqlContext, threads)
//    }
//    println("process===================" + (System.currentTimeMillis() - start) + " ms")
//  }
//
//  /**
//    * 统计开机用户
//    */
//  def statisticsBootUser(df: DataFrame, sqlContext: SQLContext, threads: Int) = {
//    import sqlContext.implicits._
//    val result = df.mapPartitions(x => {
//      val res = ListBuffer[(String, Int)]()
//      while (x.hasNext) {
//        val row = x.next()
//        val device_type = row.getAs[String]("device_type")
//        val province_id = row.getAs[String]("province_id")
//        val city_id = row.getAs[String]("city_id")
//        val region_id = row.getAs[String]("region_id")
//
//        //6 按市 终端类型（单）
//        res += (("c1," + province_id + "," + city_id + "," + device_type, 1))
//        //7、按市|区 终端类型（单）
//        res += (("c2," + province_id + "," + city_id + "," + region_id + "," + device_type, 1))
//        //8、按市 全部终端类型（总）
//        res += (("c3," + province_id + "," + city_id, 1))
//        //9、按市 区 全部终端类型（总）
//        res += (("c4," + province_id + "," + city_id + "," + region_id, 1))
//        //10、按市　全部终端　 总
//        res += (("c5," + province_id + "," + city_id, 1))
//
//      }
//      res.toIterator
//    }).reduceByKey((v1: Int, v2: Int) => v1 + v2 /*, threads*/)
//      .map { case (group, count) =>
//        var reuslt = Result()
//        val group_fileds = group.split(",")
//        group_fileds(0) match {
//          case "c1" =>
//            reuslt = reuslt.copy(group_fileds(1), group_fileds(2), "all", group_fileds(3), "", count)
//          case "c2" =>
//            reuslt = reuslt.copy(group_fileds(1), group_fileds(2), group_fileds(3), group_fileds(4), "", count)
//          case "c3" =>
//            reuslt = reuslt.copy(group_fileds(1), group_fileds(2), "all", "all", "", count)
//          case "c4" =>
//            reuslt = reuslt.copy(group_fileds(1), group_fileds(2), group_fileds(3), "all", "", count)
//          case "c5" =>
//            reuslt = reuslt.copy(group_fileds(1), group_fileds(2), "all", "all", "all", count)
//          case _ =>
//        }
//        reuslt
//      }.toDF().filter($"province_id" !== "")
//
//    save(result, Tables.T_BOOT_USER)
//  }
//
//  /*
//  [8402]2018-05-12 12:53:37:846 - [INFO] - StatisticsVideoPlayFinished,DA 50311448,DeviceID 3000433488,ProgramID 100055943,DeviceType pc,ProtocolType http,ProgramMethod demand,PlayToken 31618BKJETDEXOP10,FeeType pack,FromIP 192.168.101.222,FromPort 50360,PlayS 100,DurationS 0,ByteRate 0,URI /playurl?protocol=http&accesstoken=R5AF66BAFU5090E023KB2D6FB50IDE65A8C0P8M2FFB118V10402Z6B7EDWDE139CE63F3&verifycode=3000433488&programid=100055943&playtoken=31618BKJETDEXOP10&relocateme=true&playtype=demand,Msg video start download(maybe playing on other server)
//   */
//  //accessToken keyWord DA DeviceType DeviceID reportTime receive_time  playtype PlayS
//
//  def save(result: DataFrame, table: String): Unit = {
//    //    val s1 = System.currentTimeMillis()
//    if (result.rdd.partitions.nonEmpty) {
//      val resultDF = result.selectExpr(s"'${DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS)}' as time",
//        "province_id", "city_id", "region_id", "device_type", "device_type", "user_count")
//      DBUtils.saveDataFrameToPhoenixNew(resultDF, table)
//    }
//  }
//
//
//
//
//  def getToken(uri: String): String = {
//    val str2 = uri.substring(uri.indexOf("accesstoken=") + "accesstoken=".length)
//    str2.substring(0, str2.indexOf("&"))
//  }
//
//  def getKeyWord(str: String): String = {
//    str.trim.substring(0, str.indexOf(",") - 1)
//  }
//
//  def getReportTime(value: String): Long = {
//    val reportTime = value.substring(value.indexOf("]") + 1, value.lastIndexOf(":"))
//    DateUtils.dateToUnixtime(reportTime)
//  }
//
//}
//
