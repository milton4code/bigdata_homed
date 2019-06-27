//package cn.ipanel.homed.realtime
//
//import java.util
//
//import cn.ipanel.common._
//import cn.ipanel.utils.{DBUtils, DateUtils, PropertiUtils, RedisClient}
//import com.alibaba.fastjson.{JSON, JSONObject}
//import kafka.serializer.StringDecoder
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.{DataFrame, SQLContext}
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Durations, StreamingContext}
//import redis.clients.jedis.{Response, ScanParams, ScanResult}
//
//import scala.collection.mutable.ListBuffer
//
///**
//  * 频道在线用户分析
//  *
//  * 用户缓存到redis中结构为map second_user|5000001:频道Id|节目ID|结束时间戳
//  * spark streaming参数调优 http://blog.csdn.net/u010454030/article/details/54629049
//  * http://blog.csdn.net/xiao_jun_0820/article/details/50715623
//  *
//  * @author liujjy
//  *         date 2018/01/13 14:01
//  */
//
//
//
//object ChannelUserOnlineAnalytics extends App {
//  private lazy val sqlPro = PropertiUtils.init("sql/t_channel_useronline_analytics.properties")
//
//
//  //---------------redis 前缀--------------------------------------
//  //按秒维度存放的用户状态 前缀
//  private lazy val SECOND_USER = "second_user"
//  /** 全省在线人数 前缀 */
//  private lazy val PROVINCE_ONLINE_USER = "province_online"
//  /** 全市在线人数 前缀 */
//  private lazy val CITY_ONLINE_USER = "city_online"
//  /** 全省各地市各终端在线人数 */
//  private lazy val province_PIE_USER = "province_pie"
//  /** 全市各区各终端在线人数 */
//  private lazy val CITY_PIE_USER = "city_pie"
//
//
//  //-----------------注册的公共临时表--------------------------
//  private lazy val T_PROGRAM = "t_program"
//  private lazy val T_LOG = "t_log"
//  private lazy val T_USER_ALL = "t_user_all"
//  private lazy val T_USER_CACHE = "t_user_cache"
//
//  //---------------------常量--------------------------------
//  private lazy val TIME_FORMAT = "yyyyMMddHHmmss"
//  private lazy val KEYWORD_SUCCESS = "StatisticsVideoPlaySuccess"
//  private lazy val KEYWORD_FINSHED = "StatisticsVideoPlayFinished"
//
//
//  Logger.getLogger(ChannelUserOnlineAnalytics.getClass).setLevel(Level.ERROR)
//  //  val topic = args(0)
//  //  val duration: Long = args(1).toLong
//  val run_log_topic = "run_log_topic"
//  val nginx_log_topic = "nginx_log_topic"
//  val duration: Long = "5".toLong
//  val AREA_TYPE = "province"
//  val sparkSession = SparkSession("ChannelLiveAnalytics", "")
//  val sparkContext = sparkSession.sparkContext
//
//  //  val zkPath = "/bigdata/channel_live/offset"
//  //  val zkHosts= "192.168.18.60:2181,192.168.18.61:2181,192.168.18.63:2181/kafka"
//  //  val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//  //  val zkClient = new ZkClient(zkHosts, 30000, 30000)
//
//
//  //确保在kill任务时，能够处理完最后一批数据，再关闭程序，不会发生强制kill导致数据处理中断，没处理完的数据丢失
//  //    sparkContext.getConf.set("spark.streaming.stopGracefullyOnShutdown","true")
//  //开启后spark自动根据系统负载选择最优消费速率
//  //    sparkContext.getConf.set("spark.streaming.backpressure.enabled","true")
//  sparkContext.getConf.registerKryoClasses(Array(classOf[UserSatus], classOf[Log2]))
//
//
//  // Kafka configurations
//  val ssc = new StreamingContext(sparkSession.sparkContext, Durations.seconds(duration))
//  val run_log_topics = Set(run_log_topic)
//
//  val brokers = "192.168.18.60:9092,192.168.18.61:9092,192.168.18.63:9092"
//  val group_id = "channel_live_group_1"
//  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
//    "serializer.class" -> "kafka.serializer.StringEncoder", "group.id" -> group_id, "auto.offset.reset" -> "largest")
//
//  val runlogSream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, run_log_topics)
//  val runLogDStream: DStream[Log2] = filterLiveLog(runlogSream)
//
//  foreachRdd(runLogDStream)
//
//  // ssc.remember(Durations.seconds(duration * 3))
//  ssc.start()
//  ssc.awaitTermination()
//
//
//  private def foreachRdd(runLogDSream: DStream[Log2]): Unit = {
//    runLogDSream.foreachRDD(event => {
//      val sqlContext = SQLContext.getOrCreate(event.sparkContext)
//      import sqlContext.implicits._
//
//      val previousStatusDF = loadPreviousUserStatus()
//      previousStatusDF.persist(StorageLevel.DISK_ONLY)
//      previousStatusDF.registerTempTable(T_USER_CACHE)
//
//      //      previousStatusDF.selectExpr("device_id as preDeviceId").show(11)
//
//      val programDF = loadChannelProgram(sqlContext)
//      programDF.persist(StorageLevel.DISK_ONLY)
//      programDF.registerTempTable(T_PROGRAM)
//
//      val userOnlineDF = loadUserOnlineStatus(sqlContext)
//      userOnlineDF.persist(StorageLevel.DISK_ONLY)
//      userOnlineDF.registerTempTable(T_USER_ALL)
//
//      //      userOnlineDF.selectExpr("device_id as onlineDeviceId").show(11)
//
//      val logDF: DataFrame = wrapLog(event.toDF, sqlContext)
//      logDF.persist(StorageLevel.DISK_ONLY)
//      logDF.registerTempTable(T_LOG)
//
//      //      logDF.show()
//      //      event.toDF.selectExpr("device_id as eventDeviceId").show(11)
//      //      logDF.selectExpr("device_id as logDeviceId", "da ").show(30)
//
//      if (previousStatusDF.rdd.isEmpty()) { //缓存为空，则直接根据log做计算
//        val sql =
//          s"""select a.DA,a.device_id, a.device_type,a.channel_id, a.program_id,a.report_time,
//             |b.province_id,b.city_id,b.region_id
//             |from $T_LOG  a
//             |inner join $T_USER_ALL b
//             |on a.device_id=b.device_id
//             | """.stripMargin
//        val cacheDF = sqlContext.sql(sql)
//        cacheUserStatusToReids(cacheDF)
//      } else {
//        val baseLogDF = wrapBaseData(sqlContext, logDF, previousStatusDF, userOnlineDF)
//        baseLogDF.registerTempTable("t_base_log")
//        previousStatusDF.unpersist()
//        baseLogDF.persist(StorageLevel.DISK_ONLY)
//
//        logDF.selectExpr("device_id as logDeviceId","report_time","receive_time").show(200)
//        baseLogDF.selectExpr("device_id as baseDeviceId","report_time").show(200)
//
//        //统计所有频道在线人数
//        //        statisticsAllChannelOnlieUser(sqlContext, baseLogDF)
//        // 统计饼图,折线图人数
//        //        statisticsPieAndLineChart(sqlContext)
//        //统计列表数据
//        //        processPageListData(sqlContext, baseLogDF)
//
//        //缓存上一次用户状态
//        println("==========cacheToReids====2=========")
//
//        //        baseLogDF.selectExpr("device_id as baseDeviceId", "da ").show(30)
//        //        cacheUserStatusToReids(baseLogDF)
//
//      }
//
//      logDF.unpersist()
//      userOnlineDF.unpersist()
//      programDF.unpersist()
//    })
//
//  }
//
//
//  /**
//    * 处理页面列表数据
//    *
//    */
//  def processPageListData(sqlContext: SQLContext, resultDF: DataFrame): Unit = {
//    println("==============processPageListData=================")
//    val time = DateUtils.getNowDate(TIME_FORMAT)
//    val processTime = System.currentTimeMillis() / 1000
//    AREA_TYPE.toLowerCase match {
//      case Constant.PROVINCE => saveAllProvincePageListData(time, processTime, sqlContext)
//      case Constant.CITY => saveAllCityPageListData(time, processTime, sqlContext)
//    }
//
//  }
//
//  //保存全省 页面列表数据
//  private def saveAllProvincePageListData(time: String, processTime: Long, sqlContext: SQLContext): Unit = {
//    val cityDF = getCityPageListData(time, sqlContext)
//    // 5\省|全市|全终端
//    val province_all_city_all_devicetype = getPageListDataFrame(sqlContext, time, "page_province_all_city_all_devicetype")
//    //6\省|市|区|全终端
//    val province_city_all_region_all_devicetype = getPageListDataFrame(sqlContext, time, "page_province_city_all_region_all_devicetype")
//
//
//    cityDF.unionAll(province_all_city_all_devicetype).unionAll(province_city_all_region_all_devicetype)
//      .registerTempTable("t_page_union")
//
//    val result = sqlContext.sql(sqlPro.getProperty("page_union_program").replace("#processTime#", processTime + ""))
//    /*result.selectExpr("time as provinceTime","ratings").show(200)*/
//    DBUtils.saveDataFrameToPhoenixNew(result, Tables.T_CHANNEL_PAGE_BY_SECOND)
//  }
//
//  //保存全市 页面列表数据
//  private def saveAllCityPageListData(time: String, processTime: Long, sqlContext: SQLContext): Unit = {
//    getCityPageListData(time, sqlContext).
//      selectExpr().registerTempTable("t_page_union")
//    val result = sqlContext.sql(sqlPro.getProperty("page_union_program").replace("#processTime#", processTime + ""))
//    DBUtils.saveDataFrameToPhoenixNew(result, Tables.T_CHANNEL_PAGE_BY_SECOND)
//  }
//
//  private def getCityPageListData(time: String, sqlContext: SQLContext): DataFrame = {
//    //1\省|市|区|单终端
//    val province_city_region_single_devicetype = getPageListDataFrame(sqlContext, time, "page_province_city_region_single_devicetype")
//    //2\省|市|单终端
//    val province_city_single_devicetype = getPageListDataFrame(sqlContext, time, "page_province_city_single_devicetype")
//
//    // 3\省|单终端
//    val province_single_devicetype = getPageListDataFrame(sqlContext, time, "page_province_single_devicetype")
//    // 4\ 全省|全终端
//    val all_province_all_devicetype = getPageListDataFrame(sqlContext, time, "page_all_province_all_devicetype")
//    province_city_region_single_devicetype.unionAll(province_city_single_devicetype)
//      .unionAll(province_single_devicetype).unionAll(all_province_all_devicetype)
//  }
//
//
//  private[this] def getPageListDataFrame(sqlContext: SQLContext, time: String, sqlKey: String): DataFrame = {
//    val province_city_region_single_devicetype = sqlPro.getProperty(sqlKey).replace("#time#", time).replace("#duration#", duration.toString)
//    sqlContext.sql(province_city_region_single_devicetype)
//  }
//
//
//  /**
//    * 统计饼图和折线图数据
//    */
//  private[this] def statisticsPieAndLineChart(sqlContext: SQLContext): Unit = {
//    AREA_TYPE.toLowerCase match {
//      case Constant.PROVINCE => saveAllProvincePieAndLineChart(sqlContext)
//      case Constant.CITY => saveAllCityTerminalDeviceOnline(sqlContext)
//    }
//  }
//
//  //全市 饼图和折线图
//  private def saveAllCityTerminalDeviceOnline(sqlContext: SQLContext): Unit = {
//    //  按区 各终端统计(包含该区所有终端用户数量)
//    val pie_region_single_device = sqlContext.sql(sqlPro.getProperty("pie_region_single_device"))
//    //饼图数据
//    cacheTerminalPieUserToRedis(pie_region_single_device, CITY_PIE_USER, "region_id")
//    //折线图数据
//    saveAllCityLineChartToHabse(pie_region_single_device)
//  }
//
//  private def saveAllCityLineChartToHabse(pie_region_single_device: DataFrame): Unit = {
//    val time = DateUtils.getNowDate(TIME_FORMAT)
//    val terminalLineUserDF = pie_region_single_device.selectExpr(s"$time as time", "province_id", "city_id", "region_id", "device_type", "online_user")
//    DBUtils.saveDataFrameToPhoenixNew(terminalLineUserDF, Tables.T_TERMINAL_DEVICE_ONLINE)
//  }
//
//  //全省 饼图和折线图
//  private def saveAllProvincePieAndLineChart(sqlContext: SQLContext): Unit = {
//    // 1 按区 各终端统计(包含该区所有终端用户数量)
//    val pie_region_single_device = sqlContext.sql(sqlPro.getProperty("pie_region_single_device"))
//    //2 按市 各终端统计(包含该市所有终端用户数量)
//    val pie_city_single_device = sqlContext.sql(sqlPro.getProperty("pie_city_single_device"))
//    //3 按省 各终端统计
//    val pie_provicnce_single_device = sqlContext.sql(sqlPro.getProperty("pie_provicnce_single_device"))
//    //饼图数据
//    cacheTerminalPieUserToRedis(pie_city_single_device, province_PIE_USER, "city_id")
//    //折线图数据
//    saveAllProvinceLineChartToHabse(pie_region_single_device, pie_city_single_device, pie_provicnce_single_device)
//
//  }
//
//  private def saveAllProvinceLineChartToHabse(pie_region_single_device: DataFrame, pie_city_single_device: DataFrame,
//                                              pie_provicnce_single_device: DataFrame): Unit = {
//    val time = DateUtils.getNowDate(TIME_FORMAT)
//    val terminalLineUserDF = pie_region_single_device.drop("total_user").drop("rate")
//      .unionAll(pie_city_single_device.drop("total_user").drop("rate"))
//      .unionAll(pie_provicnce_single_device)
//      .selectExpr(s"$time as time", "province_id", "city_id", "region_id", "device_type", "online_user")
//    DBUtils.saveDataFrameToPhoenixNew(terminalLineUserDF, Tables.T_TERMINAL_DEVICE_ONLINE)
//  }
//
//  /**
//    * 缓存各终端在线人数到redis
//    *
//    * @param data           数据
//    * @param reidsKeyPrefix province_pie或者city_pie
//    * @param keyWord        city_id或者region_id
//    */
//  private[this] def cacheTerminalPieUserToRedis(data: DataFrame, reidsKeyPrefix: String, keyWord: String): Unit = {
//    data.foreachPartition(f = it => {
//      val jedis = RedisClient.getRedis()
//      val pipeline = jedis.pipelined()
//      pipeline.select(3)
//
//      val map = new util.HashMap[String, String]()
//      while (it.hasNext) {
//        val row = it.next()
//        val key = reidsKeyPrefix + Constant.REDIS_SPILT + row.getAs[String](keyWord)
//        map.put("device_type", row.getAs[String]("device_type"))
//        map.put("online_user", row.getAs[Long]("online_user").toString)
//        map.put("total_user", row.getAs[Long]("total_user").toString)
//        map.put("rate", row.getAs[Double]("rate").toString)
//        pipeline.hmset(key, map)
//        map.clear()
//      }
//      pipeline.sync()
//      RedisClient.release(jedis)
//    })
//
//  }
//
//  /**
//    * 统计（频道）在线用户数量
//    */
//  private[this] def statisticsAllChannelOnlieUser(sqlContext: SQLContext, baseLogDF: DataFrame): Unit = {
//    val distinctUserDF = baseLogDF.dropDuplicates(Seq("device_id")).select("da", "device_type", "province_id", "city_id", "region_id", "device_id")
//    distinctUserDF.registerTempTable("t_distinct_user")
//
//    AREA_TYPE.toLowerCase match {
//      case Constant.PROVINCE => saveAllProvinceOnlineUsers(sqlContext)
//      case Constant.CITY => saveAllCityOnlineUsers(sqlContext)
//    }
//  }
//
//  //全市在线人数统计
//  private def saveAllCityOnlineUsers(sqlContext: SQLContext): Unit = {
//    val all_city_watchDF = sqlContext.sql(
//      s"""
//         |select a.online_user , a.online_user/b.toal_user rate ,
//         |concat( a.province_id,'${Constant.REDIS_SPILT}',b.city_id ) as region,b.toal_user
//         |from (select count(1) online_user ,province_id ,city_id from t_distinct_user where device_type='stb' group by province_id,city_id ) a
//         |inner join (select count(1) toal_user ,province_id ,city_id from $T_USER_ALL where device_type='stb' group by province_id,city_id ) b
//         |on a.province_id = b.province_id and a.city_id = b.city_id
//       """.stripMargin)
//    cacheOnlineUserToReids(all_city_watchDF, CITY_ONLINE_USER)
//  }
//
//  //保存全省在线人数统计
//  private def saveAllProvinceOnlineUsers(sqlContext: SQLContext): Unit = {
//    val all_province_watchDF = sqlContext.sql(
//      s"""
//         |select a.online_user , a.online_user/b.toal_user rate ,a.province_id as region,b.toal_user
//         |from (select count(1) online_user ,province_id  from t_distinct_user where device_type='stb' group by province_id ) a
//         |inner join (select count(1) toal_user ,province_id  from $T_USER_ALL  where device_type='stb' group by province_id ) b
//         |on a.province_id = b.province_id
//       """.stripMargin)
//    cacheOnlineUserToReids(all_province_watchDF, PROVINCE_ONLINE_USER)
//  }
//
//  /**
//    * 缓存在线用户信息到redis
//    * <p>用于图表中在线用户数
//    */
//  private[this] def cacheOnlineUserToReids(onlineUserDF: DataFrame, preFix: String): Unit = {
//    onlineUserDF.foreachPartition(it => {
//      val jedis = RedisClient.getRedis()
//      val pipeline = jedis.pipelined()
//      pipeline.select(3)
//
//      val map = new util.HashMap[String, String]()
//      while (it.hasNext) {
//        val row = it.next()
//        val key = preFix + Constant.REDIS_SPILT + row.getAs[String]("region")
//        map.put("online_user", row.getAs[Long]("online_user").toString)
//        map.put("toal_user", row.getAs[Long]("toal_user").toString)
//        map.put("rate", row.getAs[Double]("rate").toString)
//        pipeline.hmset(key, map)
//        map.clear()
//      }
//      pipeline.sync()
//      RedisClient.release(jedis)
//    })
//  }
//
//
//  /**
//    * 拼接用户、区域、观看时长、点击数等基础信息
//    *
//    * @return
//    * DA|channel_id|device_type|channel_id|chinese_name|
//    * province_id|city_id|region_id|program_id|program_name|
//    * click|watch_time|report_time|device_id
//    */
//  private[this] def wrapBaseData(sqlContext: SQLContext, logDF: DataFrame,
//                                 preStatusDF: DataFrame, userOnlineDF: DataFrame): DataFrame = {
//
//    val startLogDF = sqlContext.sql(s"select * from $T_LOG  where keyword='$KEYWORD_SUCCESS' ")
//    val finishLogDF = sqlContext.sql(s"select * from  $T_LOG where keyword='$KEYWORD_FINSHED' ")
//    startLogDF.persist(StorageLevel.DISK_ONLY)
//    finishLogDF.persist(StorageLevel.DISK_ONLY)
//
//    //1、对于一个频道，日志上报中同时包含开始、结束 ，观看时长的计算直接相减即可
//    val compute_1 = compute(sqlContext, startLogDF, finishLogDF)
//
//    //2、只包含了开始,观看时长需要结合上一次状态一起来计算,
//    //需要排除掉1中关键字为StatisticsVideoPlaySuccess用户
//    val fields = Seq("DA", "device_type", "channel_id", "chinese_name", "province_id", "city_id", "region_id",
//      "program_id", "program_name","report_time","device_id").toArray
//
//    compute_1.persist(StorageLevel.DISK_ONLY)
//    val start_distinct_user = startLogDF.selectExpr(fields:_*).except(compute_1.selectExpr(fields:_*))
//    val compute_2 = compute(sqlContext, preStatusDF, start_distinct_user)
//
//    //3、对于一个频道，日志上报只包含了结束,观看时长需要结合上一次状态一起来计算
//    //需要排除掉1中用户关键字为StatisticsVideoPlayFinished用户
//    val finish_distinct_user = finishLogDF.selectExpr(fields:_*).except(compute_1.selectExpr(fields:_*))
//    val compute_3 = compute(sqlContext, preStatusDF, finish_distinct_user)
//
//    //4、没有日志上报，需要通过缓存状态和在线状态一起来计算
//    //Ⅲ对于一个频道，日志只上报了一次开始，之后没有上报,需要将2，3中已从缓存中计算的用户去掉
//    val distinctUserDF = start_distinct_user.select("device_id").unionAll(finish_distinct_user.select("device_id")).distinct()
//    val compute_4 = computeByOnlineUser(sqlContext, distinctUserDF, userOnlineDF)
//
//    //日志上报的数据，但和缓存不能关联起来,这部分数据需要单独计算
////    logDF.selectExpr(fields:_*).except(compute_1.selectExpr(fields:_*)).except()
//    val resutlDF = compute_1.unionAll(compute_2).unionAll(compute_3).unionAll(compute_4)
//
//
//        compute_1.selectExpr("device_id com1_deviceId","click").show(10)
//        compute_2.selectExpr("device_id com2_deviceId","click").show(10)
//        compute_3.selectExpr("device_id com3_deviceId","click").show(10)
//        distinctUserDF.selectExpr("device_id distinct_deviceId").show(10)
//        userOnlineDF.selectExpr("device_id online_deviceId").show(10)
//        compute_4.selectExpr("device_id com4_deviceId","click") .show(10)
//        resutlDF.selectExpr("device_id result_deviceId","click") .show(10)
//
//        val reportTime = compute_4.selectExpr("reprot_time")
//        sqlContext.sql(s"select * from $T_PROGRAM where $reportTime >= start_time and $reportTime <= end_time").show(100)
//    compute_1.unpersist()
//    startLogDF.unpersist()
//    finishLogDF.unpersist()
//
//    resutlDF
//  }
//
//  /**
//    *
//    * 通过用户在线状态来计算日志在缓存中但日志没有记录的用户
//    *
//    * @return DA|channel_id|device_type|channel_id|
//    *         | province_id|city_id|region_id|program_id|program_name|
//    *         click|watch_time|report_time
//    */
//  private[this] def computeByOnlineUser(sqlContext: SQLContext, distinctUserDF: DataFrame, userOnlineDF: DataFrame): DataFrame = {
//
//    distinctUserDF.registerTempTable("t_distinct_user")
//    val sql =
//      s"""select aa.da,aa.device_type,
//         |   aa.channel_id,bb.chinese_name,
//         |   aa.province_id,aa.city_id,aa.region_id,
//         |   case when aa.report_time >= bb.start_time  and aa.report_time <= bb.end_time then aa.program_id
//         |  	    when aa.report_time > bb.end_time then bb.program_id
//         |    else bb.program_id end as program_id,
//         |    bb.program_name,
//         |   0 as click,
//         |  $duration as watch_time,
//         |  ($duration + aa.report_time) as report_time,
//         |  aa.device_id
//         |from (
//         |  select a.DA,a.channel_id,a.program_id,a.report_time,a.device_type, a.province_id,a.city_id, a.region_id,a.device_id
//         |  from $T_USER_CACHE  a
//         |  left join t_distinct_user b
//         |  on a.device_id=b.device_id
//         |  where b.device_id is null
//         |) aa
//         | inner join  $T_PROGRAM  bb
//         | on aa.channel_id = bb.channel_id
//         |""".stripMargin
//
//    val sql1 =
//      s"""select aa.da,aa.device_type,
//         |   aa.channel_id, 0 as chinese_name,
//         |   aa.province_id,aa.city_id,aa.region_id,
//         |    0 as  program_id,
//         |    0 as program_name,
//         |   0 as click,
//         |  $duration as watch_time,
//         |  ($duration + aa.report_time) as report_time,
//         |  aa.device_id
//         |from (
//         |  select a.DA,a.channel_id,a.program_id,a.report_time,a.device_type, a.province_id,a.city_id, a.region_id,a.device_id
//         |  from $T_USER_CACHE  a
//         |  left join t_distinct_user b
//         |  on a.device_id=b.device_id
//         |  where b.device_id is null
//         |) aa
//         |""".stripMargin
//    sqlContext.sql(sql1)
//  }
//
//  /**
//    * 计算上报日志中包含了直播中开始、结束标识态的观看时间和点击次数
//    *
//    * @return DA|channel_id|device_type|channel_id|chinese_name
//    *         |province_id|city_id|region_id
//    *         |program_id|program_name
//    *         |click|watch_time|report_time|device_id
//    */
//  private[this] def compute(sqlContext: SQLContext, firstDF: DataFrame, sencodDF: DataFrame) = {
//    firstDF.registerTempTable("t_first")
//    sencodDF.registerTempTable("t_sencod")
//    sqlContext.sql(
//      s"""
//         |select b.DA,b.device_type,
//         |b.channel_id,b.chinese_name,
//         |b.province_id,b.city_id,b.region_id,
//         |b.program_id,b.program_name,
//         | 1 as click,
//         |if(a.report_time is null,$duration,b.report_time-a.report_time) as watch_time,
//         |b.report_time,
//         |b.device_id
//         |from t_first a
//         |right join t_sencod b
//         |on a.device_id = b.device_id and a.channel_id = b.channel_id
//       """.stripMargin)
//    //and b.keyword='$keyword'
//  }
//
//  /**
//    * 缓存数据到redis中
//    * redis结构为second_user|设备ID x._2 结构为 频道Id|节目ID|结束时间戳|设备类型|省|市|县|用户ID
//    */
//  private[this] def cacheUserStatusToReids(cacheDF: DataFrame): Unit = {
//
//    if (!cacheDF.rdd.isEmpty()) {
//      val start = System.currentTimeMillis()
//      cacheDF.foreachPartition(it => {
//        val jedis = RedisClient.getRedis()
//        val pipeline = jedis.pipelined()
//        pipeline.select(2)
//
//        val value = new StringBuilder()
//        while (it.hasNext) {
//          val log = it.next()
//          val key = SECOND_USER + Constant.REDIS_SPILT + log.getAs[Long]("device_id")
//          value.append(log.getAs[Long]("channel_id")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[Int]("program_id")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[Long]("report_time")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[String]("device_type")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[String]("province_id")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[String]("city_id")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[String]("region_id")).append(Constant.REDIS_SPILT)
//            .append(log.getAs[Long]("DA"))
//
//          pipeline.set(key, value.toString())
//          pipeline.expire(key, 60 * 60 * 2)
//          value.clear()
//        }
//        pipeline.sync()
//        RedisClient.release(jedis)
//      })
//      val end = System.currentTimeMillis()
//      println("---------cach to redis cost----------------" + (end - start) )
//    }
//  }
//
//  /**
//    * 将日志和用户信息、节目频道对应信息组合在一起
//    *
//    * @return DA,channel_id,device_type,device_id, report_time,receive_time,keyword
//    *         province_id,city_id, region_id，
//    *         program_id,chinese_name,program_name,start_time,end_time,
//    *         device_id
//    */
//  private[this] def wrapLog(log: DataFrame, sqlContext: SQLContext): DataFrame = {
//    //log   DA, channel_id,device_type,device_id, report_time,receive_time,keyword
//    log.registerTempTable("t_tmp_log")
//    sqlContext.sql(
//      s"""
//         |select a.DA,a.channel_id,a.device_type,a.device_id,a.report_time,a.receive_time,a.keyword,
//         | a.province_id,a.city_id,a.region_id,
//         | a.device_id,
//         | c.program_id,c.chinese_name,c.program_name,c.start_time,c.end_time
//         |from t_tmp_log a
//         |inner join  $T_PROGRAM c   on a.channel_id = c.channel_id and a.report_time >= c.start_time and a.report_time <= c.end_time
//      """.stripMargin)
//  }
//
//
//
//
//  //过滤直播类型日志
//  def filterLiveLog(kafkaStream: InputDStream[(String, String)]): DStream[Log2] = {
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
//      })/*.filter(x => {
//        //过滤无效日志
//        x.containsKey("keyword") && x.containsKey("playtype")
//      })*/.filter(x => { //过滤直播的日志
//        val keyword = x.getString("keyword")
//        val playtype = x.getString("playtype")
//        playtype.equalsIgnoreCase("live") && (
//          keyword.equalsIgnoreCase(KEYWORD_SUCCESS) || keyword.equalsIgnoreCase(KEYWORD_FINSHED)
//          )
//      }).map(x => {
//        println("data===filter===========" + x.getString("DA") + " == " + x.getString("timeunix"))
//        val regionIdStr = TokenParser.getRegionString(x.getString("accesstoken"))
//        val proviceId = regionIdStr.substring(0, 2) + "0000"
//        val cityId = regionIdStr.substring(0, 4) + "00"
//        val regionId = regionIdStr.substring(0, 6)
//
//        Log2(x.getIntValue("DA"), x.getLongValue("ProgramID"),
//          x.getString("DeviceType"), x.getString("DeviceID"),
//          x.getLongValue("timeunix"), x.getLongValue("receive_time"),
//          x.getString("keyword"),proviceId,cityId,regionId,"",1)
//      })/*.filter( "000000" != _.province_id )*/
//    })
//  }
//
//  /**
//    * 加载前一次用户状态
//    *
//    * @return device_id|channel_id|program_id|report_time|province_id|city_id|region_id|DA
//    */
//  def loadPreviousUserStatus(): DataFrame = {
//    val redisResponse = loadUserStatusFromReids()
//    val userStatusDF = redisToDataFrame(redisResponse)
//    userStatusDF
//  }
//
//
//  /**
//    * redis数据转成DataFrame
//    *
//    * @return device_id|channel_id|program_id|report_time|device_type|province_id|city_id|region_id|DA\
//    */
//  protected def redisToDataFrame(redisResponse: Map[String, Response[String]]): DataFrame = {
//    println("========redisToDataFrame======size=======" + redisResponse.size)
//    val buffer = new ListBuffer[UserSatus]
//    redisResponse.foreach(x => {
//      val device_id = x._1.split("\\" + Constant.REDIS_SPILT)(1).toLong
//      val paras = x._2.get.split("\\" + Constant.REDIS_SPILT)
//      buffer.append(UserSatus(device_id, paras(0).toLong, paras(1).toInt, paras(2).toLong, paras(3), paras(4), paras(5), paras(6), paras(7).toLong))
//    })
//
//    val userStatusDF = sparkSession.sqlContext.createDataFrame(buffer)
//    userStatusDF
//  }
//
//
//  /**
//    * 从redis中加载用户状态
//    */
//  def loadUserStatusFromReids(): Map[String, Response[String]] = {
//    var responses = Map[String, Response[String]]()
//    val jedis = RedisClient.getRedis()
//    jedis.select(2)
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
//  def loadUserOnlineStatus(sqlContext: SQLContext): DataFrame = {
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
//         | from t_user_online_statistics  ) as user
//         """.stripMargin
//
//    val prop = new java.util.Properties
//    prop.setProperty("user", DBProperties.USER_IACS)
//    prop.setProperty("password", DBProperties.PASSWORD_IACS)
//
//    sqlContext.read.jdbc(DBProperties.JDBC_URL_IACS, userStatusTable, "DA", 1, 99999999, 10, prop)
//  }
//
//
//  /**
//    * 加载频道、节目信息
//    * 返回字段：channel_id|program_id|chinese_name|english_name|program_name|start_time|end_time
//    */
//  def loadChannelProgram(sqlContext: SQLContext): DataFrame = {
//    val now = DateUtils.getNowDate(DateUtils.YYYY_MM_DD)
//    val programTable =
//      s"""( SELECT  hes.homed_service_id as channel_id ,cs.chinese_name,cs.english_name,
//         |hes.event_id program_id,hes.event_name as program_name ,
//         |UNIX_TIMESTAMP(hes.start_time) as start_time,
//         |UNIX_TIMESTAMP(DATE_ADD( hes.start_time,INTERVAL duration DAY_SECOND))  as end_time
//         |FROM homed_eit_schedule hes
//         |INNER JOIN channel_store cs
//         |ON hes.homed_service_id = cs.channel_id
//         |WHERE start_time >= "$now 00:00:00" and start_time <= "$now 23:59:59" ) as program"""
//        .stripMargin
//
//    val programDF = DBUtils.loadMysql(sqlContext, programTable, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
//    programDF
//  }
//
//
//
//
//
//
//}
