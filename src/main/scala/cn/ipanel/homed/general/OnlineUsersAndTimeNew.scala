package cn.ipanel.homed.general


import cn.ipanel.common._
import cn.ipanel.utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer
/**
  *
  * 开机用户及时长统计
  * Author :liujjy
  * Date : 2019-5-03 10:58
  * version: 1.0
  * Describle:  开机人数,总用户数,历史开机人数,总时长
  **/

object OnlineUsersAndTimeNew {

  private case class UserOnLineIacs(f_user_id: String = "", f_terminal: String = "", f_onLineTime: Long = 0L)

  //1,day 2,week,3 month,4 year
  private val DAY = "1"
  private val WEEK = "2"
  private val MONTH = "3"
  private val YEAR = "4"
  private val SEVEN_DAYS = "5"


  def main(args: Array[String]): Unit = {

    val time = new LogUtils.Stopwatch("OnlineUsersAndTimeNew 开机人数等模块 ")

    if (args.length != 1) {
      println("参数不正确,请输入日期 [20180528]")
      sys.exit(-1)
    }
    val sparkSession = SparkSession("OnlineUsersAndTimeNew")
    val hiveContext = sparkSession.sqlContext
    val day = args(0)

    val nextDay = DateUtils.getAfterDay(day, targetFormat = DateUtils.YYYY_MM_DD)
    val homedUserDeviceDF = UserUtils.getHomedUserDevice(hiveContext, nextDay)

    homedUserDeviceDF.persist().registerTempTable("t_homed_users")

    val homedInfoDF = UserUtils.getHomeInfo(hiveContext,nextDay).selectExpr("home_id","user_id")

    //关联主账号,确保设备都是唯一
    val disUserDF = homedUserDeviceDF.join(homedInfoDF,Seq("home_id","user_id")).persist()

    disUserDF.persist().registerTempTable("t_user")

    process(day, hiveContext, homedUserDeviceDF)

    homedUserDeviceDF.unpersist()

    println(time)

  }

  def process(day: String, sqlContext: HiveContext, homedUserDF: DataFrame): Unit = {

    delete(day)

    val nextDay = DateUtils.getAfterDay(day, targetFormat = DateUtils.YYYY_MM_DD)
    val tokenUserDF = UserUtils.getValidateUser(sqlContext, nextDay) //.registerTempTable("t_token_user")
    tokenUserDF.persist(StorageLevel.DISK_ONLY)

    statisticsByDay(day, DAY, sqlContext, homedUserDF,tokenUserDF)
    statisticsByCountType(day, WEEK, sqlContext, homedUserDF,tokenUserDF)
    statisticsByCountType(day, SEVEN_DAYS, sqlContext, homedUserDF,tokenUserDF)
    statisticsByCountType(day, MONTH, sqlContext, homedUserDF,tokenUserDF)
//    statisticsByCountType(day, YEAR, sqlContext, homedUserDF)
  }

  /**
    * 根据统计类型 来统计开机用户数
    * WEEK MONTH YEAR
    */
  private def statisticsByCountType(day: String, count_type: String, sqlContext: HiveContext
                                    , homedUserDF: DataFrame,tokenUserDF:DataFrame): Unit = {
    println("statisticsByCountType----count_type----" + count_type )

    //防止临时表重名
    scala.util.Try(sqlContext.dropTempTable("t_valid_user"))

    sqlContext.sql("use bigdata")

    //有效的在线用户
    val allOnlineuserDF = getAllOnlineUser(sqlContext, day, count_type)
    allOnlineuserDF.persist().registerTempTable("t_valid_user")


    //在线用户数 ,返回字段 f_user_count,f_type,f_terminal 其余的则是区域信息 f_province_name,f_province_id等
    val onlineUserCountDF = getOnlineUserByType(day, count_type, sqlContext)
    //用户总数(开机率分母),返回字段f_total_users,f_type,f_terminal 其余的则是区域信息
    val totalUserCountDF = getTotalUser(day, sqlContext, homedUserDF,tokenUserDF)
    //历史在线人数 f_hist_user_count
    val histOnlinUserCountDF = getHistoryUser(day, sqlContext, homedUserDF,count_type,tokenUserDF)
    // 统计用户在线时长 f_play_time|f_type|f_terminal|f_region_id
    val playTimeDF = getPlayTimeByCountType(day, count_type, sqlContext)
    import sqlContext.implicits._
    val resultDF =  totalUserCountDF.alias("b")
      .join(onlineUserCountDF.alias("a"), $"a.f_region_id" === $"b.f_region_id" && $"a.f_terminal" === $"b.f_terminal" ,"left")
      .join(histOnlinUserCountDF.alias("c"),$"b.f_region_id" === $"c.f_region_id" && $"b.f_terminal" === $"c.f_terminal", "left")
      .join(playTimeDF.alias("d"), $"b.f_region_id" === $"d.f_region_id" && $"b.f_terminal" === $"d.f_terminal","left")
      .selectExpr(s"'$day' as f_date", s"'$count_type' f_type", "b.f_terminal", "b.f_province_id", "b.f_province_name",
        "b.f_city_id", "b.f_city_name", "b.f_region_id", "b.f_region_name"
        , "nvl(round(d.f_play_time,5),0.0) f_play_time "
        , "nvl(a.f_user_count,0) f_user_count"
        , " nvl(b.f_total_users,0) f_total_users"
        , "nvl(c.f_hist_user_count,0) f_hist_user_count"
      )

//    resultDF.show(10,false)
    DBUtils.saveToHomedData_2(resultDF, Tables.T_ONLINE_USER_TIME)
  }


  private def getPlayTimeByCountType(day: String, count_type: String, sqlContext: HiveContext) = {
    val startDay = getStartDay(day, count_type)
    println("getPlayTimeByCountType--day--" + startDay +  "---" + day)

    val querySql =
      s"""
         |(SELECT sum(f_play_time) f_play_time ,f_terminal,f_region_id, '$count_type' as f_type
         |from t_online_user_time
         |where f_date> "$startDay" and f_date <= "$day" and f_type='$DAY'
         |GROUP BY f_terminal,f_region_id,f_city_id) t
      """.stripMargin

    DBUtils.loadMysql(sqlContext, querySql, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)

  }

  //-----------------------------------------------------------

  /**
    * 按天 来计算
    */
  // allHomedUsersDF : f_device_id|f_device_type|f_province_id|f_city_id|f_region_id
  def statisticsByDay(day: String, count_type: String, sqlContext: HiveContext,
                      homedUserDF: DataFrame,tokenUserDF:DataFrame): Unit = {
    //防止临时表重名
    scala.util.Try(sqlContext.dropTempTable("t_valid_user"))

    sqlContext.sql("use bigdata")

    //有效的在线用户
    val allOnlineuserDF = getAllOnlineUser(sqlContext, day, DAY)
    allOnlineuserDF.persist().registerTempTable("t_valid_user")

    //在线用户数 ,返回字段 f_user_count,f_type,f_terminal 其余的则是区域信息 f_province_name,f_province_id等
    val onlineUserCountDF = getOnlineUserByType(day, count_type, sqlContext)

    //用户总数(开机率分母),返回字段f_total_users,f_type,f_terminal 其余的则是区域信息
    val totalUserCountDF = getTotalUser(day, sqlContext, homedUserDF,tokenUserDF)

    //历史在线人数 f_hist_user_count|f_type|f_terminal 其余的则是区域信息
    val histOnlinUserCountDF = getHistoryUser(day, sqlContext, homedUserDF,count_type,tokenUserDF)

    // 统计用户在线时长 f_play_time|f_type|f_terminal|f_region_id
    import org.apache.spark.sql.functions._
    val playTimeDF = getOnlineTime(day, sqlContext, homedUserDF.selectExpr("user_id"))
        .groupBy("f_terminal","f_region_id").agg(sum("f_play_time"))
        .withColumnRenamed("sum(f_play_time)","f_play_time")
//    playTimeDF.show(2000)
    import sqlContext.implicits._
    val resultDF = totalUserCountDF.alias("a")
      .join(onlineUserCountDF.alias("b"), $"a.f_region_id" === $"b.f_region_id" && $"a.f_terminal" === $"b.f_terminal","left" )
      .join(histOnlinUserCountDF.alias("c"),$"a.f_region_id" === $"c.f_region_id" && $"a.f_terminal" === $"c.f_terminal" ,"left")
      .join(playTimeDF.alias("d"), $"a.f_region_id" === $"d.f_region_id" && $"a.f_terminal" === $"d.f_terminal","left")
      .selectExpr(s"'$day' as f_date", s"'$DAY' f_type", "a.f_terminal", "a.f_province_id", "a.f_province_name",
        "a.f_city_id", "a.f_city_name", "a.f_region_id", "a.f_region_name"
        , "nvl(round(d.f_play_time/3600,5),0.0) f_play_time "
        , "nvl(b.f_user_count,0) f_user_count"
        , " nvl(a.f_total_users,0) f_total_users"
        , "nvl(c.f_hist_user_count,0) f_hist_user_count"
      )

    DBUtils.saveToHomedData_2(resultDF, Tables.T_ONLINE_USER_TIME)
  }


  /**
    * 在线时长
    *
    * @return f_play_time|f_type|f_terminal|f_region_id
    */
  private def getOnlineTime(day: String, hiveContext: HiveContext, homedUserDF: DataFrame): DataFrame = {
    //首先需要确定数据源
    //需要对websoket和用户上报的数据进行有效性过滤,
    //对于单一源 直接计算开机时长即可
    //对于同时配置了websoket和用户上报,则需要对用户再次过滤,因为用户可能只存在于某个源中.
    // 计算开机时长只支持4中情况:用户上报,run日志,websocket和用户上报run日志混合

    var df:DataFrame = getDefaultDF(hiveContext)
    val resources = CluserProperties.LOG_SOURCE

    //只有用户上报
    if (resources.contains(SourceType.USER_REPORT)  && ! resources.contains(SourceType.WEBSOCKET)) {
      //直接利用心跳数据粗略计算开机时长,一个心跳时间一般默认60S
      val reportDF = getReportLog(hiveContext,day).join(homedUserDF,Seq("user_id"))
      reportDF.persist().registerTempTable("t_report")

      println("------只有用户上报------------")
      df = CluserProperties.STATISTICS_TYPE match {
        case StatisticsType.STB => onlinTimeByDevice("t_report", hiveContext)
        case StatisticsType.MOBILE => onlinTimeByMobile("t_report", hiveContext)
        case StatisticsType.ALL =>
          val df2 = onlinTimeByMobile("t_report", hiveContext)

          if(df2 !=null &&  df2.count()>0){
            onlinTimeByDevice("t_report", hiveContext).unionAll(df2).registerTempTable("t_union1")
          }else{
            onlinTimeByDevice("t_report", hiveContext).registerTempTable("t_union1")
          }

          hiveContext.sql(
            """
              |select sum(f_play_time) f_play_time ,f_terminal,f_region_id
              |from t_union1
              |group by f_terminal,f_region_id
            """.stripMargin)
      }
      reportDF.unpersist()
    }
    //只有websocket
    if (! resources.contains(SourceType.USER_REPORT)  && resources.contains(SourceType.WEBSOCKET)) {
      println("------只有websocket------------")

      val webSocketDF = getWebsocketData(day, homedUserDF, hiveContext).join(homedUserDF,Seq("user_id"))
      webSocketDF.persist().registerTempTable("t_websocket")

      df = CluserProperties.STATISTICS_TYPE match {
        case StatisticsType.STB => onlinTimeByDevice("t_websocket", hiveContext)
        case StatisticsType.MOBILE => onlinTimeByMobile("t_websocket", hiveContext)
        case StatisticsType.ALL =>
          val df2 =  onlinTimeByMobile("t_websocket", hiveContext)
          if(df2 != null && df2.count()>0){
            onlinTimeByDevice("t_websocket", hiveContext).unionAll(df2).registerTempTable("t_union2")
          }else{
            onlinTimeByDevice("t_websocket", hiveContext).registerTempTable("t_union2")
          }
          hiveContext.sql(
            """
              |select sum(f_play_time) f_play_time ,f_terminal,f_region_id
              |from t_union2
              |group by f_terminal,f_region_id
            """.stripMargin)
      }
      webSocketDF.unpersist()
    }

    //用户上报和websocket两种数据源都有
    if ( resources.contains(SourceType.USER_REPORT)  && resources.contains(SourceType.WEBSOCKET) ) {
      println("------用户上报和websocket两种数据源都有---")
      val webSocketDF = getWebsocketData(day,homedUserDF,hiveContext)
      val reportDF = getReportLog(hiveContext,day)
      webSocketDF.persist()
      reportDF.persist()

      val allUsersDF = webSocketDF.selectExpr("user_id", "device_id")
        .unionAll(reportDF.selectExpr("user_id", "device_id"))
        .distinct()
        .join(homedUserDF,Seq("user_id"))

      webSocketDF.unionAll(reportDF).registerTempTable("t_all_users")
      hiveContext.sql(
        """
          |select max(play_time) play_time,user_id,device_id,device_type,region_id
          |from t_all_users
          |group by  user_id,device_id,device_type,region_id
        """.stripMargin)
        .join(allUsersDF, Seq("user_id", "device_id"))
        .registerTempTable("t_all")

      df = CluserProperties.STATISTICS_TYPE match {
        case StatisticsType.STB => onlinTimeByDevice("t_all", hiveContext)
        case StatisticsType.MOBILE => onlinTimeByMobile("t_all", hiveContext)
        case StatisticsType.ALL =>
          val df2 = onlinTimeByMobile("t_all", hiveContext)
          if ( df2 != null && df2.count() > 0) {
            onlinTimeByDevice("t_all", hiveContext).unionAll(df2).registerTempTable("t_union3")

            hiveContext.sql(
              """
                |select sum(f_play_time) f_play_time ,f_terminal,f_region_id
                |from t_union3
                |group by f_terminal,f_region_id
              """.stripMargin)
          } else {
            onlinTimeByDevice("t_all", hiveContext)
          }
      }

    }

    //只有run 日志
    if (resources.contains(SourceType.RUN_LOG) &&
      ! resources.contains(SourceType.WEBSOCKET) &&  !resources.contains(SourceType.USER_REPORT)) {

      val runLogDF = getRunLogDF(day,hiveContext)
      runLogDF.registerTempTable("t_runlog")

      println("=====runLogDF========" + runLogDF.count())
      println("------只有run日志------------")
      df = CluserProperties.STATISTICS_TYPE match {
        case StatisticsType.STB => onlinTimeByDevice("t_runlog", hiveContext)
        case StatisticsType.MOBILE => onlinTimeByMobile("t_runlog", hiveContext)
        case StatisticsType.ALL =>
          val df2 =  onlinTimeByMobile("t_runlog", hiveContext)
          if(df2 != null && df2.count()>0){
            onlinTimeByDevice("t_runlog", hiveContext).unionAll(df2).registerTempTable("t_union2")
          }else{
            onlinTimeByDevice("t_runlog", hiveContext).registerTempTable("t_union2")
          }
          hiveContext.sql(
            """
              |select sum(f_play_time) f_play_time ,f_terminal,f_region_id
              |from t_union2
              |group by f_terminal,f_region_id
            """.stripMargin)
      }
    }

    df.selectExpr("f_play_time" ,"f_terminal","f_region_id")
  }


  /**
    *默认DataFrame
    */
  private def getDefaultDF(hiveContext: HiveContext): DataFrame = {
    var listBuffer = new ListBuffer[PlayTime]()
    listBuffer.append(PlayTime(0L,0,""))
    hiveContext.createDataFrame(listBuffer)
  }



  /**
    * 根据设备计算开机时长
    * @return f_play_time|f_type|f_terminal|f_region_id
    */
  def onlinTimeByDevice(table: String, hiveContext: HiveContext): DataFrame = {
    hiveContext.sql(
      s"""
         |select sum(play_time) f_play_time,device_type as f_terminal,region_id as f_region_id
         |from $table
         | where device_type in ('1','2')
         |group by device_id,device_type,region_id
      """.stripMargin)
  }

  /**
    * 根据账号计算开机时长
    * @return f_play_time|f_type|f_terminal|f_region_id
    */
  def onlinTimeByMobile(table: String, hiveContext: HiveContext): DataFrame = {
    hiveContext.sql(
      s"""
         |select sum(play_time) f_play_time,device_type as f_terminal,region_id as f_region_id
         |from $table
         | where device_type not in ('1','2')
         group by user_id,device_type,region_id
         |
      """.stripMargin)
  }

  /**
    * 获取run日志数据
    */
  def getRunLogDF(day: String, hiveContext: HiveContext):DataFrame = {
    hiveContext.sql(
      s"""
         |select userid user_id ,deviceid device_id,devicetype device_type,regionid region_id, playtime play_time
         |from bigdata.orc_video_play
         |where day='$day'
      """.stripMargin)
  }

  /**
    * 获取websokect日志开机时间
    * @return user_id|device_id|device_type|region_id|play_time
    */
  private def getWebsocketData(day: String, homedUserDF: DataFrame, hiveContext: HiveContext): DataFrame = {
    hiveContext.udf.register("getTime"
      ,(loginTime_time: String, logoutTime: String, day: String) => getTime(loginTime_time, logoutTime, day))

    val day2 = DateUtils.transformDateStr(day, format2 = DateUtils.YYYY_MM_DD_HHMMSS)
    val _websocketDF = hiveContext.sql(
      s"""
         |select f_user_id user_id ,f_device_id device_id,f_device_type device_type,
         |       substr(f_region_id,1,6) as region_id,
         |       getTime(f_login_time,f_logout_time,'$day2') as  play_time
         |from bigdata.t_user_online_history
         |where day = '$day'
      """.stripMargin)

    _websocketDF.alias("a").join(homedUserDF.alias("b"), Seq("user_id"))
  }

  /**
    * 获取上报日志开机时间
    * @return user_id|device_id|device_type|region_id|play_time
    */
  def getReportLog(hiveContext: HiveContext, day: String):DataFrame ={
    hiveContext.sql(
      s"""
         |select userId as user_id,deviceId device_id,deviceType device_type,regionId region_id,
         |count(1) * ${CluserProperties.HEART_BEAT} as play_time
         |from bigdata.orc_report_behavior
         |where day='$day' and reporttype='${GatherType.SYSTEM_HEARTBEAT}'
         |group by userId,deviceId,deviceType,regionId
       """.stripMargin)
  }

  private def getTime(loginTime_time: String, logoutTime: String, day: String): Long = {
    val f_login_time = if (loginTime_time < day) DateUtils.dateToUnixtime(day) else DateUtils.dateToUnixtime(loginTime_time)
    val f_logout_time = DateUtils.dateToUnixtime(logoutTime)
    val onLineTime = f_logout_time - f_login_time
    Math.ceil(onLineTime).toLong
  }

  //----------------------------历史使用人数(设备数)--------------------------
  private def getHistoryUser(day: String, hiveContext: HiveContext, homedUserDF: DataFrame,count_type:String,tokenUserDF:DataFrame): DataFrame = {
    val df = CluserProperties.STATISTICS_TYPE match {
      case StatisticsType.STB => historyUserByDevice(hiveContext, day,tokenUserDF)
      case StatisticsType.MOBILE => totalUserByMoblie(hiveContext, day,tokenUserDF).withColumnRenamed("f_total_users", "f_hist_user_count")
      case StatisticsType.ALL =>
        val df2 = totalUserByMoblie(hiveContext, day,tokenUserDF)
        if (df2!= null && df2.count() > 0) {
          totalUserByDevice(hiveContext, day)
            .unionAll(df2).withColumnRenamed("f_total_users", "f_hist_user_count")
        } else {
          totalUserByDevice(hiveContext, day)
        }

    }
    df.selectExpr("f_hist_user_count", s" '$count_type' as f_type", "f_terminal", "f_province_id", "f_province_name",
      "f_city_id", "f_city_name", "f_region_id", "f_region_name")
  }

  private def historyUserByDevice(hiveContext: HiveContext, day: String,tokenUserDF:DataFrame): DataFrame = {
    tokenUserDF.registerTempTable("t_token_user")
    hiveContext.sql(
      s"""
         |select count(distinct(device_id)) f_hist_user_count, '$DAY' as f_type,
         |device_type f_terminal,province_id f_province_id,first(province_name) f_province_name,
         |city_id f_city_id,first(city_name) f_city_name,region_id f_region_id, first(region_name)  f_region_name
         |from  t_token_user
         |group by province_id,city_id,region_id,device_type
        """.stripMargin)
  }


  //--------------------- 用户总量------------------------------------
  /**
    * 获取用户总量
    */
  private def getTotalUser(day: String, hiveContext: HiveContext, homedUserDF: DataFrame,tokenUserDF:DataFrame): DataFrame = {
    val df = CluserProperties.STATISTICS_TYPE match {
      case StatisticsType.STB => totalUserByDevice(hiveContext, day)
      case StatisticsType.MOBILE => totalUserByMoblie(hiveContext, day,tokenUserDF)
      case StatisticsType.ALL =>
        val df2 = totalUserByMoblie(hiveContext, day,tokenUserDF)
        df2.persist()
        if (df2!= null && df2.count() > 0) {
          totalUserByDevice(hiveContext, day).unionAll(df2)
        } else {
          totalUserByDevice(hiveContext, day)
        }
        df2.unpersist()

    }

    df.selectExpr("f_total_users", "f_type", "f_terminal", "f_province_id", "f_province_name"
      , "f_city_id", "f_city_name", "f_region_id", "f_region_name")
  }

  //按登陆过的手机账号统计总用户数
  private def totalUserByMoblie(hiveContext: HiveContext, day: String,tokenUserDF:DataFrame): DataFrame = {
    var table = "t_tmp_user"
    tokenUserDF.registerTempTable(table)
    if (Constant.PANYU_CODE.equals(RegionUtils.getRootRegion)) {
      val nextDay = DateUtils.getAfterDay(day, targetFormat = DateUtils.YYYY_MM_DD)
      //在番禺，没有绑定家庭的手机账号也是无效用户
      //需要关联到device_info 找到对应设备类型
      UserUtils.getMobileCaBinds(hiveContext, nextDay).registerTempTable("t_ca_home")

      hiveContext.sql(
        s"""
           |select t1.*
           |from $table t1
           |join t_homed_users t2 on t1.user_id = t2.user_id
           |join t_ca_home t3 on t2.home_id = t3.home_id
           |where t1.device_type not in (1,2)
        """.stripMargin).registerTempTable("t_panyu")
      table = "t_panyu"
    }

    hiveContext.sql(
      s"""
         |select count(distinct(user_id)) f_total_users, '$DAY' as f_type,
         |device_type f_terminal,province_id f_province_id,first(province_name) f_province_name,
         |city_id f_city_id,first(city_name) f_city_name,region_id f_region_id, first(region_name)  f_region_name
         |from  $table
         |where  device_type not in ('1','2')
         |group by province_id,city_id,region_id,device_type
        """.stripMargin)
  }

  //按机顶盒设备数量统计总用户数
  private def totalUserByDevice(hiveContext: HiveContext, day: String): DataFrame = {
   val nextDay = DateUtils.getAfterDay(day,targetFormat = DateUtils.YYYY_MM_DD)
    hiveContext.sql(
      s"""
         |
        |select count(distinct(device_id)) f_total_users, '$DAY' as f_type,
         |device_type f_terminal,province_id f_province_id,province_name f_province_name,
         |city_id f_city_id,city_name f_city_name,region_id f_region_id, region_name f_region_name
         |from t_user
         |where device_type in ('1','2')
         |group by province_id,city_id,region_id,device_type,province_name,city_name,region_name
      """.stripMargin)

    /*
     hiveContext.sql(
      s"""
         |select count(distinct(f_device_id)) as f_total_users, '$DAY' as f_type,
         |f_terminal,f_province_id,f_province_name,
         |f_city_id ,f_city_name,f_region_id ,  f_region_name
         |from t_user
         |where f_terminal in (1,2) and f_create_time < '$nextDay'
         |group by f_terminal,f_province_id,f_province_name,f_city_id ,f_city_name,f_region_id ,f_region_name
        """.stripMargin)
     */
  }



  //----------------------------开机人数/设备数---------------------------------------------

  /**
    * 根据统计类型获取在线(开机)人数
    */
  private def getOnlineUserByType(day: String, count_type: String, hiveContext: HiveContext): DataFrame = {
    val df = CluserProperties.STATISTICS_TYPE match {
      case StatisticsType.STB => onlineUserByDevice(hiveContext, count_type)
      case StatisticsType.MOBILE => onlineUserByMobile(day, hiveContext, count_type)
      case StatisticsType.ALL =>
        val df2 = onlineUserByMobile(day, hiveContext, count_type)
        df2.persist()
        if (df2 !=null &&  df2.count() > 0) {
          onlineUserByDevice(hiveContext, count_type).unionAll(df2)
        } else {
          onlineUserByDevice(hiveContext, count_type)
        }
    }
    df.selectExpr("f_user_count", s"'$count_type' as f_type", "f_terminal", "f_province_id", "f_province_name"
      , "f_city_id", "f_city_name", "f_region_id", "f_region_name")
  }

  //按账号统计移动端登录人数
  private def onlineUserByMobile(day: String, hiveContext: HiveContext, count_type: String): DataFrame = {
    var table = "t_valid_user"
    if (Constant.PANYU_CODE.equals(RegionUtils.getRootRegion)) {
      val nextDay = DateUtils.getAfterDay(day, targetFormat = DateUtils.YYYY_MM_DD)
      //在番禺，没有绑定家庭的手机账号也是无效用户
      //需要关联到device_info 找到对应设备类型
      UserUtils.getMobileCaBinds(hiveContext, nextDay).registerTempTable("t2")
      hiveContext.sql(
        """
          |select t1.*
          |from t_valid_user t1
          |join t2 on t1.home_id = t2.home_id and t1.device_type not in (1,2)
        """.stripMargin).registerTempTable("t_panyu")
      table = "t_panyu"
    }

    hiveContext.sql(
      s"""
         |select count(distinct(user_id)) f_user_count, '$count_type' as f_type,
         |device_type f_terminal,province_id f_province_id,first(province_name) f_province_name,
         |city_id f_city_id,first(city_name) f_city_name,region_id f_region_id, first(region_name)  f_region_name
         |from  $table
         |group by province_id,city_id,region_id,device_type
        """.stripMargin)
  }

  //按设备统计在线设备数
  private def onlineUserByDevice(hiveContext: HiveContext, count_type: String): DataFrame = {
    hiveContext.sql(
      s"""
         |select count(distinct(device_id)) f_user_count, '$count_type' as f_type,
         |device_type f_terminal,province_id f_province_id,first(province_name) f_province_name,
         |city_id f_city_id,first(city_name) f_city_name,region_id f_region_id, first(region_name)  f_region_name
         |from t_valid_user
         |where  device_type in("1","2")
         |group by province_id,city_id,region_id,device_type
        """.stripMargin)
  }

  //---------------------------------------------------------------------------------------------
  /**
    * 所有在线的用户
    */
  private def getAllOnlineUser(hiveContext: HiveContext, day: String, count_type: String) = {
    val startDay = getStartDay(day, count_type)
    println(s"getAllOnlineUser====$count_type==$startDay====$day")
    hiveContext.sql(
      s"""
         |select user_id,device_id,device_type,province_id,province_name,city_id,city_name,region_id,region_name
         |from  bigdata.orc_user
         |where day >= '$startDay' and day <= '$day'
      """.stripMargin)
  }

  private def getStartDay(day: String, count_type: String): String = {
    val dateTime = DateUtils.dateStrToDateTime(day, DateUtils.YYYYMMDD)
    val startDay = count_type match {
      case DAY => day
      case WEEK => dateTime.plusDays(-dateTime.getDayOfWeek+1).toString(DateUtils.YYYYMMDD)
      case SEVEN_DAYS => dateTime.plusDays(-6).toString(DateUtils.YYYYMMDD)
      case MONTH => dateTime.plusDays(-dateTime.getDayOfMonth+1).toString(DateUtils.YYYYMMDD)
      case YEAR => dateTime.plusDays(-dateTime.getDayOfYear +1).toString(DateUtils.YYYYMMDD)
    }
    startDay
  }

  private def delete(day: String): Unit = {
    val sql = s"delete from  t_online_user_time where f_date='$day'"
    println("delete sql=== " + sql)
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, sql)

  }

  private case class PlayTime(f_play_time:Long,f_terminal:Int,f_region_id:String)

}
