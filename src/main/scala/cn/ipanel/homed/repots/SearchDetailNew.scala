package cn.ipanel.homed.repots

import java.text.SimpleDateFormat

import cn.ipanel.common.{CluserProperties, DBProperties, SparkSession, Tables}
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable


/** *
  * 通过用户上报数据122统计搜索报表
  */

object SearchDetailNew {

  private lazy val LOGGER = LoggerFactory.getLogger(SearchDetailNew.getClass)

  def main(args: Array[String]): Unit = {
    var day = DateUtils.getYesterday()
    if (args.size == 1) {
      day = args(0)
    }

    val sparkSession = SparkSession("SearchDetailNew", CluserProperties.SPARK_MASTER_URL)
    val sparkContext = sparkSession.sparkContext
    val hiveContext = sparkSession.sqlContext
    val begintime = DateUtils.dateToUnixtime(DateUtils.getStartTimeOfDay(day)) * 1000
    val endtime = DateUtils.dateToUnixtime(DateUtils.getEndTimeOfDay(day)) * 1000
    //日志路径
    // val path = LogConstant.HDFS_ARATE_LOG + day + "/*/*"
    //val path = "file:///C:\\Users\\Administrator\\Desktop\\11111111111111111"
    //    val lines = sparkContext.textFile(path)
    //      .filter(x => x.startsWith("<?><[0122,"))
    //      .map(_.substring(3).replaceAll("(<|>|\\[|\\]|\\(|\\))", "").replaceAll("\\s*|\t|\r|\n", ""))
    //      .filter(x => {
    //        var b = false
    //        if (x.contains("|")) {
    //          val arr = x.split("\\|")
    //          if (arr.length == 2) {
    //            val userid = arr(0).split(",")(2)
    //            val expandDate = arr(1)
    //            val maps = str_to_map(expandDate)
    //            if (maps.contains("R") && maps("R") != "" && !userid.contains("guest")) b = true
    //          }
    //        }
    //        b
    //      }).map(x => {
    //      val arr = x.split("\\|")
    //      val user = parseUser(arr(0), begintime, endtime)
    //      val map = str_to_map(arr(1))
    //      val resultid = map("R")
    //      val key = user.f_date + "," + user.f_hour + "," + user.f_timerange + "," + user.f_userid + "," + user.f_terminal + "," + resultid
    //      (key, 1)
    //    }).reduceByKey(_ + _).map(x => {
    //      val key_arr = x._1.split(",")
    //      val count = x._2
    //      val tmp_re = key_arr(5).replaceAll("\\D", "").trim
    //      val result_id = if (tmp_re.length > 0) tmp_re.toLong else 0L
    //      (key_arr(0), key_arr(1).toInt, key_arr(2), key_arr(3), key_arr(4), result_id, count)
    //    }).toDF("f_date", "f_hour", "f_timerange", "f_userid", "f_terminal", "result_id", "f_count")
    //    lines.show()

    hiveContext.sql("use bigdata")
    val lines = hiveContext.sql(
      s"""
         |select
         |a.f_date,a.f_hour,a.f_timerange,a.f_userid,a.result_id,
         |a.f_terminal,
         |count(*) as f_count
         |from
         |(select '$day' as f_date,hour(reporttime)  as f_hour,devicetype as f_terminal,
         |(case when minute(reporttime)>30 then '60' else '30' end) as f_timerange,
         |userid as f_userid,exts['R'] as result_id
         |from
         |orc_report_behavior
         |where day = '$day' and reporttype = 0122
         |and exts['R'] != ''  and exts['R'] is not null
         |) a
         |group by
         |a.f_date,a.f_hour,a.f_timerange,a.f_userid,a.result_id,a.f_terminal
      """.stripMargin)
    //    +------+------+-----------+--------+----------+---------+-------+
    //    |f_date|f_hour|f_timerange|f_userid|f_terminal|result_id|f_count|
    //    +------+------+-----------+--------+----------+---------+-------+
    //    +------+------+-----------+--------+----------+---------+-------+
    // 拿到result id 集
    val resultList = new mutable.HashSet[Int]()
    lines.select("result_id").distinct().collect().map(x => {
      val result_id =
        try {
          x.getAs[String]("result_id").toLong
        } catch {
          case e: Exception => 0
        }
      val num = getNumByResultId(result_id)
      resultList.add(num)
    })

    val adressDF = getUserAdressAndDeviceID(hiveContext)
    //    adressDF.show(false)
    val eventDF = getDataFrameByNum(resultList, hiveContext)
    //    eventDF.show(false)
    val df = lines
      .join(eventDF, "result_id")
      .join(adressDF, "f_userid")
      .drop("result_id")
    // df.show(false)
    //    println(df.count())
    delMysql(day, Tables.mysql_user_search_keyword)
    DBUtils.saveToHomedData_2(df, Tables.mysql_user_search_keyword)
  }

  def getNumByResultId(e: Long): Int = {
    var num = 0
    if (e > 100000000L && e < 199999999L) {
      num = 1
    } else if (e > 300000000L && e < 399999999L) {
      num = 2
    } else if (e > 200000000L && e < 299999999L) {
      num = 3
    } else if (e > 400000000L && e < 499999999L) {
      num = 4
    } else if (e > 1000000000L && e < 1099999999L) {
      num = 5
    } else if (e > 500000000L && e < 549999999L) {
      num = 6
    } else if (e > 575000000L && e < 599999999L) {
      num = 7
    } else if (e > 550000000L && e < 574999999L) {
      num = 8
    } else if (e > 1400000000L && e < 1499999999L) {
      num = 9
    } else if (e > 1300000000L && e < 1399999999L) {
      num = 10
    } else if (e > 4210000000L && e < 4211999999L) {
      num = 11
    } else if (e > 4200000000L && e < 4201999999L) {
      num = 12
    } else if (e > 4202000000L && e < 4203999999L) {
      num = 13
    } else if (e > 600000000L && e < 699999999L) {
      num = 14
    } else if (e > 0L && e < 9999999L) {
      num = 15
    } else if (e > 10000000L && e < 19999999L) {
      num = 16
    } else if (e > 20000000L && e < 29999999L) {
      num = 17
    } else if (e > 30000000L && e < 39999999L) {
      num = 18
    } else if (e > 60000000L && e < 69999999L) {
      num = 19
    } else if (e > 700000000L && e < 799999999L) {
      num = 20
    } else if (e > 800000000L && e < 899999999L) {
      num = 21
    } else if (e > 1100000000L && e < 1199999999L) {
      num = 22
    } else if (e > 1200000000L && e < 1299999999L) {
      num = 23
    } else if (e > 4208000000L && e < 4209999999L) {
      num = 24
    } else if (e > 4212000000L && e < 4214999999L) {
      num = 25
    } else if (e > 4215000000L && e < 4217999999L) {
      num = 26
    }
    num
  }

  def parseUser(str: String, begintime: Long, endtime: Long): User = {
    val arr = str.split(",")
    if (arr.length >= 5) {
      var time = arr(1).replaceAll("\\D", "")
      if (time.toLong < begintime || time.toLong > endtime) {
        time = begintime.toString
      }
      time = DateUtils.timestampFormat(time.toLong, "yyyy-MM-dd HH:mm:ss")
      val date = DateUtils.dateStrToDateTime(time)
      val hour = date.getHourOfDay
      val minute = date.getMinuteOfHour
      val timerange = if (minute > 30) "60" else "30"
      val terminal = deviceIdMapToDeviceType(arr(4))
      User(time.split(" ")(0).replaceAll("-", ""), hour, timerange, arr(2), terminal)
    } else {
      User()
    }
  }

  def str_to_map(str: String, delimiter1: String = "&", delimiter2: String = ","): mutable.HashMap[String, String] = {
    val map = new mutable.HashMap[String, String]()
    if (StringUtils.isEmpty(str)) {
      map.put("", null)
    } else {
      str.split(delimiter1).foreach(word => {
        val splits = word.split(delimiter2)
        if (splits.length == 2) {
          map.put(splits(0), splits(1))
        }
      })
    }
    map

  }

  def getTimeStamp(time: String): Long = {
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val t = format.parse(time)
    t.getTime
  }



  def deviceIdMapToDeviceType(deviceId: String): String = {
    //    var deviceType = "unknown"
    //终端（0 其他, 1stb,2 CA卡,3mobile,4 pad, 5pc）
    var deviceType = "0"
    try {
      val device = deviceId.toLong
      if (device >= 1000000000l && device <= 1199999999l) {
        deviceType = "1"
      } else if (device >= 1400000000l && device <= 1599999999l) {
        deviceType = "2"
      } else if (device >= 1800000000l && device < 1899999999l) {
        deviceType = "4"
      } else if (device >= 2000000000l && device <= 2999999999l) {
        deviceType = "3"
      } else if (device >= 3000000000l && device < 3999999999l) {
        deviceType = "5"
      }
    } catch {
      case ex: Exception => LOGGER.error(ex.getMessage); ex.printStackTrace()
    }
    deviceType
  }

  //通过用户id 获取用户地址和机顶盒号
  def getUserAdressAndDeviceID(sqlContext: SQLContext): DataFrame = {
    //user_id-home_id
    val uh_sql =
      """(select DA as f_userid,home_id from account_info where status=1 or status=2) as a"""
    //home_id-region_id-city_id-province_id
    val ha_sql =
      """(select home_id,region_id as f_region_id,city_id as f_city_id,province_id as f_province_id from address_info where status=1) as b"""
    //省信息
    val provincesql =
      """(SELECT province_id as f_province_id,province_name as f_province_name from province) as c""".stripMargin
    //城市信息
    val citysql =
      """(SELECT city_id as f_city_id,city_name as f_city_name from city) as d """.stripMargin
    //区域信息
    val regionsql =
      """(SELECT area_id as f_region_id,area_name as f_region_name from area) as e """.stripMargin
    val homed_df = DBUtils.loadMysql(sqlContext, uh_sql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()
    val adress_df = DBUtils.loadMysql(sqlContext, ha_sql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()
    val provinceDF = DBUtils.loadMysql(sqlContext, provincesql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()
    val cityDF = DBUtils.loadMysql(sqlContext, citysql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()
    val regionDF = DBUtils.loadMysql(sqlContext, regionsql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()

    val finalDF = homed_df.join(adress_df, "home_id")
      .join(provinceDF, "f_province_id")
      .join(cityDF, "f_city_id")
      .join(regionDF, "f_region_id")
      .drop("home_id").distinct()

    finalDF
  }

  def getDataFrameByNum(list: mutable.HashSet[Int], sqlContext: SQLContext): DataFrame = {
    var df = getVideoSeries(sqlContext)
    for (e <- list) {
      var df1 = sqlContext.emptyDataFrame
      e match {
        case 1 => df1 = getDemand(sqlContext)
        case 2 => df1 = getDemandSeries(sqlContext)
        case 3 => df1 = getLookBaack(sqlContext)
        case 4 => df1 = getLookBaackSeries(sqlContext)
        case 5 => df1 = getHomedApp(sqlContext)
        case 6 => df1 = getMusic(sqlContext)
        case 7 => df1 = getMusiAlbum(sqlContext)
        case 8 => df1 = getSinger(sqlContext)
        case 9 => df1 = getPlayList(sqlContext)
        case 10 => df1 = getSubject(sqlContext)
        case 11 => df1 = getStar(sqlContext)
        case 12 => df1 = getChannel(sqlContext)
        case 13 => df1 = getMonitor(sqlContext)
        case 14 => df1 = getNew(sqlContext)
        case 15 => df1 = getColumn(sqlContext)
        case 16 => df1 = getShop(sqlContext)
        case 17 => df1 = getPromo(sqlContext)
        case 18 => df1 = getShoppingCategory(sqlContext)
        case 19 => df1 = getMosaicStore(sqlContext)
        case 20 => df1 = getShoppingMainProduct(sqlContext)
        case 21 => df1 = getLiveProgram(sqlContext)
        case 22 => df1 = getTourismRouteInfo(sqlContext)
        case 23 => df1 = getTourismTicketInfo(sqlContext)
        case 24 => df1 = getLiveRoom(sqlContext)
        case 25 => df1 = getProgramPackage(sqlContext)
        case 26 => df1 = getProgramGroup(sqlContext)
        case _ =>
      }
      if (df1.count() > 0) { // 只有返回的数据不为空才合并
        df = df.unionAll(df1)
      }
    }
    df.distinct()
  }

  // 1
  //点播单集 100000000L-199999999L
  def getDemand(sqlContext: SQLContext): DataFrame = {
    val demandSql = "(select video_id as result_id,video_name as f_keyword from video_info) as a"
    val demandDF = DBUtils.loadMysql(sqlContext, demandSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    demandDF
  }

  // 2
  //点播剧集 300000000L-399999999L
  def getDemandSeries(sqlContext: SQLContext): DataFrame = {
    val demandSeriesSql = "(select series_id as result_id,series_name as f_keyword from video_series) as a"
    val demandSeriesDF = DBUtils.loadMysql(sqlContext, demandSeriesSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    demandSeriesDF
  }

  // 3
  //回看剧集 200000000L-299999999L
  def getLookBaack(sqlContext: SQLContext): DataFrame = {
    val lookBackSql = "(select event_id as result_id,event_name as f_keyword from homed_eit_schedule_history) as a"
    val lookBackDF = DBUtils.loadMysql(sqlContext, lookBackSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    lookBackDF
  }

  // 4
  //回看剧集 400000000L-499999999L
  def getLookBaackSeries(sqlContext: SQLContext): DataFrame = {
    val lookBackSeriesSql = "(select series_id as result_id,series_name as f_keyword from event_series) as a"
    val lookBackSeriesDF = DBUtils.loadMysql(sqlContext, lookBackSeriesSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    lookBackSeriesDF
  }

  // 5
  //homed应用 1000000000L-1099999999L
  def getHomedApp(sqlContext: SQLContext): DataFrame = {
    val homedAppSql = "(select id as result_id,chinese_name as f_keyword from app_store) as a"
    val homedAppDF = DBUtils.loadMysql(sqlContext, homedAppSql, DBProperties.JDBC_URL_ICORE, DBProperties.USER_ICORE, DBProperties.PASSWORD_ICORE).distinct()
    homedAppDF
  }

  // 6
  //音乐 500000000L-549999999L
  def getMusic(sqlContext: SQLContext): DataFrame = {
    val musicSql = "(select music_id as result_id ,music_name as f_keyword from music_info) as a"
    val musicDF = DBUtils.loadMysql(sqlContext, musicSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    musicDF
  }

  // 7
  //音乐专辑 575000000L-599999999L
  def getMusiAlbum(sqlContext: SQLContext): DataFrame = {
    val musicAlbumSql = "(select album_id as result_id ,album_name as f_keyword from music_album_info) as a"
    val musicAlbumDF = DBUtils.loadMysql(sqlContext, musicAlbumSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    musicAlbumDF
  }

  // 8
  //歌手 550000000L-574999999L
  def getSinger(sqlContext: SQLContext): DataFrame = {
    val singerSql = "(select singer_id as result_id,singer_name as f_keyword from music_singer_info) as a"
    val singerDF = DBUtils.loadMysql(sqlContext, singerSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    singerDF
  }

  // 9
  //公共播单 1400000000L-1499999999L
  def getPlayList(sqlContext: SQLContext): DataFrame = {
    val playSql = "(select f_playlist_id as result_id,f_playlist_name as f_keyword from t_playlist_info) as a"
    val playDF = DBUtils.loadMysql(sqlContext, playSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    playDF
  }

  // 10
  //专题 1300000000L-1399999999L
  def getSubject(sqlContext: SQLContext): DataFrame = {
    val subjectSql = "(select f_subject_id as result_id,f_subject_name as f_keyword from t_subject_info) as a"
    val subjectDF = DBUtils.loadMysql(sqlContext, subjectSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    subjectDF
  }

  // 11
  //明星百科 4210000000L-4211999999L
  def getStar(sqlContext: SQLContext): DataFrame = {
    val starSql = "(select f_star_id as result_id, f_star_name as f_keyword from t_star_info) as a  "
    val starDF = DBUtils.loadMysql(sqlContext, starSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    starDF
  }

  // 12
  //频道相关 4200000000L-4201999999L
  def getChannel(sqlContext: SQLContext): DataFrame = {
    val channelSql = "(select channel_id as result_id ,chinese_name as f_keyword from channel_store) as a"
    val channelDF = DBUtils.loadMysql(sqlContext, channelSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    channelDF
  }

  //13
  // 监控 4202000000L-4203999999L dtvs.t_monitor_store
  def getMonitor(sqlContext: SQLContext): DataFrame = {
    val monitorSql =
      """
        |(select distinct(f_monitor_id) as result_id,f_monitor_name as f_keyword from t_monitor_store ) t   """.stripMargin
    val monitorDF = DBUtils.loadMysql(sqlContext, monitorSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    monitorDF
  }

  //14
  //新闻 600000000L-699999999L dtvs.t_news_info
  def getNew(sqlContext: SQLContext): DataFrame = {
    val newsSql =
      """
        (select distinct(f_news_id) as result_id, f_title as f_keyword  from t_news_info ) t
      """.stripMargin
    val newsDF = DBUtils.loadMysql(sqlContext, newsSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    newsDF
  }

  // 聚合节目 4100000000L-4199999999L t_duplicate_series
  def getVideoSeries(sqlContext: SQLContext): DataFrame = {
    //节目Id关联 剧集
    val videoSeriesSql = s" (select f_duplicate_id result_id,f_program_name as f_keyword from t_duplicate_series) as t_video_series"
    val videoSeriesDF = DBUtils.loadMysql(sqlContext, videoSeriesSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    videoSeriesDF
  }

  //15
  //! homed栏目id 1L-9999999L dtvs.t_column_info
  def getColumn(sqlContext: SQLContext): DataFrame = {
    val columnSql = s" (select f_column_id result_id,f_column_name as f_keyword from t_column_info) as t"
    val columnDF = DBUtils.loadMysql(sqlContext, columnSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    columnDF
  }

  //16
  //! homed商铺id 10000000L-19999999L dtvs.t_shopping_shop
  def getShop(sqlContext: SQLContext): DataFrame = {
    val shopSql = s" (select f_shop_id result_id,f_shopname as f_keyword from t_shopping_shop) as t"
    val shopDF = DBUtils.loadMysql(sqlContext, shopSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    shopDF
  }

  //17
  //! homed活动id 20000000L-29999999L t_shopping_promo
  def getPromo(sqlContext: SQLContext): DataFrame = {
    val promoSql = s" (select f_promo_id result_id,f_promoname as f_keyword from t_shopping_promo) as t"
    val promoDF = DBUtils.loadMysql(sqlContext, promoSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    promoDF
  }

  //18
  //! homed电商分类id 30000000L-39999999L t_shopping_category
  def getShoppingCategory(sqlContext: SQLContext): DataFrame = {
    val shoppingCategorySql = s" (select f_category_id result_id,f_category_name as f_keyword from t_shopping_category) as t"
    val shoppingCategoryDF = DBUtils.loadMysql(sqlContext, shoppingCategorySql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    shoppingCategoryDF
  }

  //19
  //! homed马赛克集合id 60000000L-69999999L t_mosaic_store
  def getMosaicStore(sqlContext: SQLContext): DataFrame = {
    val mosaicStoreSql = s" (select f_mosaicId result_id,f_mosaicName as f_keyword from t_mosaic_store) as t"
    val mosaicStoreDF = DBUtils.loadMysql(sqlContext, mosaicStoreSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    mosaicStoreDF
  }

  //20
  //! homed电商主商品id 700000000L-799999999L t_shopping_main_product
  def getShoppingMainProduct(sqlContext: SQLContext): DataFrame = {
    val shoppingMainProductSql = s" (select f_main_product_id result_id,f_product_name as f_keyword from t_shopping_main_product) as t"
    val shoppingMainProductDF = DBUtils.loadMysql(sqlContext, shoppingMainProductSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    shoppingMainProductDF
  }

  //21
  //! homed直播间回看id 800000000L-899999999L t_live_program
  def getLiveProgram(sqlContext: SQLContext): DataFrame = {
    val liveProgramSql = s" (select f_program_id result_id,f_live_title as f_keyword from t_live_program) as t"
    val liveProgramDF = DBUtils.loadMysql(sqlContext, liveProgramSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    liveProgramDF
  }

  //22
  //! homed旅游（线）id 1100000000L-1199999999L t_tourism_route_info
  def getTourismRouteInfo(sqlContext: SQLContext): DataFrame = {
    val tourismRouteInfoSql = s" (select f_route_id result_id,f_route_name as f_keyword from t_tourism_route_info) as t"
    val tourismRouteInfoDF = DBUtils.loadMysql(sqlContext, tourismRouteInfoSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    tourismRouteInfoDF
  }

  //23
  //! homed旅游（票）id 1200000000L-1299999999L t_tourism_ticket_info
  def getTourismTicketInfo(sqlContext: SQLContext): DataFrame = {
    val tourismTicketInfoSql = s" (select f_ticket_id result_id,f_ticket_name as f_keyword from t_tourism_ticket_info) as t"
    val tourismTicketInfoDF = DBUtils.loadMysql(sqlContext, tourismTicketInfoSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    tourismTicketInfoDF
  }

  //24
  //! homed直播间id 4208000000L-4209999999L t_live_room
  def getLiveRoom(sqlContext: SQLContext): DataFrame = {
    val liveRoomSql = s" (select f_room_id result_id,f_room_name as f_keyword from t_live_room) as t"
    val liveRoomDF = DBUtils.loadMysql(sqlContext, liveRoomSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    liveRoomDF
  }

  //25
  //! homed套餐id 4212000000L-4214999999L program_package
  def getProgramPackage(sqlContext: SQLContext): DataFrame = {
    val programPackageSql = s" (select package_id result_id,package_name as f_keyword from program_package) as t"
    val programPackageDF = DBUtils.loadMysql(sqlContext, programPackageSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    programPackageDF
  }

  //26
  //! homed产品包id 4215000000L-4217999999L program_group
  def getProgramGroup(sqlContext: SQLContext): DataFrame = {
    val programGroupSql = s" (select group_id result_id,group_name as f_keyword from program_group) as t"
    val programGroupDF = DBUtils.loadMysql(sqlContext, programGroupSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    programGroupDF
  }

  case class User(f_date: String = "", f_hour: Int = 0, f_timerange: String = "", f_userid: String = "", f_terminal: String = "")

  /**
    * 防止重跑导致重复数据
    **/
  def delMysql(day: String, table: String): Unit = {
    val del_sql = s"delete from $table where f_date='$day'"
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, del_sql)
  }

  /*def getDataFrameByNum(list: mutable.HashSet[Int], sqlContext: SQLContext): DataFrame = {
    var df = getData("t_duplicate_series", "f_duplicate_id", "f_program_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext)
    for (e <- list) {
      var df1 = sqlContext.emptyDataFrame
      e match {
        case 1 => df1 = getData("video_info", "video_id", "video_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //点播单集 100000000L-199999999L
        case 2 => df1 = getData("video_series", "series_id", "series_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //点播剧集 300000000L-399999999L
        case 3 => df1 = getData("homed_eit_schedule_history", "event_id", "event_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //回看剧集 200000000L-299999999L
        case 4 => df1 = getData("event_series", "series_id", "series_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //回看剧集 400000000L-499999999L
        case 5 => df1 = getData("app_store", "id", "chinese_name", DBProperties.JDBC_URL_ICORE, DBProperties.USER_ICORE, DBProperties.PASSWORD_ICORE, sqlContext) //homed应用 1000000000L-1099999999L
        case 6 => df1 = getData("music_info", "music_id", "music_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //音乐 500000000L-549999999L
        case 7 => df1 = getData("music_album_info", "album_id", "album_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //音乐专辑 575000000L-599999999L
        case 8 => df1 = getData("music_singer_info", "singer_id", "singer_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //歌手 550000000L-574999999L
        case 9 => df1 = getData("t_playlist_info", "f_playlist_id", "f_playlist_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //公共播单 1400000000L-1499999999L
        case 10 => df1 = getData("t_subject_info", "f_subject_id", "f_subject_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //专题 1300000000L-1399999999L
        case 11 => df1 = getData("t_star_info", "f_star_id", "f_star_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //明星百科 4210000000L-4211999999L
        case 12 => df1 = getData("channel_store", "channel_id", "chinese_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //频道相关 4200000000L-4201999999L
        case 13 => df1 = getData("t_monitor_store", "f_monitor_id", "f_monitor_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) // 监控 4202000000L-4203999999L dtvs.t_monitor_store
        case 14 => df1 = getData("t_news_info", "f_news_id", "f_title", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //新闻 600000000L-699999999L dtvs.t_news_info
        case 15 => df1 = getData("t_column_info", "f_column_id", "f_column_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //! homed栏目id 1L-9999999L dtvs.t_column_info
        case 16 => df1 = getData("t_shopping_shop", "f_shop_id", "f_shopname", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //! homed商铺id 10000000L-19999999L dtvs.t_shopping_shop
        case 17 => df1 = getData("t_shopping_promo", "f_promo_id", "f_promoname", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //! homed活动id 20000000L-29999999L t_shopping_promo
        case 18 => df1 = getData("t_shopping_category", "f_category_id", "f_category_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //! homed电商分类id 30000000L-39999999L t_shopping_category
        case 19 => df1 = getData("t_mosaic_store", "f_mosaicId", "f_mosaicName", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //! homed马赛克集合id 60000000L-69999999L t_mosaic_store
        case 20 => df1 = getData("t_shopping_main_product", "f_main_product_id", "f_product_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //! homed电商主商品id 700000000L-799999999L t_shopping_main_product
        case 21 => df1 = getData("t_live_program", "f_program_id", "f_live_title", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //! homed直播间回看id 800000000L-899999999L t_live_program
        case 22 => df1 = getData("t_tourism_route_info", "f_route_id", "f_route_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //! homed旅游（线）id 1100000000L-1199999999L t_tourism_route_info
        case 23 => df1 = getData("t_tourism_ticket_info", "f_ticket_id", "f_ticket_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //! homed旅游（票）id 1200000000L-1299999999L t_tourism_ticket_info
        case 24 => df1 = getData("t_live_room", "f_room_id", "f_room_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //! homed直播间id 4208000000L-4209999999L t_live_room
        case 25 => df1 = getData("program_package", "package_id", "package_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //! homed套餐id 4212000000L-4214999999L program_package
        case 26 => df1 = getData("program_group", "group_id", "group_name", DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS, sqlContext) //! homed产品包id 4215000000L-4217999999L program_group
        case _ =>
      }
      df = df.unionAll(df1)
    }
    df.distinct()
  }

  def getData(table: String, column1: String, column2: String, jdbc_url: String, user: String, password: String, sqlContext: SQLContext): DataFrame = {
    val sql = s"(select $column1 as result_id,$column2 as f_keyword from $table) as a"
    val df = DBUtils.loadMysql(sqlContext, sql, jdbc_url, user, password).distinct()
    df
  }*/
}
