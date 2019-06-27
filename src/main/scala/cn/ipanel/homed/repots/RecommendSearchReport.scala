package cn.ipanel.homed.repots

import java.text.SimpleDateFormat

import cn.ipanel.common.{CluserProperties, DBProperties, SparkSession, Tables}
import cn.ipanel.etl.LogConstant
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/** *
  * 通过用户上报数据122统计推荐搜索报表
  */

case class User_re(f_date: String = "", f_userid: String = "", f_terminal: String = "")

object RecommendSearchReport {
  private lazy val LOGGER = LoggerFactory.getLogger(RecommendSearchReport.getClass)
  def main(args: Array[String]): Unit = {
    var day = DateUtils.getYesterday()
    if (args.size == 1) {
      day = args(0)
    }

    val sparkSession = SparkSession("RecommendSearchReport", CluserProperties.SPARK_MASTER_URL)
    val sparkContext = sparkSession.sparkContext
    val hiveContext = sparkSession.sqlContext
    import hiveContext.implicits._
    val begintime = DateUtils.dateToUnixtime(DateUtils.getStartTimeOfDay(day)) * 1000
    val endtime = DateUtils.dateToUnixtime(DateUtils.getEndTimeOfDay(day)) * 1000
    //日志路径
    val path = LogConstant.HDFS_ARATE_LOG + day + "/*/*"
    val lines = sparkContext.textFile(path)
      .filter(x => x.startsWith("<?><[0122,"))
      .map(_.substring(3).replaceAll("(<|>|\\[|\\]|\\(|\\))", "").replaceAll("\\s*|\t|\r|\n", ""))
      .filter(x => {
        var b = false
        if (x.contains("|")) {
          val arr = x.split("\\|")
          if (arr.length == 2) {
            val userid = arr(0).split(",")(2)
            val expandDate = arr(1)
            val maps = str_to_map(expandDate)
            if (maps.contains("R")) b = true
          }
        }
        b
      }).map(x => {
      val arr = x.split("\\|")
      val user = parseUser(arr(0), begintime, endtime)
      val map = str_to_map(arr(1))
      val resultid = map("R").replaceAll("\\D", "").toLong
      (user.f_date, user.f_userid, user.f_terminal, resultid, 2)
    }).filter(x => x._2 != "").toDF("time", "user_id", "terminal", "result_id", "action_type")
    //    lines.show()

    // 拿到result id 集
    val resultList = new mutable.HashSet[Int]()
    lines.select("result_id").distinct().collect().map(x => {
      val result_id = x.getAs[Long]("result_id")
      val num = getNumByResultId(result_id)
      resultList.add(num)
    })

    //    val adressDF = getUserAdressAndDeviceID(hiveContext)
    //        adressDF.show(false)
    val eventDF = getDataFrameByNum(resultList, hiveContext)
    //    eventDF.show(false)
    val df = lines
      .join(eventDF, "result_id")
      .drop("result_id").registerTempTable("recommendSearch_table")
    //    df.printSchema()
    //    df.show(false)

    val partition = DateUtils.transformDateStr(day)
    hiveContext.sql(
      s"""
         |insert into table recommend_db.t_user_rank_info partition(day='$partition')
         |select user_id,time,terminal,program_id,program_name,series_id,series_name,contenttype,subtype,action_type
         |from recommendSearch_table
      """.stripMargin)
    getLiveChannelInfoHive(hiveContext, day,partition)
  }

  def getLiveChannelInfoHive(sqlContext: HiveContext, date: String,partition:String) = {
    sqlContext.sql("use userprofile")
    sqlContext.sql(
      s"""
         |select f_user_id,f_channel_id,f_channel_name,
         |f_terminal,f_start_time
         |from
         |t_user_basic where day ='$date'
         |and f_play_type ='live'
      """.stripMargin).registerTempTable("live")
    sqlContext.sql("use recommend_db")
    sqlContext.sql(
      s"""
         |INSERT INTO TABLE
         |t_user_rank_info partition(day='$date')
         |select f_user_id as user_id,f_start_time as time, f_terminal as terminal,
         | f_channel_id as program_id, f_channel_name as program_name,
         |'' as series_id,'' as series_name,
         |'' as contenttype,'' as subtype,1  as action_type
         |from live
      """.stripMargin)
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
    }
    num
  }

  def parseUser(str: String, begintime: Long, endtime: Long): User_re = {
    val arr = str.split(",")
    if (arr.length >= 5) {
      var time = arr(1).replaceAll("\\D", "")
      if (time.toLong < begintime || time.toLong > endtime) {
        //        time = begintime.toString
        User_re()
      } else {
        time = DateUtils.timestampFormat(time.toLong, "yyyy-MM-dd HH:mm:ss")
        val terminal = deviceIdMapToDeviceType(arr(4))
        User_re(time, arr(2), terminal)
      }

    } else {
      User_re()
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
      }
      df = df.unionAll(df1)
    }
    df
  }

  // 1
  //点播单集 100000000L-199999999L
  def getDemand(sqlContext: SQLContext): DataFrame = {
    val demandSql = "(select video_id as result_id,video_id as program_id,video_name as program_name,f_series_id as series_id,f_content_type as contenttype,f_sub_type as subtype from video_info) as a"
    val demandSeriesSql = "(select series_id ,series_name from video_series) as a"
    val demandDF = DBUtils.loadMysql(sqlContext, demandSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    val demandSeriesDF = DBUtils.loadMysql(sqlContext, demandSeriesSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    demandDF.join(demandSeriesDF, "series_id").select("result_id", "program_id", "program_name", "series_id", "series_name", "contenttype", "subtype")
  }

  // 2
  //点播剧集 300000000L-399999999L
  def getDemandSeries(sqlContext: SQLContext): DataFrame = {
    val demandSeriesSql = "(select series_id as result_id,'' as program_id ,'' as program_name ,series_id,series_name ,f_content_type as contenttype,f_sub_type as subtype from video_series) as a"
    val demandSeriesDF = DBUtils.loadMysql(sqlContext, demandSeriesSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    demandSeriesDF
  }

  // 3
  //回看单集 200000000L-299999999L
  def getLookBaack(sqlContext: SQLContext): DataFrame = {
    val lookBackSql = "(select event_id as result_id，event_id as program_id,event_name as program_name, f_series_id as series_id,f_content_type as contenttype,f_sub_type as subtype from homed_eit_schedule_history) as a"
    val lookBackDF = DBUtils.loadMysql(sqlContext, lookBackSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    val lookBackSeriesSql = "(select series_id ,series_name from event_series) as a"
    val lookBackSeriesDF = DBUtils.loadMysql(sqlContext, lookBackSeriesSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    lookBackDF.join(lookBackSeriesDF, "series_id").select("result_id", "program_id", "program_name", "series_id", "series_name", "contenttype", "subtype")
  }

  // 4
  //回看剧集 400000000L-499999999L
  def getLookBaackSeries(sqlContext: SQLContext): DataFrame = {
    val lookBackSeriesSql = "(select series_id as result_id,'' as program_id ,'' as program_name,series_id ,series_name ,f_content_type as contenttype,f_sub_type as subtype from event_series) as a"
    val lookBackSeriesDF = DBUtils.loadMysql(sqlContext, lookBackSeriesSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    lookBackSeriesDF
  }

  // 5
  //homed应用 1000000000L-1099999999L
  def getHomedApp(sqlContext: SQLContext): DataFrame = {
    val homedAppSql = "(select id as result_id,id as program_id,chinese_name as program_name,''as series_id,''as series_name,f_content_type as contenttype,f_sub_type as subtype from app_store) as a"
    val homedAppDF = DBUtils.loadMysql(sqlContext, homedAppSql, DBProperties.JDBC_URL_ICORE, DBProperties.USER_ICORE, DBProperties.PASSWORD_ICORE).distinct()
    homedAppDF
  }

  // 6
  //音乐 500000000L-549999999L
  def getMusic(sqlContext: SQLContext): DataFrame = {
    val musicSql = "(select music_id as result_id,music_id as program_id ,music_name as program_name,album_id as series_id,f_content_type as contenttype,f_sub_type as subtype from music_info) as a"
    val musicDF = DBUtils.loadMysql(sqlContext, musicSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    val musicAlbumSql = "(select album_id as series_id ,album_name as series_name from music_album_info) as a"
    val musicAlbumDF = DBUtils.loadMysql(sqlContext, musicAlbumSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    musicDF.join(musicAlbumDF, "series_id").select("result_id", "program_id", "program_name", "series_id", "series_name", "contenttype", "subtype")
  }

  // 7
  //音乐专辑 575000000L-599999999L
  def getMusiAlbum(sqlContext: SQLContext): DataFrame = {
    val musicAlbumSql = "(select album_id as result_id ,'' as program_id ,'' as program_name,album_id as series_id,album_name as series_name ,f_content_type as contenttype,f_sub_type as subtype from music_album_info) as a"
    val musicAlbumDF = DBUtils.loadMysql(sqlContext, musicAlbumSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    musicAlbumDF
  }

  // 8
  //歌手 550000000L-574999999L
  def getSinger(sqlContext: SQLContext): DataFrame = {
    val singerSql = "(select singer_id as result_id,singer_id as program_id,singer_name as program_name,'' as series_id,'' as series_name, f_content_type as contenttype,f_sub_type as subtype from music_singer_info) as a"
    val singerDF = DBUtils.loadMysql(sqlContext, singerSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    singerDF
  }

  // 9
  //公共播单 1400000000L-1499999999L
  def getPlayList(sqlContext: SQLContext): DataFrame = {
    val playSql = "(select f_playlist_id as result_id,f_playlist_id as program_id,f_playlist_name as program_name,''as series_id,''as series_name,f_content_type as contenttype,f_subtype as subtype from t_playlist_info) as a"
    val playDF = DBUtils.loadMysql(sqlContext, playSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    playDF
  }

  // 10
  //专题 1300000000L-1399999999L
  def getSubject(sqlContext: SQLContext): DataFrame = {
    val subjectSql = "(select f_subject_id as result_id,f_subject_id as program_id,f_subject_name as program_name,'' as series_id,'' as series_name,f_content_type as contenttype,f_subtype as subtype from t_subject_info) as a"
    val subjectDF = DBUtils.loadMysql(sqlContext, subjectSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    subjectDF
  }

  // 11
  //明星百科 4210000000L-4211999999L
  def getStar(sqlContext: SQLContext): DataFrame = {
    val starSql = "(select f_star_id as result_id ,f_star_id as program_id ,f_star_name as program_name,'' as series_id,'' as series_name,f_content_type as contenttype,f_sub_type as subtype from t_star_info) as a  "
    val starDF = DBUtils.loadMysql(sqlContext, starSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    starDF
  }

  // 12
  //频道相关 4200000000L-4201999999L
  def getChannel(sqlContext: SQLContext): DataFrame = {
    val channelSql = "(select channel_id as result_id,channel_id as program_id ,chinese_name as program_name,'' as series_id,'' as series_name,f_contenttype as contenttype,f_subtype as subtype from channel_store) as a"
    val channelDF = DBUtils.loadMysql(sqlContext, channelSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    channelDF
  }

  // 聚合节目
  def getVideoSeries(sqlContext: SQLContext): DataFrame = {
    //节目Id关联 剧集
    val videoSeriesSql = s" (select f_duplicate_id result_id,'' as program_id,'' as program_name,f_duplicate_id as series_id,f_program_name as series_name,'' as contenttype,'' as subtype from t_duplicate_series) as t_video_series"
    val videoSeriesDF = DBUtils.loadMysql(sqlContext, videoSeriesSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    videoSeriesDF
  }
}
