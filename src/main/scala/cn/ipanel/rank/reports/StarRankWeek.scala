package cn.ipanel.rank.reports

import cn.ipanel.common.{DBProperties, SparkSession}
import cn.ipanel.rank.common.Constant
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.DataFrame
import org.joda.time.Days

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Sorting

/**
  * 明星周热度
  *
  * @author ZouBo
  * @date 2018/6/25 0025 
  */
/**
  *
  * @param mediaId        媒资id
  * @param actorName      演员名称
  * @param actorMediaHeat 明星相关媒资热度
  */
case class ActorHeat(mediaId: String, actorName: String, actorMediaHeat: Double, date: String)

/**
  *
  * @param f_star_id             明星id
  * @param f_star_name           明星名称
  * @param f_date                日期，对应统计周期的最后一天
  * @param f_click_count         点击量
  * @param f_search_count        搜索量
  * @param f_attention_count     关注量
  * @param f_net_attention_count 网络关注量
  * @param f_star_heat           明星热度
  * @param f_period_type         统计周期 1-日榜 2-周榜 3-月榜
  */
case class ActorHeat2(f_star_id: String, f_star_name: String, f_date: String, f_click_count: Long, f_search_count: Long, f_attention_count: Long, f_net_attention_count: Long, f_star_heat: Double, f_period_type: Int)

object StarRankWeek {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("请输入正确参数[yyyyMMdd]")
      System.exit(1)
    }
    val sunday = args(0)
    val monday = DateUtils.getNDaysAfter(-7, sunday)

    val session = SparkSession("StarRankWeek", "")
    session.sparkContext.getConf.registerKryoClasses(Array(classOf[ActorHeat2], classOf[ActorHeat]))
    val result = getActorRank(monday, sunday, session)
    DBUtils.saveToMysql(result, "t_start_heat_rank", DBProperties.JDBC_URL_RANK, DBProperties.USER_RANK, DBProperties.PASSWORD_RANK)
  }

  /**
    *
    * @param sac          关注量，点击量，搜索量
    * @param netAttention 网络关注量
    * @param actorHeat    明星热度
    * @param sparkSession
    * @return
    */
  def processDF(sac: DataFrame, netAttention: DataFrame, actorHeat: DataFrame, sparkSession: SparkSession, periodType: Int) = {
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    sac.unionAll(netAttention).unionAll(actorHeat).registerTempTable("sna")
    //    sac.registerTempTable("sac")
    //    netAttention.registerTempTable("netAttention")
    //    actorHeat.registerTempTable("actorHeat")
    //union all
    //    val sql =
    //      """
    //        |select f_star_id,f_star_name,f_click_count,f_search_count,f_attention_count,f_net_attention_count,f_actor_heat,f_total_heat,f_date
    //        |from(
    //        | select f_star_id,f_star_name,f_click_count,f_search_count,f_attention_count,f_net_attention_count,f_actor_heat,f_total_heat,f_date from sac
    //        | union all
    //        | select f_star_id,f_star_name,f_click_count,f_search_count,f_attention_count,f_net_attention_count,f_actor_heat,f_total_heat,f_date from netAttention
    //        | union all
    //        | select f_star_id,f_star_name,f_click_count,f_search_count,f_attention_count,f_net_attention_count,f_actor_heat,f_total_heat,f_date from actorHeat
    //        | )
    //      """.stripMargin
    //    sqlContext.sql(sql).registerTempTable("sna")
    val sql2 =
    """
      | select f_star_id,f_star_name,f_date,sum(f_click_count) as f_click_count,sum(f_search_count) as f_search_count,
      | sum(f_attention_count) as f_attention_count,sum(f_net_attention_count) as f_net_attention_count,
      | sum(f_actor_heat) as f_actor_heat,sum(f_total_heat) as f_total_heat
      | from sna
      | group by f_star_id,f_star_name,f_date
    """.stripMargin
    val sql3 =
      """
        |select f_date,sum(f_click_count) as f_total_click_count,sum(f_search_count) as f_total_search_count,
        |sum(f_attention_count) as f_total_attention_count,sum(f_net_attention_count) as f_total_net_attention_count
        |from sna
        |group by f_date
      """.stripMargin

    sqlContext.sql(sql2).registerTempTable("t_actor_heat")
    sqlContext.sql(sql3).registerTempTable("t_total_heat")
    val sql4 =
      """
        |select t1.f_star_id,t1.f_star_name,t1.f_date,t1.f_click_count,t2.f_total_click_count,
        |t1.f_search_count,t2.f_total_search_count,t1.f_attention_count,t2.f_total_attention_count,
        |t1.f_net_attention_count,t2.f_total_net_attention_count,t1.f_actor_heat,t1.f_total_heat
        |from t_actor_heat t1 left join t_total_heat t2
        |on t1.f_date=t2.f_date
      """.stripMargin

    val actorHeat2 = sqlContext.sql(sql4)
    val result = actorHeat2.mapPartitions(it => {
      val list = new ListBuffer[ActorHeat2]()
      it.foreach(row => {
        val starId = row.getAs[Long]("f_star_id").toString
        val starName = row.getAs[String]("f_star_name")
        val date = row.getAs[String]("f_date")
        val clickCount = row.getAs[Long]("f_click_count")
        val totalClickCount = row.getAs[Long]("f_total_click_count")
        val searchCount = row.getAs[Long]("f_search_count")
        val totalSearchCount = row.getAs[Long]("f_total_search_count")
        val attentionCount = row.getAs[Long]("f_attention_count")
        val totalAttentionCount = row.getAs[Long]("f_total_attention_count")
        val netAttentionCount = row.getAs[Long]("f_net_attention_count")
        val totalNetAttentionCount = row.getAs[Long]("f_total_net_attention_count")
        val actorHeat = row.getAs[Double]("f_actor_heat")
        val totalHeat = row.getAs[Double]("f_total_heat")
        val heat = computeActorHeat(clickCount, totalClickCount, searchCount, totalSearchCount, attentionCount, totalAttentionCount, netAttentionCount
          , totalNetAttentionCount, actorHeat, totalHeat)
        list += (ActorHeat2(starId, starName, date, clickCount, searchCount, attentionCount, netAttentionCount, heat, periodType))
      })
      list.iterator
    }).toDF
    result
  }

  /**
    * 获取周数据
    */
  def getActorRank(monday: String, sunday: String, sparkSession: SparkSession) = {
    //1.搜索量，关注量，点击量
    val sac = loadSearchAttentionClick(monday, sunday, sparkSession)
    //2.网络关注量
    val netAttention = loadNetAttention(monday, sunday, sparkSession)
    //3.相关媒资热度
    val actorHeat = computeMediaHeat(sparkSession, loadMediaHeat(monday, sunday, sparkSession, 2), sunday)

    processDF(sac, netAttention, actorHeat, sparkSession, Constant.ACTOR_RANK_PERIOD_TYPE_WEEK)

  }

  /**
    * 计算明星热度,保留4位小数
    *
    * @param clickCount             点击量
    * @param totalClickCount        总点击量
    * @param searchCount            搜索量
    * @param totalSearchCount       总搜索量
    * @param attentionCount         关注量
    * @param totalAttentionCount    总关注量
    * @param netAttentionCount      网络关注量
    * @param totalNetAttentionCount 总网络关注量
    * @param actorHeat              明星相关媒资热度
    * @param totalHeat              总媒资热度
    */
  def computeActorHeat(clickCount: Long, totalClickCount: Long, searchCount: Long, totalSearchCount: Long, attentionCount: Long,
                       totalAttentionCount: Long, netAttentionCount: Long, totalNetAttentionCount: Long, actorHeat: Double, totalHeat: Double) = {
    val attention = divide2element(attentionCount, totalAttentionCount) * Constant.ACTOR_RANK_ATTENTION_PROPORTION
    val mediaHeat = divide2element(actorHeat, totalHeat) * Constant.ACTOR_RANK_MEDIA_HEAT_PROPORTION
    val search = divide2element(searchCount, totalSearchCount) * Constant.ACTOR_RANK_SEARCH_PROPORTITION
    val click = divide2element(clickCount, totalClickCount) * Constant.ACTOR_RANK_CLICK_PROPORTION
    val netSearch = divide2element(netAttentionCount, totalNetAttentionCount) * Constant.ACTOR_RANK_NET_SEARCH_PROPORTION
    val heat = attention + mediaHeat + search + click + netSearch
    heat.formatted("%.4f").toDouble
  }

  val divide2element = (ele1: Double, ele2: Double) => {
    val result = if (ele2 == 0) {
      0.0
    } else {
      ele1 / ele2
    }
    result
  }

  /**
    * 计算明星相关媒资热度
    *
    * @param sparkSession
    * @param dataFrame
    */
  def computeMediaHeat(sparkSession: SparkSession, dataFrame: DataFrame, lastDay: String) = {
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    val startHeatDF = dataFrame.mapPartitions(it => {
      val list = new ListBuffer[ActorHeat]()
      it.foreach(row => {
        val mediaId = row.getAs[String]("f_media_id")
        val netPlay = scala.math.BigDecimal(row.getAs[java.math.BigDecimal]("f_net_play_count")).longValue()
        val netSearch = scala.math.BigDecimal(row.getAs[java.math.BigDecimal]("f_net_search_count")).longValue()
        val localSearch = row.getAs[Long]("f_local_search_count")
        val localPlay = row.getAs[Long]("f_local_play_count")
        val enshrine = row.getAs[Long]("f_enshrine")
        val favorite = row.getAs[Long]("f_favorite")
        val screenTime = row.getAs[String]("F_SCREEN_TIME")
        val seriesName = row.getAs[String]("series_name")
        val director = row.getAs[String]("director")
        val firstActor = row.getAs[String]("first_actor")
        val secondActor = row.getAs[String]("second_actor")
        val otherActors = row.getAs[String]("other_actors")
        //1.计算出剧集总统计量
        val totalHeat = totalHeatForSeries(netPlay, netSearch, localSearch, localPlay, enshrine, favorite, screenTime, lastDay)
        //主要演员集合
        val actors = allActorForSeries(director, firstActor, secondActor, otherActors)
        //2.计算明星对剧集的贡献量，根据剧集id+演员名字获取占比值
        val map = actorRuleAndProportion(director, firstActor, secondActor, otherActors)
        for (actor <- actors) {
          val actorHeat = map.getOrElse(actor, 0.0)
          //明星相关媒资热度
          val actorMediaHeat = multiply2element(totalHeat, actorHeat)
          list += (ActorHeat(mediaId, actor, actorMediaHeat, lastDay))
        }


      })
      list.iterator
    }).toDF
    //单个明星相关媒资热度，全部明星相关媒资热度
    startHeatDF.registerTempTable("actor")
    loadActorInfo(sparkSession).registerTempTable("actorInfo")
    val sql =
      """
        | select mediaId,actorName,actorMediaHeat,f_star_id,date
        | from actor,actorInfo
        | where mediaId=f_series_id and actorName=f_star_name
      """.stripMargin
    sqlContext.sql(sql).registerTempTable("actorheat")
    val sql2 =
      """
        |select f_star_id,actorName,date,sum(actorMediaHeat) as f_actor_heat
        |from actorheat
        |group by f_star_id,actorName,date
      """.stripMargin
    val sql3 =
      """
        |select date as f_date,sum(actorMediaHeat) as f_total_heat
        |from actorheat
        |group by date
      """.stripMargin
    sqlContext.sql(sql2).registerTempTable("actor2")
    sqlContext.sql(sql3).registerTempTable("actor3")
    val sql4 =
      """
        |select a2.f_star_id,a2.actorName as f_star_name,a2.f_actor_heat,a3.f_total_heat
        |from actor2 a2 left join actor3 a3
        |on a2.date=a3.f_date
      """.stripMargin
    sqlContext.sql(sql4).registerTempTable("heat")
    val sql5 =
      s"""
         |select f_star_id,f_star_name,0 as f_click_count,0 as f_search_count,
         |0 as f_attention_count,0 as f_net_attention_count,
         |f_actor_heat,f_total_heat,'$lastDay' as f_date
         |from heat
      """.stripMargin
    sqlContext.sql(sql5)

  }

  /**
    * 媒资热度总量统计
    *
    * @param netPlay     网络播放量
    * @param netSearch   网络搜索量
    * @param localSearch 本地搜索量
    * @param localPlay   本地播放量
    * @param enshrine    喜爱收藏
    * @param favorite    追剧
    * @param screenTime  上映时间
    */
  def totalHeatForSeries(netPlay: Long, netSearch: Long, localSearch: Long, localPlay: Long, enshrine: Long, favorite: Long, screenTime: String, lastDay: String) = {
    val totalPlay = multiply2element(netPlay, Constant.ACTOR_RANK_NET_PLAY_PROPORTION) + multiply2element(localPlay, Constant.ACTOR_RANK_LOCAL_PLAY_PROPORTION)
    val totalSearch = multiply2element(netSearch, Constant.ACTOR_RANK_NET_SEARCH_PROPORTION) + multiply2element(localSearch, Constant.ACTOR_RANK_LOCAL_SEARCH_PROPORTION)
    //避免上映时间为空抛出异常
    val screenDate = if (screenTime != null && screenTime.trim.length == 8) {
      DateUtils.dateStrToDateTime(screenTime, DateUtils.YYYYMMDD)
    } else {
      DateUtils.dateStrToDateTime("20180101", DateUtils.YYYYMMDD)
    }
    //    val screenDate = DateUtils.dateStrToDateTime(screenTime, DateUtils.YYYYMMDD)
    val statisticsDay = DateUtils.dateStrToDateTime(lastDay, DateUtils.YYYYMMDD)
    //剧集新度
    val days = Days.daysBetween(screenDate, statisticsDay).getDays
    val newHeat: Double = days match {
      case _ if (days <= 7) => Constant.ACTOR_SERIES_NEW_HEAT_7
      case _ if (days > 7 && days <= 30) => Constant.ACTOR_SERIES_NEW_HEAT_30
      case _ => Constant.ACTOR_SERIES_NEW_HEAT_BEYOND_30
    }
    val mediaHeat = multiply2element(totalPlay, Constant.ACTOR_RANK_PLAY_PROPORTION) + multiply2element(totalSearch, Constant.ACTOR_RANK_SEARCH_PROPORTITION)
    +multiply2element(enshrine, Constant.ACTOR_RANK_LIKE_ENSHRINE_PROPORTITION) + multiply2element(favorite, Constant.ACTOR_RANK_FAVORITE_PROPORTITION) +
      multiply2element(newHeat, Constant.ACTOR_RANK_SERIES_NEW_HEAT)
    mediaHeat.formatted("%.0f").toLong
  }

  val multiply2element = (ele1: Double, ele2: Double) => {
    ele1 * ele2
  }


  /**
    * 所有演员
    *
    * @param director
    * @param firstActor
    * @param secondActor
    * @param otherActors
    */
  def allActorForSeries(director: String, firstActor: String, secondActor: String, otherActors: String): List[String] = {
    director.split("\\|").toList ::: firstActor.split("\\|").toList ::: secondActor.split("\\|").toList ::: otherActors.split("\\|").toList
  }

  /**
    * 演员角色及总量占比
    * 考虑到既是导演又是演员情况:多个map合并，key相同情况下，占比高者覆盖占比低者
    *
    * @param director
    * @param firstActor
    * @param secondActor
    * @param otherActors
    */
  def actorRuleAndProportion(director: String, firstActor: String, secondActor: String, otherActors: String): mutable.Map[String, Double] = {
    val otherMap = computeActorProportion(otherActors, Constant.ACTOR_RULE_OTHER)
    val secondMap = computeActorProportion(secondActor, Constant.ACTOR_RULE_SECOND)
    val firstMap = computeActorProportion(firstActor, Constant.ACTOR_RULE_FIRST)
    val directorMap = computeActorProportion(director, Constant.ACTOR_RULE_DIRECTOR)

    val actorMap = Array(firstMap, secondMap, directorMap, otherMap)
    //对占比情况排序，map合并操作占比低在前，占比高在后
    /**
      * ACTOR_RULE_DIRECTOR 更改为明星占比？
      */
    val proD = Constant.ACTOR_RULE_DIRECTOR + "_2"
    val proF = Constant.ACTOR_RULE_FIRST + "_0"
    val proS = Constant.ACTOR_RULE_SECOND + "_1"
    val proO = Constant.ACTOR_RULE_OTHER + "_3"
    val proArr = Array(proF, proS, proD, proO)
    Sorting.quickSort(proArr)

    if (actorMap.length == proArr.length && proArr.length == 4) {
      return actorMap(getIndex(0, proArr)) ++ actorMap(getIndex(1, proArr)) ++ actorMap(getIndex(2, proArr)) ++ actorMap(getIndex(3, proArr))
    } else {
      return firstMap ++ secondMap ++ directorMap ++ otherMap
    }
  }

  /**
    * 获取下标
    */
  val getIndex = (index: Int, proArr: Array[String]) => {
    proArr(index).split("_")(1).toInt
  }
  /**
    * map(actor->proportion)
    */
  val computeActorProportion = (str: String, flag: String) => {
    val map = new mutable.HashMap[String, Double]()
    val arr = str.split("\\|")
    for (a <- arr if a.trim.length > 0) {
      flag match {
        case Constant.ACTOR_RULE_DIRECTOR => map += (a -> Constant.ACTOR_PROPORTION_DIRECTOR)
        case Constant.ACTOR_RULE_FIRST => map += (a -> Constant.ACTOR_PROPORTION_FIRST)
        case Constant.ACTOR_RULE_SECOND => map += (a -> Constant.ACTOR_PROPORTION_SECOND)
        case Constant.ACTOR_RULE_OTHER => map += (a -> Constant.ACTOR_PROPORTION_OTHER)
      }
    }
    map
  }

  /**
    * 加载演员信息，剧集+名字确定唯一演员id
    */
  def loadActorInfo(sparkSession: SparkSession) = {
    val sqlContext = sparkSession.sqlContext
    val sql =
      s"""
         |(select cast(tp.f_series_id as char) as f_series_id,tp.f_star_id,ti.f_star_name
         |from t_star_series_mapping tp join t_star_info ti
         |on(tp.f_star_id=ti.f_star_id)
         |where tp.f_series_weight>=0 and tp.f_series_status=1
         |) as actorInfo
      """.stripMargin
    DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
  }

  /**
    * 加载homed系统中明星点击量、搜索量、关注量
    * 其余维度数据补0
    *
    * @param firstDay 统计周日开始日期
    * @param lastDay  统计周期最后日期
    * @param sparkSession
    * @return
    */
  def loadSearchAttentionClick(firstDay: String, lastDay: String, sparkSession: SparkSession) = {
    val sqlContext = sparkSession.sqlContext
    val table =
      s"""
         |(select f_star_id,f_star_name,f_total_count,f_from_search_count,f_homed_attention_count
         |from  t_star_rank
         |where f_date='$lastDay' and f_before_date='$firstDay'
         |) as starrank
      """.stripMargin
    DBUtils.loadDataFromPhoenix2(sqlContext, table).registerTempTable("csa")
    val sql2 =
      s"""
         | select F_STAR_ID as f_star_id,F_STAR_NAME as f_star_name,
         | F_TOTAL_COUNT as f_click_count,F_FROM_SEARCH_COUNT as f_search_count,F_HOMED_ATTENTION_COUNT as f_attention_count,
         | 0 as f_net_attention_count,0 as f_actor_heat,0 as f_total_heat,'$lastDay' as f_date
         | from csa
      """.stripMargin
    sqlContext.sql(sql2)
  }

  /**
    * 从hbase中加载明星的网络关注量
    * 其余维度数据补0
    *
    * @param firstDay 统计周期开始日期
    * @param lastDay  统计周期最后日期
    * @param sparkSession
    * @return
    */
  def loadNetAttention(firstDay: String, lastDay: String, sparkSession: SparkSession) = {
    val sqlContext = sparkSession.sqlContext
    sqlContext.sql("use bigdata")

    val sql =
      s"""
         |(select f_name,f_media_id,f_accord_value
         |from star_net
         |where f_date>='$firstDay' and f_date<='$lastDay'
         |) as net_attention
      """.stripMargin
    DBUtils.loadDataFromPhoenix2(sqlContext, sql).registerTempTable("net")

    /**
      * phoenix方式读取数据，spark无法识别sum(f_accord_value) as f_net_attention_count这种格式给字段起别名
      * 故先将数据加载进来，再group by
      */
    val sql2 =
      """
        |select F_NAME as f_star_name,F_MEDIA_ID as f_star_id,
        |sum(F_ACCORD_VALUE) as f_net_attention_count
        |from net
        |group by F_NAME,F_MEDIA_ID
      """.stripMargin
    sqlContext.sql(sql2).registerTempTable("net2")
    val sql3 =
      s"""
         |select f_star_id, f_star_name,0 as f_click_count,0 as f_search_count,0 as f_attention_count,
         |f_net_attention_count,0 as f_actor_heat,0 as f_total_heat,'$lastDay' as f_date
         |from net2
      """.stripMargin
    sqlContext.sql(sql3)
  }

  /**
    * 加载明星相关媒资热度
    * 其余维度数据补0
    *
    * @param firstDay 开始日期
    * @param lastDay  结束日期
    * @param sparkSession
    * @return
    */
  def loadMediaHeat(firstDay: String, lastDay: String, sparkSession: SparkSession, periodType: Int) = {
    val sqlContext = sparkSession.sqlContext
    sqlContext.sql("use bigdata")
    //从数据库中加载网络搜索量和播放量
    val netSql =
      s"""
         |(select t1.f_media_id ,sum(f_net_play_count) as f_net_play_count ,
         |sum(f_net_search_count) as f_net_search_count from (select cast(f_media_id as char) as f_media_id,
         |(case when f_accord_type=1 then f_accord_value else 0 end) as f_net_play_count,
         |(case when f_accord_type=2 then f_accord_value else 0 end) as f_net_search_count
         |from t_rank_i1
         |where f_date='$lastDay' and f_period=$periodType and f_net_id=1871
         |) t1
         |group by t1.f_media_id) as t_net_play_search
      """.stripMargin
    val netPlaySearchDF = DBUtils.loadMysql(sqlContext, netSql, DBProperties.SPIDER2_JDBC_URL, DBProperties.SPIDER2_USER, DBProperties.SPIDER2_PASSWORD)
    netPlaySearchDF.registerTempTable("net")
    val netSql2 =
      """
        |select f_media_id,f_net_play_count,f_net_search_count,
        |0 as f_local_search_count,0 as f_local_play_count,0 as f_enshrine,0 as f_favorite
        |from net
      """.stripMargin
    val netSearchPlay = sqlContext.sql(netSql2)

    //本地搜索量 hbase
    val localSearchSql =
      s"""
         |(select f_series_id,f_search_count
         |from  t_local_search
         |where f_date='$lastDay' and f_before_date='$firstDay'
         |) as t_localsearch
      """.stripMargin
    val localSerchDF = DBUtils.loadDataFromPhoenix2(sqlContext, localSearchSql)
    localSerchDF.registerTempTable("localsearch")
    val localSearchSql2 =
      """
        |select F_SERIES_ID as f_media_id,0 as f_net_play_count,0 as f_net_search_count,
        |F_SEARCH_COUNT as f_local_search_count,0 as f_local_play_count,0 as f_enshrine,0 as f_favorite
        |from localsearch
      """.stripMargin
    val localSerch = sqlContext.sql(localSearchSql2)
    localSerch.show()
    //本地播放量
    val localPlaySql =
      s"""
         |( select f_series_id,f_screen_time,f_count
         |from t_local_watch
         |where f_date='$lastDay' and f_before_date='$firstDay'
         |) as t_local_play
      """.stripMargin
    val localPlayDF = DBUtils.loadDataFromPhoenix2(sqlContext, localPlaySql)
    localPlayDF.registerTempTable("localplay")
    val localPlaySql2 =
      """
        | select F_SERIES_ID as f_media_id,0 as f_net_play_count,0 as f_net_search_count,
        | 0 as f_local_search_count,F_COUNT as f_local_play_count,0 as f_enshrine,0 as f_favorite
        | from localplay
      """.stripMargin
    val localPlay = sqlContext.sql(localPlaySql2)
    //加载喜爱收藏、追剧
    //此phoenix sql spark无法识别，需拆分
    //    val favoriteSql =
    //      s"""
    //         |( select t1.favorite_id,sum(f_enshrine) as f_enshrine,sum(f_favorite) as f_favorite
    //         |from (select favorite_id,
    //         |(case when f_function=0 then f_homed_count else 0 end) as f_enshrine,
    //         |(case when f_function=2 then f_homed_count else 0 end) as f_favorite
    //         |from user_favorite
    //         |where f_date='$lastDay' and f_before_date='$firstDay'
    //         |) t1
    //         |group by favorite_id) as t_favorite
    //       """.stripMargin
    val favoriteSql =
    """
      | (select favorite_id,f_homed_count,f_function
      | from user_favorite
      | where f_date='$lastDay' and f_before_date='$firstDay'
      | ) as t_favorite
      |
      """.stripMargin
    val localFavoriteDF = DBUtils.loadDataFromPhoenix2(sqlContext, favoriteSql)
    localFavoriteDF.registerTempTable("favorite")

    val favoriteSql2 =
      """
        | select t1.favorite_id,sum(t1.f_enshrine) as f_enshrine,sum(t1.f_favorite) as f_favorite
        | from (select FAVORITE_ID as favorite_id,
        |  (case when F_FUNCTION=0 then F_HOMED_COUNT else 0 end) as f_enshrine,
        |  (case when F_FUNCTION=2 then F_HOMED_COUNT else 0 end) as f_favorite
        | from favorite) t1
        | group by t1.favorite_id
      """.stripMargin
    sqlContext.sql(favoriteSql2).registerTempTable("favorite2")
    val favoriteSql3 =
      """
        |select cast(favorite_id as string) as f_media_id,0 as f_net_play_count,0 as f_net_search_count,
        |0 as f_local_search_count,0 as f_local_play_count,f_enshrine,f_favorite
        |from favorite2
      """.stripMargin
    val favorite = sqlContext.sql(favoriteSql3)
    netSearchPlay.unionAll(localSerch).unionAll(localPlay).unionAll(favorite).registerTempTable("union")
    //union all
    //    val unionSql =
    //      """
    //        | select f_media_id,f_net_play_count,f_net_search_count,f_local_search_count,f_local_play_count,f_enshrine,f_favorite
    //        | from (
    //        | select f_media_id,f_net_play_count,f_net_search_count,f_local_search_count,f_local_play_count,f_enshrine,f_favorite from net2
    //        | union all
    //        | select f_media_id,f_net_play_count,f_net_search_count,f_local_search_count,f_local_play_count,f_enshrine,f_favorite from localsearch2
    //        | union all
    //        | select f_media_id,f_net_play_count,f_net_search_count,f_local_search_count,f_local_play_count,f_enshrine,f_favorite from localplay2
    //        | union all
    //        | select f_media_id,f_net_play_count,f_net_search_count,f_local_search_count,f_local_play_count,f_enshrine,f_favorite from favorite3
    //        | )
    //      """.stripMargin
    //    val u=sqlContext.sql(unionSql)
    //    u.show()
    //      u.registerTempTable("union")
    val groupSql =
    """
      | select f_media_id,sum(f_net_play_count) as f_net_play_count,sum(f_net_search_count) as f_net_search_count,
      | sum(f_local_search_count) as f_local_search_count,sum(f_local_play_count) as f_local_play_count,
      | sum(f_enshrine) as f_enshrine,sum(f_favorite) as f_favorite
      | from union
      | group by f_media_id
    """.stripMargin
    val groupDF = sqlContext.sql(groupSql)
    val result = groupDF.join(localPlayDF, groupDF("f_media_id") === localPlayDF("f_series_id"), "left_outer").drop("F_SERIES_ID").drop("F_COUNT")
    val seriesActorDF = loadSeriesActorMapping(sparkSession)
    result.join(seriesActorDF, result("f_media_id") === seriesActorDF("series_id"), "left_outer").drop("series_id")
  }

  /**
    * 加载媒资参演人员信息
    *
    * @return
    */
  def loadSeriesActorMapping(sparkSession: SparkSession) = {

    val sqlContext = sparkSession.sqlContext
    sqlContext.sql("use bigdata")
    val sql =
      """
        |select cast(series_id as string) as series_id, series_name,director,first_actor,second_actor,other_actors
        |from orc_series_mapping
      """.stripMargin
    sqlContext.sql(sql)
  }

}
