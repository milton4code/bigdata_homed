package cn.ipanel.rank.reports

import cn.ipanel.common.SparkSession
import cn.ipanel.rank.common.{RankType, Constant => RankConstant}
import cn.ipanel.utils._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import redis.clients.jedis.Jedis

import scala.collection.mutable.{ArrayBuffer, HashMap => scalaHasMap}



/**
  * SetFavoriteRank<br>
  * 收藏榜/追剧/关注榜
  * Function  0 表示收藏, 1 表示关注, 2 表示追剧
  * author liujjy
  * create 2018/7/3
  * since 1.0.0
  */
object SetFavoriteRank {

  private val pro = PropertiUtils.init("redis.properties")

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println(
        """
          |请输入 结束日期,统计间隔，是否debug,初始化日期：例如【20180808】 【7】  [true] [20180711]
          |   --结束日期 :必选,表示统计截止日期
          |   --统计间隔 : 必选,0表示日榜,7表示周榜,30表示月榜, 365表示年榜,-1表示总榜
          |   --是否debug :必选,true 表示debug ,打印所有榜单数据,false 不打印榜单数据
          |   --初始化日期 : 初始化日期主要为了和历史数据隔离
        """.stripMargin)
      System.exit(-1)
    }

    val day = args(0)
    val interval = args(1).toInt
    val period = getPeriod(interval)
    var startDay = DateUtils.dateStrToDateTime(args(0), DateUtils.YYYYMMDD).plusDays(-interval).toString(DateUtils.YYYYMMDD)
    val debug = args(2).toBoolean
    val initDay = args(3)

    if (period == RankType.TOTAL_RANK) {
      startDay = args(3)
    }

    println("=======period========="+ period)
    val session = new SparkSession("SetFavoriteRank")
    val sc = session.sparkContext
    val hiveContext = session.sqlContext
    import hiveContext.implicits._
    import hiveContext.sql


    val favoriteDF = getData(hiveContext, day, startDay,initDay)
    favoriteDF.registerTempTable("t_favorite")


    val rankDF = sql(
      """
        |select count(favorite_id) as counts,source_type as type,favorite_id,f_parent_id,function_id
        |from t_favorite
        |group by source_type,favorite_id,f_parent_id,function_id
        |union all
        |select count(favorite_id) as counts,f_contenttype as type ,favorite_id,f_parent_id,function_id
        |from t_favorite
        |group by f_contenttype,favorite_id,f_parent_id,function_id
      """.stripMargin)
      .sort($"counts".desc)
      .collect()

    val redisMap = rddToMap(rankDF, period)

    println("====================print log====================")
    printLog(debug, redisMap,hiveContext)
    saveToRedis(redisMap)
    postToRecommend()

    session.stop()
  }



  /**
    * 获取收藏数据
    *
    * @return user_id|favorite_id|function_id|f_contenttype|source_type
    */
  private def getData(hiveContext: HiveContext, day: String, startDay: String,initDay:String): DataFrame = {
    hiveContext.sql("use bigdata")
    hiveContext.udf.register("sourceType", IDRangeUtils.getTheTypeOfProgram _)
    hiveContext.udf.register("rankType", rankTypeMapping _)
        hiveContext.sql(
          s"""
             |select userid, cast(t1.exts['FavoriteId'] as bigint) favorite_id ,rankType(t1.exts['Function']) as function_id,
             |cast(t1.exts['Contenttype'] as bigint) f_contenttype,
             |sourceType( cast(t1.exts['FavoriteId'] as bigint)) source_type,
             |t1.exts['Topcolumnid'] as f_parent_id
             |from orc_user_behavior t1
             |where reporttype='SetFavoriteSuccess'
             |and day>=$startDay and day<=$day
             |and day >=$initDay
           """.stripMargin)
  }

  def rankTypeMapping(funtoinId: String): String = {
    funtoinId match {
      case "0" => RankType.COLLECT_BANK
      case "1" => RankType.FOCUS_BANK
      case "2" => RankType.FOLLOW_BANK
    }

  }

  private def postToRecommend(): Unit = {
    val data = "{ \"type\":10, \"value\":10 }"
    val dtvsUrl = getUrl()
    PushTools.sendPostRequest(dtvsUrl, data)
  }

  private def getUrl(): String = {
    val dtvsServer = {
      import scala.language.postfixOps
      import sys.process._
      ("""sed -n s/export.dtvs_ips=\"\([0-9.]\+\)\"/\1/p /homed/allips.sh """ !!).trim() + ":12890"
    }
    s"http://$dtvsServer/recommend/schedule"
  }

  private def saveToRedis(redisMap: scalaHasMap[String, ArrayBuffer[(Long, Long)]]):Unit = {
    //    redis.select(15)
    val redis = new Jedis(pro.getProperty("redis_host"), pro.getProperty("redis_port").toInt)
    redisMap.foreach(r => {
      redis.hset("t_rank_list".getBytes, r._1.getBytes(), encodeToRedisDD(r._2))
//      redis.expire("t_rank_list", 60 * 60 * 24)
    })
    redis.close()
  }

  private def encodeToRedisDD(data: ArrayBuffer[(Long, Long)]) = {
    val arrayByte = new scala.collection.mutable.ArrayBuffer[Byte]()

    def writeToByte(n: Long): Unit = {
      var m = n
      while ((m & ~0x7F) != 0) {
        arrayByte += ((m & 0x7F) | 0x80).toByte
        m = m >>> 7
      }
      arrayByte += m.toByte
    }

    writeToByte(data.length * 2)
    //first write reason and last write pid
    data.foreach(v => {
      writeToByte(v._1)
      writeToByte(v._2)
    })

    arrayByte.toArray
  }

  def printLog(debug: Boolean, redisMap: scalaHasMap[String, ArrayBuffer[(Long, Long)]],hiveContext: HiveContext): Unit = {

    hiveContext.sql("select * from t_favorite").show(200,false)

    if(debug){
      redisMap.foreach(x => {
        println(x._1 + "===>" + x._2)
      })
    }

  }

  /**
    * 将rdd封装成Map结构
    *  key
    *    column_id：数值型，根栏目id
    *   contenttype:数值型，节目类型。0为混合类型，其它值按homed系统定义
    *   accordtype:数值型，排行统计依据。取值0，代表按热度排行；取值1，代表按点击量排行；取值2，代表按搜索量排行
    *   period:数值型，排行榜统计时段。取值1，代表日榜；取值2，代表周榜；取值3，代表月榜；取值4，代表年度榜；取值5，代表总榜
    * value
    *    ArrayList 结构, 元素为剧集id 以及收藏(追剧后或者关注)数量
    */
  private def rddToMap(rankRDD: Array[Row], period: Int): scalaHasMap[String, ArrayBuffer[(Long, Long)]] = {
    val redisMap = new scalaHasMap[String, ArrayBuffer[(Long, Long)]]()
    rankRDD.foreach(r => {
      val sb = new StringBuffer()
      val key = sb.append(r.
        getAs[String]("f_parent_id")).append(RankConstant.DECOLLATOR)
        .append(r.getAs[Long]("type")).append(RankConstant.DECOLLATOR)
        .append(r.getAs[String]("function_id")).append(RankConstant.DECOLLATOR)
        .append(period).toString

      if (redisMap.contains(key)) {
        redisMap(key).append((r.getAs[Long]("favorite_id"), r.getAs[Long]("counts")))
      } else {
        var list = new ArrayBuffer[(Long, Long)]
        list.append((r.getAs[Long]("favorite_id"), r.getAs[Long]("counts")))
        redisMap.put(key, list)
      }

      sb.delete(0, sb.length())
    })

    redisMap
  }



  private def getPeriod(interval: Int): Int = {
    interval match {
      case 0 => RankType.DAY_RANK
      case 7 => RankType.WEEK_RANK
      case 30 => RankType.MONTH_RANK
      case 365 => RankType.MONTH_RANK
      case -1 => RankType.TOTAL_RANK
    }

  }
}
