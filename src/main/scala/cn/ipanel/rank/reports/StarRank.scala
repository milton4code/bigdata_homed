package cn.ipanel.rank.reports

import cn.ipanel.common.{DBProperties, SparkSession}
import cn.ipanel.utils._
import org.apache.spark.sql.Row

import scala.collection.mutable.{HashMap => scalaHasMap}
import scala.collection.mutable.ArrayBuffer
import cn.ipanel.rank.common.{Constant => RankConstant}
import org.apache.spark.sql.hive.HiveContext

/**
  * 明星排行榜发送到redies
  **/

object StarRank {
  private val pro = PropertiUtils.init("redis.properties")

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("请输入正确参数[yyyyMMdd]")
      System.exit(1)
    }
    //月榜最后一日
    val lastDay = args(0).trim
    //月榜第一日
    val firstDay = DateUtils.getNDaysAfter(-30, lastDay)
    val session = SparkSession("StarRank")
    val hiveContext = session.sqlContext
    val sql1 =
      """
        |(select f_period_type,f_star_id ,f_star_heat,f_star_name from t_start_heat_rank) as aa
      """.stripMargin
    val rankDF1 = DBUtils.loadMysql(hiveContext, sql1, DBProperties.JDBC_URL_RANK, DBProperties.USER, DBProperties.PASSWORD)

    import hiveContext.implicits._
    import hiveContext.sql
    rankDF1.registerTempTable("t_star_basic")
    val rankDF = sql(
      """
        |select cast(counts as bigint) as counts,1112L as type,a.f_star_id as favorite_id,
        |"100" as f_parent_id,"0" as function_id
        |from
        |(
        |select f_star_id ,f_star_heat,
        |dense_rank() over(partition by f_period_type order by f_star_heat desc) counts
        |from t_star_basic
        |) a
        |where a.counts<=100
      """.stripMargin)
      .collect()
    //Map(100_1112_0_1 -> ArrayBuffer((4210000730,1), (4210000593,2),
    val redisMap = rddToMap(rankDF, 5)
    //println("====================print log====================")
    saveToRedis(redisMap)
    //postToRecommend()
    session.stop()
  }

  /**
    * 将rdd封装成Map结构
    * key
    * column_id：数值型，根栏目id
    *   contenttype:数值型，节目类型。0为混合类型，其它值按homed系统定义
    *   accordtype:数值型，排行统计依据。取值0，代表按热度排行；取值1，代表按点击量排行；取值2，代表按搜索量排行
    *   period:数值型，排行榜统计时段。取值1，代表日榜；取值2，代表周榜；取值3，代表月榜；取值4，代表年度榜；取值5，代表总榜
    * value
    * ArrayList 结构, 元素为剧集id 以及收藏(追剧后或者关注)数量
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

  import redis.clients.jedis.Jedis

  private def saveToRedis(redisMap: scalaHasMap[String, ArrayBuffer[(Long, Long)]]): Unit = {
    //    redis.select(15)

    //val redis = new Jedis(pro.getProperty("redis_host"), pro.getProperty("redis_port").toInt)
    val redis = RedisClient.getRedis()
    redisMap.foreach(r => {
      redis.hset("t_rank_list".getBytes, r._1.getBytes(), encodeToRedisDD(r._2))
      print()
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

  def printLog(debug: Boolean, redisMap: scalaHasMap[String, ArrayBuffer[(Long, Long)]], hiveContext: HiveContext): Unit = {

    hiveContext.sql("select * from t_favorite").show(200, false)

    if (debug) {
      redisMap.foreach(x => {
        println(x._1 + "===>" + x._2)
      })
    }

  }

}
