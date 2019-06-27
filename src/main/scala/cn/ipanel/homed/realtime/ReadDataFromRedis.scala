package cn.ipanel.homed.realtime

import java.util.concurrent.{Executors, TimeUnit}

import cn.ipanel.utils.{DateUtils, RedisClient}
import redis.clients.jedis.ScanParams.SCAN_POINTER_START


object ReadDataFromRedis {
  private lazy val SECOND_USER = "p"

  def main(args: Array[String]): Unit = {
    val runnable = new Runnable {
      override def run(): Unit = {
        loadFromRedis()
      }
    }
    val service = Executors.newSingleThreadScheduledExecutor()
    service.scheduleAtFixedRate(runnable, 1, 2, TimeUnit.SECONDS)

  }

  def loadFromRedis(): Unit = {
    import redis.clients.jedis.{ScanParams, ScanResult}
    val jedis = RedisClient.getRedis()
    //    val jedis = new Jedis("localhost")
    jedis.select(13)
    var responses = Map[String, String]()
    val scanParams: ScanParams = new ScanParams().count(1000).`match`("*")
    var cur: String = SCAN_POINTER_START

    import scala.collection.JavaConversions._
    do {
      val scanResult: ScanResult[String] = jedis.scan(cur, scanParams)
      for (key: String <- scanResult.getResult) {
        val values: Seq[String] = jedis.zrevrange(key, 0, -1).toSeq
        responses += (key -> values.head)
        if (values.size > 1) { // 至少保留一条记录
          jedis.zrem(key, values.tail: _*) // : _*  scala传递可变参特殊用法
        }
      }
      cur = scanResult.getStringCursor
    } while (!(cur == SCAN_POINTER_START)
    )
    RedisClient.release(jedis)
    //  jedis.close()
    println("=========" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS) + "==================")
    println(responses.size())
    //    responses.foreach(println(_))

  }
}
