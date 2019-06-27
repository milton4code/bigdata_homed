package cn.ipanel.homed.realtime

import java.util

import cn.ipanel.common.{Constant, SparkSession}
import cn.ipanel.utils.RedisClient
import org.apache.spark.sql.{DataFrame, SQLContext}
import redis.clients.jedis.{Jedis, ScanParams, ScanResult}

import scala.collection.mutable.ListBuffer

object JuniteTest {
  val prefix = "abc"
  val REDIS_INDEX = 10


  def main(args: Array[String]): Unit = {
    val sparkSession = new SparkSession("ss", "")
    val sqlContext = sparkSession.sqlContext
    loadPreviousUserStatus(sqlContext)

  }

  /**
    * 加载前一次用户状态
    *
    * @return device_id|device_type|DA|province_id|city_id|region_id|report_time
    */
  def loadPreviousUserStatus(sqlContext: SQLContext) = {
    val start = System.currentTimeMillis()

    val mpa = loadUserStatusFromReids2()
    println(" ================" + (System.currentTimeMillis() - start))

    println(mpa.size)
  }

  /**
    * redis数据转成DataFrame
    *
    * @return device_id|device_type|DA|province_id|city_id|region_id|play_type|report_time|receive_time
    */
  def redisToDataFrame(redisResponse: Map[String, String], sqlContext: SQLContext): DataFrame = {
    //    println("========redisToDataFrame======size=======" + redisResponse.size)
    val buffer = new ListBuffer[CachUser]
    redisResponse.foreach(x => {
      val device_id = x._1.split("\\" + Constant.REDIS_SPILT)(1).toLong
      val DA = x._1.split("\\" + Constant.REDIS_SPILT)(2).toLong
      val playType = x._1.split("\\" + Constant.REDIS_SPILT)(3)
      val paras = x._2.split("\\" + Constant.REDIS_SPILT)
      buffer.append(CachUser(device_id, DA, playType, paras(0), paras(1), paras(2), paras(3), paras(4).toLong, paras(5).toLong))
    })

    val userStatusDF = sqlContext.createDataFrame(buffer)
    userStatusDF
  }


  private def loadUserStatusFromReids2(): Map[String, String] = {
    import redis.clients.jedis.ScanParams.SCAN_POINTER_START
    var responses = Map[String, String]()
    val jedis = new Jedis("localhost")
    jedis.select(2)

    val scanParams: ScanParams = new ScanParams().count(10000).`match`( "*")
    var scanCursor: String = SCAN_POINTER_START
    // scala遍历java集合时需要导入此类
    import scala.collection.JavaConversions._
    do {
      val scanResult: ScanResult[String] = jedis.scan(scanCursor, scanParams)
      scanCursor = scanResult.getStringCursor
      for (key: String <- scanResult.getResult) {
        val values: Seq[String] = jedis.zrevrange(key, 0, -1).toSeq
        responses += (key -> values.head)
        /*  if (values.size > 1) { // 至少保留一条记录
            jedis.zrem(key, values.tail: _*) // : _*  scala传递可变参特殊用法
          }*/
      }
      scanCursor = scanResult.getStringCursor
    } while (!scanCursor.eq(SCAN_POINTER_START))

    jedis.close()
    responses
  }

  /**
    * 从redis中加载用户状态
    */
  def loadUserStatusFromReids(): Map[String, String] = {
    //    var responses = Map[String, Response[String]]()
    var responses = Map[String, String]()
//    val jedis = RedisClient.getRedis()
    val jedis = new Jedis("localhost")
    jedis.select(2)

    //    val pipeline = jedis.pipelined()
    var scanCursor = "0"
    var scanResult: java.util.List[String] = new util.ArrayList[String]()

    // scala遍历java集合时需要导入此类
    import scala.collection.JavaConversions._
    do {
      val scan: ScanResult[String] = jedis.scan(scanCursor, new ScanParams().`match`(prefix + "*"))
      scanCursor = scan.getStringCursor
      for (key: String <- scan.getResult) {
        //        responses += (key -> jedis.get(key))
        //        val maxValue = jedis.zrevrange(key, 0, -1).toSeq.head

        val values: Seq[String] = jedis.zrevrange(key, 0, -1).toSeq

        responses += (key -> values.head)
        if(values.size > 1){
          jedis.zrem(key,values.tail:_*)
        }
      }
    } while (!"0".equals(scanCursor) )
    //    pipeline.sync()
    RedisClient.release(jedis)
    responses
  }

  case class CachUser(device_id: Long = 0, DA: Long = 0, play_type: String = "", device_type: String = "",
                      province_id: String = "", city_id: String = "", region_id: String = "",
                      report_time: Long = 0, receive_time: Long = 0)

}
