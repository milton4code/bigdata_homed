//package com.examples
//
//import cn.ipanel.utils.RedisClient
//import redis.clients.jedis.{Response, ScanParams, ScanResult}
//
///**
//  *
//  *
//  * @author liujjy
//  * @date 2018/01/25 19:23
//  */
//
//object OnlineTest extends App {
//
//  /*val sparkSession = SparkSession("OnlineTest", "")
//  val sparkContext = sparkSession.sparkContext
//
//  */
//
//
//
//
//  //  dev()
//
//  def test()={
//    var responses = Map[String, Response[String]]()
//    val jedis = RedisClient.getRedis()
//
//    jedis.select(2)
//    val pipeline = jedis.pipelined()
//    //  pipeline.select(2)
//    var scanCursor = "0"
//    var scanResult: java.util.List[String] = new util.ArrayList[String]()
//
//    //  // scala遍历java集合时需要导入此类
//    import scala.collection.JavaConversions._
//
//    do {
//      //         val scan: ScanResult[String] = jedis.scan(scanCursor, new ScanParams().`match`(SECOND_USER + Constant.REDIS_SPILT+"*" ))
//      val scan: ScanResult[String] = jedis.scan(scanCursor, new ScanParams().`match`("second_user|*"))
//
//      scanCursor = scan.getStringCursor
//      for (key: String <- scan.getResult) {
//
//        println("key: " + key)
//        println("key: " + pipeline.get(key))
//        //      responses.put(key,pipeline.get(key))
//        responses += (key->pipeline.get(key))
//      }
//    } while (!scanCursor.equals("0") && !scanResult.isEmpty)
//    pipeline.sync()
//    RedisClient.release(jedis)
//
//    responses.foreach(x=>{
//      println(x._2.get())
//      println(x._1)
//    })
//  }
//
//}
