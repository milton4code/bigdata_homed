//package com.examples
//
//import cn.ipanel.utils.RedisClient
//import redis.clients.jedis.ScanParams.SCAN_POINTER_START
//import redis.clients.jedis.{Jedis, Response}
//
//import scala.util.Random
//
//
///**
//  *
//  *
//  * @author liujjy
//  * @date 2018/01/18 15:21
//  */
//
//object RedisTest extends App {
//
//  val prefix = "abc"
//  val DB_INDEX = 10
////  zadd_add()
////  zadd_get()
//
//
////  testUsePipeline()
//
//
//  testPipelineGet()
//
//  def testPipelineGet(): Unit ={
//    val jedis = new Jedis("192.168.18.63",6379)
//    val pip = jedis.pipelined()
//    pip.select(14)
//    val s: Response[util.Set[String]] = pip.smembers("users")
//    pip.sync()
//    val unit = s.get()
//    for (s <- unit){
//      println(s)
//    }
//
//  }
//
//  def testFlushDB(): Unit = {
//    val jedis = new Jedis("localhost")
//    val pipelined = jedis.pipelined()
//    pipelined.select(4)
//    pipelined.flushDB()
//    pipelined.close()
//  }
//
//  def testAdd(): Unit = {
//    val jedis = new Jedis("localhost")
//    val piplied = jedis.pipelined()
//    piplied.select(4)
//    piplied.set("aaaa1","xxx")
//    piplied.set("aaaa2","xxx")
//    piplied.set("aaaa3","xxx")
//
//    piplied.close()
//  }
//
//  /**
//    * redis性能测试对比
//    */
//  def performance(): Unit = {
//    testNonPipelined()
//    testUsePipeline()
//  }
//
//
//  //性能测试 不用pipeline
//  def testNonPipelined(): Unit = {
//    val jedis = new Jedis("localhost")
//    //    val pipeline = jedis.pipelined
//    val start: Long = System.currentTimeMillis
//    var i = 0
//    while ( {
//      i < 100000
//    }) {
//      jedis.set("p" + i, "p" + i)
//
//      {
//        i += 1;
//        i - 1
//      }
//    }
//    jedis.close()
//    /*   val results = pipeline.syncAndReturnAll*/
//    val end: Long = System.currentTimeMillis
//    System.out.println("jedis SET: " + ((end - start) / 1000.0) + " seconds")
//    jedis.disconnect()
//  }
//
//  //性能测试 用pipeline
//  def testUsePipeline(): Unit = {
//    val jedis = new Jedis("192.168.18.63",6379)
//    val start: Long = new Date().getTime
//    jedis.flushDB
//    val p = jedis.pipelined
//    p.select(14)
//    var i = 0
//    while (i < 10) {
//      p.sadd("users",i.toString)
//
//
//      i = i+1
//    }
//    p.sync // 一次性发给redis-server
//
//    System.out.println(jedis.keys("age*"))
//    val end: Long = System.currentTimeMillis()
//    System.out.println("use pipeline cost:" + (end - start) + "ms")
//  }
//
//  def testScan2(): Unit = {
//    import redis.clients.jedis.{Jedis, ScanParams, ScanResult}
//    val jedis = new Jedis("localhost")
//    jedis.select(2)
//    val map = new util.HashMap[String, String]()
//    val scanParams: ScanParams = new ScanParams().count(100).`match`("p*")
//    var cur: String = SCAN_POINTER_START
//    do {
//      val scanResult: ScanResult[String] = jedis.scan(cur, scanParams)
//
//      import scala.collection.JavaConversions._
//      for (key: String <- scanResult.getResult) {
//        val values: Seq[String] = jedis.zrevrange(key, 0, -1).toSeq
//        map += (key -> values.head)
//        /*  if (values.size > 1) { // 至少保留一条记录
//            jedis.zrem(key, values.tail: _*) // : _*  scala传递可变参特殊用法
//          }*/
//      }
//
//      cur = scanResult.getStringCursor
//    } while (!(cur == SCAN_POINTER_START)
//    )
//    jedis.close()
//
//    println(map.size())
//  }
//
//
//  def testScan() {
//    import redis.clients.jedis.{Jedis, ScanParams, ScanResult}
//    // 创建一个jedis的对象。
//    val jedis = new Jedis("localhost", 6379)
//    //    jedis.auth("zhifu123")
//    // 调用jedis对象的方法，方法名称和redis的命令一致。
//    //    jedis.select(2)
//    /*    val scanParams = new ScanParams
//        scanParams.`match`("p*")
//        scanParams.count(10000)*/
//
//    val scanParams = new ScanParams()
//    scanParams.count(1000)
//    scanParams.`match`("p*")
//    val start = System.currentTimeMillis()
//    var scanCursor = "0"
//    var responses = Map[String, String]()
//    // scan(curso,params) cursor 表示开始遍历的游标   params 是ScanParams 对象，此对象可以设置 每次返回的数量，以及遍历时的正则表达式
//    // 需要注意的是，对元素的模式匹配工作是在命令从数据集中取出元素之后，向客户端返回元素之前的这段时间内进行的，
//    //  所以如果被迭代的数据集中只有少量元素和模式相匹配，那么迭代命令或许会在多次执行中都不返回任何元素。
//
//    // scala遍历java集合时需要导入此类
//    do {
//      val scan: ScanResult[String] = jedis.scan(scanCursor, scanParams)
//      val res: util.List[String] = scan.getResult
//      res.foreach(x => {
//        println(x)
//      })
//      //      val scan: ScanResult[String] = jedis.scan("0", scanParams)
//      //      val res: ScanResult[Tuple] = jedis.zscan("p","0",scanParams)
//      //      System.out.println("scan：返回用于下次遍历的游标" + res.getStringCursor)
//      //      scanCursor = res.getStringCursor
//      //
//      //      val tuples: util.List[Tuple] = res.getResult
//      //      tuples.foreach(tuple=>{
//      //        println(tuple.getScore)
//      //      })
//
//    } while (!"0".equals(scanCursor))
//    jedis.close()
//    val end = System.currentTimeMillis()
//    println("size" + responses.size)
//    println("cost==" + (end - start) / 1000)
//
//    /* System.out.println("scan：返回用于下次遍历的游标" + scan.getStringCursor)
//     System.out.println("scan：返回结果" + scan.getResult)*/
//    // 关闭jedis。
//  }
//
//  def test3Pipelined(): Unit = {
//    val jedis = new Jedis("localhost")
//    val pipeline = jedis.pipelined
//    pipeline.select(2)
//    val start: Long = System.currentTimeMillis
//    var i = 0
//    while ( {
//      i < 10000
//    }) {
//      pipeline.zadd("p" + i, math.random, "p" + i)
//
//      {
//        i += 1;
//        i - 1
//      }
//    }
//    val results = pipeline.syncAndReturnAll
//    val end: Long = System.currentTimeMillis
//
//    System.out.println("Pipelined SET: " + ((end - start) / 1000.0) + " seconds")
//    pipeline.close()
//  }
//
//  def test(): Unit = {
//    val start = System.currentTimeMillis()
//    zadd_add()
//    val end = System.currentTimeMillis()
//    println("  redis cost :::" + (end - start))
//  }
//
//  def zadd_add(): Unit = {
//    val random = new Random()
//    val jedis = RedisClient.getRedis()
//    jedis.select(10)
//    for (i: Int <- 0 until 1000) {
//      jedis.zadd(prefix + random.nextInt(10), math.random, "name|time|pro|city" + i)
//    }
//    RedisClient.release(jedis)
//  }
//
//  def zadd_get(): Unit = {
//    val jedis = RedisClient.getRedis()
//    jedis.select(10)
//    //    zrevrange
//    val set: util.Set[String] = jedis.zrevrange(prefix, 0, -1)
//    for (key: String <- set) {
//      println(key)
//    }
//
//  }
//
//
//}
