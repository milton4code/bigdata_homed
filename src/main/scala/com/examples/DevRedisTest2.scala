//package com.examples
//
///**
//  * DevRedisTest<br>
//  * ${DESCRIPTION}
//  *
//  * author liujjy
//  * create 2018/7/7
//  * since  1.0.0
//  */
//object DevRedisTest2 {
//
//  def main(args: Array[String]): Unit = {
//    import redis.clients.jedis.{JedisPool, JedisPoolConfig}
//    val config = new JedisPoolConfig
//    config.setMaxTotal(200)
//    config.setMaxIdle(50)
//    config.setMinIdle(8) //设置最小空闲数
//
//    config.setMaxWaitMillis(10000)
//    config.setTestOnBorrow(true)
//    config.setTestOnReturn(true)
//    //Idle时进行连接扫描
//    config.setTestWhileIdle(true)
//    //表示idle object evitor两次扫描之间要sleep的毫秒数
//    config.setTimeBetweenEvictionRunsMillis(30000)
//    //表示idle object evitor每次扫描的最多的对象数
//    config.setNumTestsPerEvictionRun(10)
//    //表示一个对象至少停留在idle状态的最短时间，然后才能被idle object evitor扫描并驱逐；这一项只有在timeBetweenEvictionRunsMillis大于0时才有意义
//    config.setMinEvictableIdleTimeMillis(60000)
//
//    val pool = new JedisPool(config, "192.168.36.100", 7379)
//
//    val jd = pool.getResource()
//
//    print(jd.ping())
//
////    import scala.collection.JavaConversions._
//    val result = jd.keys("dev*")
//
//    for( key <- result){
//      println("key=>" + key)
//    }
//
//    jd.close()
//
//  }
//}
