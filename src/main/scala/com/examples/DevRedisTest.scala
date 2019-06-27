//package com.examples
//
//import cn.ipanel.utils.PropertiUtils
//import redis.clients.jedis.Jedis
//
///**
//  * DevRedisTest<br>
//  * ${DESCRIPTION}
//  *
//  * author liujjy
//  * create 2018/7/7
//  * since  1.0.0
//  */
//object DevRedisTest {
//
//  private val pro = PropertiUtils.init("redis.properties")
//
//  def main(args: Array[String]): Unit = {
////    test1()
//    test2()
//
//  }
//
//  def test2(): Unit ={
//    val jedis = new Jedis("192.168.36.100",7379)
//    val result  = jedis.keys("dev*")
//
//    for( key <- result){
//      jedis.expire(key,4)
//    }
//
//    jedis.close()
//
//  }
//
//
//
//  def test1(): Unit ={
//    val redis = new Jedis(pro.getProperty("redis_host"), pro.getProperty("redis_port").toInt)
//    val result = redis.keys("dev*")
//
//    for( key <- result){
//      println("test2=>" + key)
//    }
//
//    redis.close()
//
//
//  }
//
//  def getRedisPool(): Unit ={
//    import redis.clients.jedis.{JedisPool, JedisPoolConfig}
//    val config = new JedisPoolConfig
//    config.setMaxTotal(pro.getProperty("max_total").toInt)
//    config.setMaxIdle(pro.getProperty("max_idle").toInt)
//    config.setMinIdle(pro.getProperty("min_idle").toInt)
//
//    config.setMaxWaitMillis(pro.getProperty("max_wait_millis").toLong)
//
//    //在获取Jedis连接时，自动检验连接是否可用
//    config.setTestOnBorrow(true)
//    //在将连接放回池中前，自动检验连接是否有效
//    config.setTestOnReturn(true)
//    //自动测试池中的空闲连接是否都是可用连接
//    config.setTestWhileIdle(true)
//    //表示idle object evitor两次扫描之间要sleep的毫秒数
//    config.setTimeBetweenEvictionRunsMillis(pro.getProperty("time_between_eviction_runs").toLong)//100000
//    //表示一个对象至少停留在idle状态的最短时间，然后才能被idle object evitor扫描并驱逐；这一项只有在timeBetweenEvictionRunsMillis大于0时才有意义
//    config.setMinEvictableIdleTimeMillis(pro.getProperty("min_evictable_idle_time").toLong)//100000
//    //表示idle object evitor每次扫描的最多的对象数
//    config.setNumTestsPerEvictionRun(pro.getProperty("tests_pereviction_run").toInt)
//
//     new JedisPool(config, "192.168.36.100", 7379)
//  }
//}
