package com.examples

import cn.ipanel.utils.PropertiUtils
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.Jedis

object RedisUtil{
  private var jPool: JedisPool = _
  private val pro = PropertiUtils.init("redis.properties")
  def getRedis() : Jedis = {
    if(null == jPool){
      val poolConfig = new JedisPoolConfig()
      poolConfig.setMaxTotal(pro.getProperty("max_total").toInt)
      poolConfig.setMaxIdle(pro.getProperty("max_idle").toInt)
      poolConfig.setMinIdle(pro.getProperty("min_idle").toInt)

      poolConfig.setMaxWaitMillis(pro.getProperty("max_wait_millis").toLong)

      //在获取Jedis连接时，自动检验连接是否可用
      poolConfig.setTestOnBorrow(true)
      //在将连接放回池中前，自动检验连接是否有效
      poolConfig.setTestOnReturn(true)
      //自动测试池中的空闲连接是否都是可用连接
      poolConfig.setTestWhileIdle(true)
      //表示idle object evitor两次扫描之间要sleep的毫秒数
      poolConfig.setTimeBetweenEvictionRunsMillis(pro.getProperty("time_between_eviction_runs").toLong)//100000
      //表示一个对象至少停留在idle状态的最短时间，然后才能被idle object evitor扫描并驱逐；这一项只有在timeBetweenEvictionRunsMillis大于0时才有意义
      poolConfig.setMinEvictableIdleTimeMillis(pro.getProperty("min_evictable_idle_time").toLong)//100000
      //表示idle object evitor每次扫描的最多的对象数
      poolConfig.setNumTestsPerEvictionRun(pro.getProperty("tests_pereviction_run").toInt)
      jPool = new JedisPool(poolConfig, pro.getProperty("redis_host"),pro.getProperty("redis_port").toInt)
    }
    jPool.getResource
  }
  
  def returnRedis(jedis : Jedis) : Unit = {
    assert(jPool != null)
    jedis.close()
  }
  
    def getHash(jedis:Jedis,key:String,field:String):String = {
      var result = "nocid"
      var channelId = jedis.hget(key, field)   
      if(channelId != null){
       result = channelId
     }
      result
  }
  
 
}