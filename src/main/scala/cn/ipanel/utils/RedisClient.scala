package cn.ipanel.utils

import com.mysql.jdbc.StringUtils
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis._

/**
  * Redis 工具类
  */
object RedisClient extends Serializable {

  private val pro = PropertiUtils.init("redis.properties")
  @transient private var pool: JedisPool = _

  def release(jedis: Jedis): Unit = {
    assert(pool != null)
    jedis.close()
  }

  def getRedis(): Jedis = {
    makePool()
    assert(pool != null)
    pool.getResource
  }

  private def makePool(): Unit = {
    if (pool == null) {
      val poolConfig = new JedisPoolConfig
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

      import redis.clients.jedis.JedisPool

      if(StringUtils.isNullOrEmpty(pro.getProperty("redis_password"))){
        pool = new JedisPool(poolConfig,
          pro.getProperty("redis_host"),
          pro.getProperty("redis_port").toInt)
      }else{
         pool = new JedisPool(poolConfig,
          pro.getProperty("redis_host"),
          pro.getProperty("redis_port").toInt,
          pro.getProperty("redis_time_out").toInt,
          pro.getProperty("redis_password"))
      }

      val hook = new Thread {
        override def run = pool.destroy()
      }
      sys.addShutdownHook(hook.run)
    }
  }


}