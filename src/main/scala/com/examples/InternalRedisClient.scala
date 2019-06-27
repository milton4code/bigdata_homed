package com.examples

import cn.ipanel.utils.PropertiUtils
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object InternalRedisClient extends Serializable {
  private val pro = PropertiUtils.init("redis.properties")

    private var pool: JedisPool = null
    private def makePool(): Unit = {
      if(pool == null) {
        val config = new JedisPoolConfig
        config.setMaxTotal(pro.getProperty("max_total").toInt)
        config.setMaxIdle(pro.getProperty("max_idle").toInt)
        config.setMinIdle(pro.getProperty("min_idle").toInt)

        config.setMaxWaitMillis(pro.getProperty("max_wait_millis").toLong)

        //在获取Jedis连接时，自动检验连接是否可用
        config.setTestOnBorrow(true)
        //在将连接放回池中前，自动检验连接是否有效
        config.setTestOnReturn(true)
        //自动测试池中的空闲连接是否都是可用连接
        config.setTestWhileIdle(true)
        //表示idle object evitor两次扫描之间要sleep的毫秒数
        config.setTimeBetweenEvictionRunsMillis(pro.getProperty("time_between_eviction_runs").toLong)//100000
        //表示一个对象至少停留在idle状态的最短时间，然后才能被idle object evitor扫描并驱逐；这一项只有在timeBetweenEvictionRunsMillis大于0时才有意义
        config.setMinEvictableIdleTimeMillis(pro.getProperty("min_evictable_idle_time").toLong)//100000
        //表示idle object evitor每次扫描的最多的对象数
        config.setNumTestsPerEvictionRun(pro.getProperty("tests_pereviction_run").toInt)

        val hook = new Thread{
          override def run = pool.destroy()
        }
        sys.addShutdownHook(hook.run)
      }
    }

    def getPool: JedisPool = {
      makePool()
      assert(pool != null)
      pool
    }
  }