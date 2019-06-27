package cn.ipanel.rank.reports

import cn.ipanel.common.{CluserProperties, SparkSession}
import cn.ipanel.rank.common.Constant
import cn.ipanel.utils.{DBUtils, PropertiUtils}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.DataFrame
import redis.clients.jedis.Jedis

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * 个人观看频道排行
  *
  * @author ZouBo
  * @date 2018/6/6 0006 
  */
object UserChannelRank {
  private val pro = PropertiUtils.init("redis.properties")

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("参数错误,请传入正确参数")
      System.exit(1)
    }
    val day = args(0).toInt

    val partitionNum = args(1).toInt

    val session = SparkSession("UserChannelRank", "")
    val sqlContext = session.sqlContext

    val sql =
      s"""
         |(select f_before_date,f_date,f_user_id,f_terminal,f_channel_id,f_sum_time
         |from t_user_channel
         |where f_date='$day') as t_user_channel
      """.stripMargin
    val userDF = DBUtils.loadDataFromPhoenix2(sqlContext, sql)
    val result = computeChannelRank(userDF, partitionNum)

    val jedis = new Jedis(pro.getProperty("redis_host"), pro.getProperty("redis_port").toInt, pro.getProperty("redis_time_out").toInt)
    val pipeline = jedis.pipelined
    result.collect.foreach(tuple => {
      val key = tuple._1
      val value = encodeToRedis(tuple._2)
      pipeline.hset(Constant.REDIS_DB_NAME.getBytes, key.getBytes, value)
    })
    pipeline.sync()
//    jedis.publish("350","")
    jedis.close()
    sendSuccessMessage()
  }

  def computeChannelRank(df: DataFrame, partitionNum: Int) = {
    //1.按用户分组
    val reduceRdd = df.mapPartitions(it => {
      val list = new ListBuffer[(String, Array[String])]
      it.foreach(row => {
        //        val sunday = row.getAs[String]("F_DATE")
        val userId = row.getAs[String]("F_USER_ID")
        val terminal = row.getAs[String]("F_TERMINAL")
        val channelId = row.getAs[String]("F_CHANNEL_ID")
        val playTimes = row.getAs[Double]("F_SUM_TIME")

        val result = playTimes + "," + channelId
        list += ((userId + "_" + terminal, Array(result)))
      })
      list.iterator
    }).reduceByKey((a, b) => sort(a, b), partitionNum)

    val result = reduceRdd.mapPartitions(it => {
      val list = new ListBuffer[(String, ArrayBuffer[Long])]()
      it.foreach(x => {
        val userIdAndTerminal = x._1.split("_")
        val userId = userIdAndTerminal(0)
        val terminal = userIdAndTerminal(1)
        val channelArrs = x._2
        val mark = terminal match {
          case "1" => Constant.CHANNEL_RANK_STB
          case "2" => Constant.CHANNEL_RANK_STB
          case "3" => Constant.CHANNEL_RANK_MOB
          case _ => Constant.CHANNEL_RANK_PC
        }
        val key = mark + "_" + userId
        val buffer = new ArrayBuffer[Long]()
        if (channelArrs.length > Constant.CHANNEL_RANK_TOP_TEN) {
          for (channel <- channelArrs if channelArrs.indexOf(channel) < Constant.CHANNEL_RANK_TOP_TEN) {
            val channelId = channel.split(",")(1).toLong
            buffer.append(channelId)
          }
        } else {
          for (channel <- channelArrs) {
            val channelId = channel.split(",")(1).toLong
            buffer.append(channelId)
          }
        }
        list += ((key, buffer))
      })
      list.iterator
    })
    result
  }

  val getUint32_t = (l: Long) => {
    l & 0x00000000ffffffff
  }

  /**
    * 将序排序
    *
    * @param arr1
    * @param arr2
    * @return
    */
  def sort(arr1: Array[String], arr2: Array[String]): Array[String] = {
    val buffer = arr1.toBuffer
    buffer ++= arr2
    val result = buffer.toArray
    for (i <- 0 until result.length - 1) {
      for (j <- 0 until result.length - 1 - i) {
        val value1 = result(j).split(",")(0).toDouble
        val value2 = result(j + 1).split(",")(0).toDouble
        if (value1 < value2) {
          val temp = result(j)
          result(j) = result(j + 1)
          result(j + 1) = temp
        }
      }
    }
    result
  }

  /**
    * 压缩
    *
    * @param data
    * @return
    */
  def encodeToRedis(data: ArrayBuffer[Long]) = {
    val arrayByte = new scala.collection.mutable.ArrayBuffer[Byte]()

    def writeToByte(n: Long): Unit = {
      var m = n
      while ((m & ~0x7F) != 0) {
        arrayByte += ((m & 0x7F) | 0x80).toByte
        m = m >>> 7
      }
      arrayByte += m.toByte
    }

    writeToByte(data.length * 1)
    //first write reason and last write pid
    data.foreach(v => {
      writeToByte(v)
    })

    arrayByte.toArray
  }

  /**
    * 解码
    *
    * @param data
    * @return
    */
  def decodeFromByte(data: Array[Byte]) = {
    val intData = new scala.collection.mutable.ArrayBuffer[Long]()
    var shift = 0
    var totalData = 0
    for (cur <- data) {
      if ((cur & 0x80) != 0) { //判断正负
        val curData = cur & 0x7f
        totalData |= curData << shift
        shift += 7
      }
      else {
        val curData = cur & 0x7f
        import math._
        val res = totalData.toLong + curData * pow(2, shift).toLong
        intData.append(res)
        shift = 0
        totalData = 0
      }
    }
    intData
  }

  /**
    * 发送确认put请求
    */
  def sendSuccessMessage() = {
    val url = CluserProperties.CHANNEL_PUT_URL
    try {
      val httpClient = HttpClients.createDefault()
      val httpGet = new HttpGet(url)
      val response = httpClient.execute(httpGet)
      val httpEntity = response.getEntity
      if (null != httpEntity) {
        val result=EntityUtils.toString(httpEntity, "UTF-8")
        EntityUtils.consume(httpEntity)
        println("请求通讯[" + url + "]成功:"+result)
      }
    } catch {
      case e: Exception => println("请求通讯[" + url + "]异常,异常信息:" + e.printStackTrace())
    }
  }


}
