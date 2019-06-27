package cn.ipanel.homed.realtime

import java.util.concurrent._

import cn.ipanel.common.{CluserProperties, SparkSession}
import cn.ipanel.utils.DateUtils
import org.joda.time.DateTime

object ReadUserCountThread extends App {
  val sparkSession = new SparkSession("ReadUserContThread", "")
  val sqlContext = sparkSession.sqlContext

  val service = Executors.newScheduledThreadPool(1)
  service.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      val date = DateTime.now()
      val time = date.plusSeconds(-10).toString(DateUtils.YYYY_MM_DD_HHMMSS)
      println("time ======" + date.toString(DateUtils.YYYY_MM_DD_HHMMSS))
      val df = sqlContext.load(
        "org.apache.phoenix.spark",
        Map("table" -> "t_user_online", "zkUrl" -> CluserProperties.PHOENIX_ZKURL))

      df.filter(s"time >= '${time}'").show(11111)
    }
  }, 10, 5, TimeUnit.SECONDS)

}
