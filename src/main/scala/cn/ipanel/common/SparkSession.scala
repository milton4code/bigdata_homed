package cn.ipanel.common

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark 基本配置
  *
  * @param appName
  * @param master
  */
class SparkSession(appName: String, master: String="") {


  val conf = new SparkConf()
  conf.setAppName(appName)

//
  conf.set("spark.sql.codegen", "true")
  conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
  conf.set("spark.sql.autoBroadcastJoinThreshold", "209715200") //小于200M就会broadcast
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  private[this] val os_name = System.getProperties().get("os.name").toString
  if (os_name.toLowerCase.contains("windows")) {
    System.setProperty("hadoop.home.dir", "E:\\study\\hadoop\\winutils-master\\hadoop-2.6.4")
    conf.setMaster("local[*]")
  }

  val sparkContext = new SparkContext(conf)
  val sqlContext = new HiveContext(sparkContext)

  sqlContext.setConf("hive.exec.dynamic.partition", "true")
  sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

  def stop()={
    sparkContext.stop()
  }
}

/**
  * 提供apply 方法，工厂模式，最佳的实践就是集成和模式匹配；
  */
object SparkSession {
  def apply(appName: String, master: String=""): SparkSession = {
    new SparkSession(appName, master)
  }
}