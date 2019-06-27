package com.examples

import cn.ipanel.common.SparkSession
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object MainExample {

  def main(arg: Array[String]) {

    var logger = Logger.getLogger(this.getClass())

//    if (arg.length < 2) {
//      logger.error("=> wrong parameters number")
//      System.err.println("Usage: MainExample <path-to-files> <output-path>")
//      System.exit(1)
//    }

    val jobName = "MainExample"

    val conf = new SparkConf()  setAppName jobName
//    val sc = new SparkContext(conf)

    val session = new SparkSession("aa")
     val sc = session .sparkContext

    val sqlContext = session.sqlContext

    sqlContext.sql("show tables").show()

    sqlContext.sql("select * from bigdata.orc_nginx_log").show()
    Thread.sleep(222222222222L)
    session.stop()
  }
}
