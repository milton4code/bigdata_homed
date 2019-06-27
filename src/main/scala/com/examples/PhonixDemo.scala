//package com.examples
//
//import java.util.Random
//
//import cn.ipanel.common.SparkSession
//import cn.ipanel.utils.{DBUtils, DateUtils}
//import org.apache.log4j.Logger
//
//import scala.collection.mutable.ListBuffer
//
///**
//  *
//  *
//  * @author liujjy
//  * @date 2018/01/29 13:55
//  */
//
//object PhonixDemo extends App {
//
//  case class abc(time: String, id: Long, payload: String)
//
//  Log4jPrintStream.redirectSystemOut()
//  val logger = Logger.getLogger(this.getClass)
//  val sparkSession = new SparkSession("PhonixDemo", "")
//  //  sparkSession.sparkContext.addJar("E:\\study\\phoenix\\lib\\phoenix-4.8.1-HBase-1.2-client.jar")
//  //  sparkSession.sparkContext.addJar("E:\\study\\phoenix\\lib\\phoenix-4.8.1-HBase-1.2-server.jar")
//
//  val buffer1 = new ListBuffer[abc]()
//  val random = new Random()
//
//  buffer1.append(abc(DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS), 122, "1111"))
//  buffer1.append(abc(DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS) + 1, 122, "aaaaaaa"))
//  buffer1.append(abc(DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS) + 2, 122, "aaaaaaa"))
//
//
//  val df = sparkSession.sqlContext.createDataFrame(buffer1)
//  DBUtils.saveDataFrameToPhoenixNew(df, "my_table2")
//
//}
