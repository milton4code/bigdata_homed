package com.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object MutiPathTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\study\\hadoop\\winutils-master\\hadoop-2.6.4")
    val conf = new SparkConf().setAppName("TestUDF").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

     val inputPath = List("file:///D:/data/dev/*","").mkString(",")
      sc.textFile(inputPath).foreach(println(_))
  }
}
