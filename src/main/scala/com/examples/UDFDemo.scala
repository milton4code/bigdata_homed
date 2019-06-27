//package com.examples
//
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
//  *
//  *
//  * created by liujjy at 2018/03/29 12:02
//  */
//
//object UDFDemo {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("TestUDF").setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//
//    import sqlContext.implicits._
//
//    val sales = Seq(
//      (1, "Widget Co", 1000.00, 0.00, "AZ", "2014-01-01"),
//      (2, "Acme Widgets", 2000.00, 500.00, "CA", "2014-02-01"),
//      (3, "Widgetry", 1000.00, 200.00, "CA", "2015-01-11"),
//      (4, "Widgets R Us", 2000.00, 0.0, "CA", "2015-02-19"),
//      (5, "Ye Olde Widgete", 3000.00, 0.0, "MA", "2015-02-28")
//    )
//
//    val salesRows = sc.parallelize(sales, 4)
//    val salesDF = salesRows.toDF("id", "name", "sales", "discount", "state", "saleDate")
//    salesDF.registerTempTable("sales")
//
////    val current = DateRange(Timestamp.valueOf("2015-01-01 00:00:00"), Timestamp.valueOf("2015-12-31 00:00:00"))
////    val yearOnYear = new YearOnYearBasis(current)
//
////    sqlContext.udf.register("yearOnYear", "")
////    val dataFrame = sqlContext.sql("select yearOnYear(sales, saleDate) as yearOnYear from sales")
////    dataFrame.show()
////    sqlContext.udf.register("devieTypeToNum",new devieTypeToNum())
//
//
//
//  }
//
//  def devieTypeToNum(devieType:String)={
//    devieType.toLowerCase match {
//      case "机顶盒" =>"1"
//      case "stb" =>"1"
//      case "smartcard" =>"2"
//      case "mobile" =>"3"
//      case "pad" =>"4"
//      case "pc" =>"5"
//      case  _  =>"0"
//    }
//  }
//}
