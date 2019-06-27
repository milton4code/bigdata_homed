package com.examples

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object SparkAggDemo {
  case class Test(bf: Int, df: Int, duration: Int, tel_date: Int)
  case class Tmp(regionId:String,count:Int)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("dem0")
    val sc = new SparkContext(conf)
     val sqlContext = new SQLContext(sc)

    val schema=StructType(List(
      //指定schema
      StructField("id",StringType,true),
      StructField("score",StringType,true)
    ))



    val list = new ListBuffer[Tmp]()
    list.append(Tmp("11",1233))

    import sqlContext.implicits._
    val rr: DataFrame = sqlContext.createDataFrame(list)
    rr.show()



//        sqlContext.createDataFrame(Row("1","0"),StructType(List(StructField("region"),StringType,true),StructField("region"),StringType,true))

//    createDataFrame(sparkContext.emptyRDD[Row], StructType(Nil))

//    import  sqlContext.implicits._
//    val df = Seq(Test(1,1,1,1), Test(1,1,2,2), Test(1,1,3,3), Test(2,2,3,3), Test(2,2,2,2), Test(2,2,1,1)).toDF
//
//
//    df.show()
//
//    df.groupBy("bf").max("duration").show()
  }
}
