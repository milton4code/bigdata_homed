package com.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * datafrmae 聚合函数使用
  */

case class Test1(bf: Int, df: Int, duration: Int, tel_date: Int)

object DataFrameWindowOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("SparkSQLInnerFunctions") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    //    conf.setMaster("spark://Master:7077") //此时，程序在Spark集群
    conf.setMaster("local")
    val sc = new SparkContext(conf) //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行的具体参数和配置信息
    val sqlContext = new SQLContext(sc) //构建SQL上下文

    //要使用Spark SQL的内置函数，就一定要导入SQLContext下的隐式转换

    val userData = Array(
      "2016-3-27,2,http://spark.apache.org/,1000",
      "2016-3-27,001,http://hadoop.apache.org/,1001",
      "2016-3-27,002,http://fink.apache.org/,1002",
      "2016-3-29,003,http://kafka.apache.org/,1020",
      "2016-3-28,004,http://spark.apache.org/,1010",
      "2016-3-28,002,http://hive.apache.org/,1200",
      "2016-3-28,001,http://parquet.apache.org/,1500",
      "2016-3-28,001,http://spark.apache.org/,1800"
    )

    val userData2 = Array(
      "2016-3-27,001,http://spark.apache.org/,1000",
      "2016-3-27,001,http://hadoop.apache.org/,1001",
      "2016-3-27,002,http://fink.apache.org/,1002",
      "2016-3-29,003,http://kafka.apache.org/,1020",
      "2016-3-28,004,http://spark.apache.org/,1010",
      "2016-3-28,002,http://hive.apache.org/,1200",
      "2016-3-28,001,http://parquet.apache.org/,1500",
      "2016-3-28,001,http://spark.apache.org/,1800"
    )


    val userDataRDD = sc.parallelize(userData) //生成DD分布式集合对象
    val userDataRDD2 = sc.parallelize(userData2) //生成DD分布式集合对象

    val userDataRDDRow = userDataRDD.map(row => {
      val splited = row.split(",")
      Row(splited(0), splited(1).toInt, splited(2), splited(3).toInt)
    })

    val userDataRDDRow2= userDataRDD2.map(row => {
      val splited = row.split(",")
      Row(splited(0), splited(1).toInt, splited(2), splited(3).toInt)
    })

    val structTypes = StructType(Array(
      StructField("time", StringType, true),
      StructField("status", IntegerType, true),
      StructField("url", StringType, true),
      StructField("amount", IntegerType, true)
    ))
    val userDataDF1 = sqlContext.createDataFrame(userDataRDDRow, structTypes)
    userDataDF1.registerTempTable("t1")
    val userDataDF2 = sqlContext.createDataFrame(userDataRDDRow2, structTypes)
    userDataDF2.registerTempTable("t2")
    userDataDF1.show()
   sqlContext.sql("""select t1.*  from t1 full join t2 """).show()

    //：内置函数生成的Column对象且自定进行CG；
    import sqlContext.implicits._
    //    userDataDF.agg(countDistinct(col("id")).alias("count")).show()
    //    userDataDF.groupBy("time").agg('time, sum('amount)).show()
    //    userDataDF.selectExpr("time","id","url","amount").sortWithinPartitions("time")

//    userDataDF.sortWithinPartitions($"time".desc).show(false)
//    userDataDF.sortWithinPartitions($"time".asc).show(false)


    //    userDataDF.groupBy("url").agg(last("time").alias("last_time")).where($"id" > 2 )
    //      .select("id","time","amout").show(false)
    //     userDataDF.groupBy("time").agg(("amount","sum")).show()

  }

  def foreachParttion(data: DataFrame) = {
    val ss: RDD[Row] = data.mapPartitions(it => {
      var list = new ArrayBuffer[Row]()

      while (it.hasNext) {
        list.append(it.next())
      }

      list.iterator
    })
    ss.collect().foreach(println(_))

  }
}
