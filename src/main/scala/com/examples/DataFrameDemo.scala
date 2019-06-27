package com.examples

import cn.ipanel.common.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  *
  *
  * @author liujjy  
  * @date 2018/01/31 16:00
  */

object DataFrameDemo extends App {

  val conf = new SparkConf()
  conf.setAppName("xx").setMaster("local[2]")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  //
  conf.set("spark.sql.codegen", "true")
  conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
  conf.set("spark.sql.autoBroadcastJoinThreshold", "209715200") //小于200M就会broadcast
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //
  //
//  val sparkSession = new SparkSession("ss", "")
//  val sqlContext = sparkSession.sqlContext
  val buffer1 = new ListBuffer[UserStatus]()
  buffer1.append(UserStatus(121, "1", 333, 1112))
  buffer1.append(UserStatus(122, "3", 333, 1111))
  buffer1.append(UserStatus(124, null, 331, 1111))
  buffer1.append(UserStatus(125, null, 331, 1111))

  val buffer2 = new ListBuffer[UserStatus2]()
  buffer2.append(UserStatus2(120, "1", 333, 1112))
  buffer2.append(UserStatus2(122, "4", 333, 1111))
  buffer2.append(UserStatus2(123, "123", 331, 1111))


  val df1 = sqlContext.createDataFrame(buffer1)
  val df2 = sqlContext.createDataFrame(buffer2)

  var demandDF = sqlContext.emptyDataFrame

  demandDF.unionAll(df1).show()
//  df2.filter("1=1").show(30)

//  df1.map(_.toString()).foreach(println(_))
//   df1.map(_.toString()).saveAsTextFile("/r2/testaa")

//  df1.select("DA").except(df2.select("DA")).show()
//  df1.except(df2).show()


  import org.apache.spark.sql._

//  df2.selectExpr("DA").toDF("中文").show()
//  val emdf = sqlContext.emptyDataFrame

//  emdf.unionAll(df1).show()

//  df1.columns.sorted.foreach(println(_))

  //  df2.cache()
  //  df2.selectExpr("count(1) as count","sum(DA) as sum").show()

  //  sqlContext.sql("select * from t1 ").show(20)

  //  Thread.sleep(Long.MaxValue)

//  def testRedisFlushDB(): Unit = {
//    val log = new ListBuffer[CacheLog]()
//    log.append(CacheLog("12", "1", false))
//    log.append(CacheLog("13", "1", true))
//    log.append(CacheLog("15", "1", true))
//    log.append(CacheLog("16", "1", false))
//    val logDF = sparkSession.sqlContext.createDataFrame(log)
//    println(" num ==" + logDF.rdd.getNumPartitions)
//    flushDB()
//    logDF.foreachPartition(f => {
//      val jedis = new Jedis("localhost")
//      val pip = jedis.pipelined()
//      pip.select(7)
//
//      while (f.hasNext) {
//        val r = f.next()
//        //        pip.set(r.getString(0), r.getString(1))
//      }
//      pip.set("ssssss", "xxxxxxaaaaaaaaa")
//      pip.sync()
//      jedis.close()
//    })
//
//  }

  def flushDB(): Unit = {
    val jedis = new Jedis("localhost")
    val pip = jedis.pipelined()
    pip.select(7)
    pip.flushDB()
    pip.sync()
    jedis.close()
  }

  import sqlContext.implicits._

  def mapToDataFrame(): Unit = {
    val m = Map("A" -> 0.11164610291904906, "B" -> 0.11856755943424617, "C" -> 0.1023171832681312)
    m.map(x => {
      demo2(x._1 + "222", x._2.toString)
    }).toSeq.toDF().show()


  }


  def dataToMap(_df1: DataFrame) = {
    /*{
         val list: util.List[Row] = _df1.collectAsList()
         val map = new mutable.HashMap[String, Long]()
         import scala.collection.JavaConversions._
         for (row: Row <- list) {
           row.mkString(",")
           map += row.getAs[String]("DA") -> row.getAs[Int]("channel_id")
         }
         map.keySet.foreach(println(_))
         map.keySet.filterNot(_.length == 4).foreach(println(_))
         map.keySet.map(x => {
           x.substring(0, 3)
         }).foreach(println(_))*/
  }

  def tesTableIsCached(dataFrame: DataFrame): Unit = {
    val empty = sqlContext.emptyDataFrame
    println("===" + empty.rdd.isEmpty())
  }

  def testDataFrameAlis(dataFrame: DataFrame, dataFrame2: DataFrame): Unit = {
    dataFrame.alias("a").join(dataFrame2.alias("b"), "DA")
      .selectExpr("Da", "b.program_id").show()
  }

//  def union(): Unit = {
//    val log = new ListBuffer[CacheLog]()
//    log.append(CacheLog("12", "1", false))
//    log.append(CacheLog("12", "1", true))
//    log.append(CacheLog("13", "1", false))
//    log.append(CacheLog("15", "1", true))
//    val logDF = sparkSession.sqlContext.createDataFrame(log)
//
//
//    val cache = new ListBuffer[CacheLog]()
//    cache.append(CacheLog("12", "1", true))
//    cache.append(CacheLog("13", "1", true))
//    cache.append(CacheLog("14", "1", true))
//    /*
//    12 1 1
//    14 1 1
//    15 1 1
//     */
//    val cacheDF = sparkSession.sqlContext.createDataFrame(cache)
//
//    val unionDF = logDF.unionAll(cacheDF)
//    val dropDuplicatesDF = unionDF.dropDuplicates(Seq("uid", "play_type", "status"))
//    //    unionDF.selectExpr("uid as u_1","play_type","status").show()
//    //    dropDuplicatesDF.selectExpr("uid as u_2","play_type","status").show()
//
//    logDF.registerTempTable("t_log")
//    cacheDF.registerTempTable("t_cache")
//    val dis = sqlContext.sql(
//      """
//        |select nvl(t1.uid,t2.uid) uid,
//        |nvl(t1.play_type,t2.play_type) play_type,
//        |nvl(t1.status,true) log_status ,
//        |nvl(t2.status,true) cache_status
//        |from t_log t1
//        |full join t_cache t2
//        |on t1.uid=t2.uid and t1.play_type=t2.play_type
//      """.stripMargin)
//    dis.show()
//    dis.selectExpr("uid", "play_type", " (log_status and cache_status)  as status")
//      .show()
//
//
//  }
//
//  def except(df1: DataFrame, df2: DataFrame): Unit = {
//    df1.except(df2).show()
//  }
//
//  def testDataFrameIsEmpty(df: DataFrame) = {
//    if (df.rdd.isEmpty()) {
//      println("isEmpty")
//    } else {
//      println(" not Empty")
//    }
//    df.show()
//  }
//
//  def testDataFrameToJson(df: DataFrame) = {
//    //    UserStatus
//    //      case class UserStatus(DA: Int, channel_id: Long, program_id: Int, end_time: Long)
//    val data12 = df.map(f => {
//      f.mkString(",")
//    }).collectAsync().get().zipWithIndex.toMap
//
//    data12.foreach(println(_))
//
//
//    val data13 = df.map(f => {
//      f.mkString(",")
//    }).collect().zipWithIndex.toMap
//    data13.foreach(println(_))
//  }
//
//  def dropDuplicates(dataFrame: DataFrame): Unit = {
//    dataFrame.show()
//    dataFrame.dropDuplicates(Seq("da", "channel_id"))
//      .show(20)
//  }
//
//  def devieTypeToNum(devieType: String): String = {
//    devieType.toLowerCase match {
//      case "机顶盒" => "1"
//      case "stb" => "1"
//      case "smartcard" => "2"
//      case "mobile" => "3"
//      case "pad" => "4"
//      case "pc" => "5"
//      case _ => "0"
//    }
//  }
//
//  def selectExpr(df: DataFrame) = {
//    df.selectExpr(s"${getUniuxTime()} as time", "DA as user_id", "channel_id", "program_id").show()
//  }
//
//  /**
//    * 获取Unix时间
//    */
//  def getUniuxTime(): Long = {
//    val dateTime: DateTime = new DateTime()
//    dateTime.getMillis / 1000
//  }
//
//  //  df1.withColumn("channel_id22", sqlfunc($"channel_id",lit(1),lit(3))).show()
//
//  import org.apache.spark.sql.functions._
//
//
//  def testUdf(): Unit = {
//    sqlContext.udf.register("len", len _)
//    val sqlfunc = udf(fun)
//    val longLength = udf((bookTitle: String, length: Int) => bookTitle.length < length)
//  }
//
//  val fun: ((String, Int, Int) => String) = (args: String, k1: Int, k2: Int) => {
//    args.substring(k1, k2)
//  }

  def len(bookTitle: String): Int = bookTitle.length

  def create_1(df: DataFrame, df2: DataFrame) = {
    df.registerTempTable("t1")
    df2.registerTempTable("t2")
    val sql =
      """
        |select a.da,a.channel_id
        |from t1 a
        |left join t2 b
        |on a.da = b.da
        |where  b.da is null
      """.stripMargin
    //   sqlContext.sql(sql).show()

    val sql2 =
      """select b.da,b.channel_id from  t2  b
        | where
        | (select count(1) as num from t1 a where a.da = b.da ) = 0""".stripMargin
    sqlContext.sql(sql2).show()

    //    df.foreach(x=>{
    //      println(x.getInt(0))
    ////      println(x.getInt(1))
    //
    //    })


    //    df.show()
    //    df.distinct().show()
    //    df.dropDuplicates(Seq("DA")).show()

    //    df.registerTempTable("t_1")
    //   sqlContext.sql("select coalesce(da,'a',da) da , program_id,1 as click  from t_1").registerTempTable("t_2")
    //    sqlContext.sql(" select da ,sum(click) from t_2 group by program_id,da").show()
  }

  //  val booksWithLongTitle = df1.filter(longLength($"DA", $"10"))
  //  booksWithLongTitle.show()
  df1.registerTempTable("t_df1")
  //  sqlContext.sql("select len(DA) from t_df1").show()

  def create_2(df: DataFrame) = {

    df.registerTempTable("t_1")
    sqlContext.sql("select * from t_1").show(2)
  }

//  sqlContext.udf.register("deviceNum", devieTypeToNum _)

  //  sqlContext.sql("select deviceNum(DA) from t_df1").show()

  case class UserStatus(DA: Int, channel_id: String, program_id: Int, end_time: Long)

  case class UserStatus2(DA: Int, channel_id: String, program_id: Int, end_time2: Long)

  case class CacheLog(uid: String, play_type: String, status: Boolean)

  case class UserStatus3(DA: String, channel_id: Int, end_time: Long, program_id: String)

  case class demo2(s1: String, s2: String)


}
