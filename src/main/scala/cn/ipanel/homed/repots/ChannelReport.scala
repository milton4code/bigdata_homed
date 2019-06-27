package cn.ipanel.homed.repots

import cn.ipanel.common.{CluserProperties, DBProperties, SparkSession}
import cn.ipanel.utils.{DBUtils, DateUtils, MOEDateUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
  * 频道人群特征及频道topN节目
  */
object ChannelReport {

  def main(args: Array[String]): Unit = {
    var day = DateUtils.getYesterday()
    var topN = 10
    if (args.size == 2) {
      day = args(0)
      topN = args(1).toInt
    } else {
      println("请输入 日期和节目topN个数 如20190301 10")
      System.exit(-1)
    }

    val sparkSession = SparkSession("ChannelReport", CluserProperties.SPARK_MASTER_URL)
    val sparkContext = sparkSession.sparkContext
    val hiveContext = sparkSession.sqlContext

    val dataFrame = getData(hiveContext, day)
    dealData(hiveContext, sparkContext, dataFrame, day, topN)

  }

  case class ChannelBaseData(f_date: String, f_user_id: String, f_terminal: String, f_region_id: String, f_channel_id: String, f_start_time: String, f_hour: String, f_time_range: String, f_play_time: Long, f_play_count: Integer)

  def getData(hiveContext: SQLContext, day: String): DataFrame = {
    import hiveContext.implicits._
    val df = hiveContext.sql(
      s"""
         |select * from
         |bigdata.orc_video_play
         |where day='$day' and playtype='live' and serviceid is not null and playtime > 0
      """.stripMargin)
    val final_df = df.map(x => {
      val listBuffer = new ListBuffer[(ChannelBaseData)]
      val userid = x.getAs[String]("userid")
      val devicetype = x.getAs[String]("devicetype")
      val regionid = x.getAs[String]("regionid")
      val starttime = x.getAs[String]("starttime")
      val channelid = x.getAs[Long]("serviceid").toString
      val playtime = x.getAs[Long]("playtime")
      val list = process(starttime, playtime)
      for (e <- list) {
        listBuffer += ChannelBaseData(day, userid, devicetype, regionid, channelid, starttime, e._1.toString, e._2.toString, e._5, e._6)
      }
      listBuffer.toList
    }).flatMap(x => x).toDF()
    final_df
  }

  def dealData(hiveContext: SQLContext, sparkContext: SparkContext, df: DataFrame, day: String, topN: Int): Unit = {

    getChannelEventData(hiveContext).registerTempTable("channelAndEventData")
    df.registerTempTable("df_table")
    val baseDF = hiveContext.sql(
      """
        |select f_date,f_user_id,cast(f_terminal as int) as f_terminal,f_region_id,f_hour,f_time_range,f_play_time,f_play_count,f_channel_id,channel_name as f_channel_name,
        |program_id as f_program_id ,program_name as f_program_name,f_relevance_id,start_time as f_program_start_time
        |from channelAndEventData b join df_table a  on a.f_channel_id=b.channel_id and a.f_start_time>=b.start_time and a.f_start_time<=b.end_time
      """.stripMargin)

    val adressDF = getAdressInfo(hiveContext)

    val df_u = adressDF.join(baseDF, "f_region_id")
    df_u.persist(StorageLevel.MEMORY_AND_DISK)
    groupbyHalfHour(df_u, hiveContext, "t_channel_report_by_halfhour", topN)
    println("halfhour 统计完成")
    val changName_udf = udf((program_name: String) => {
      val program_name_new = program_name.replaceAll("(\\(.*\\))", "")
      program_name_new
    })
    //基础数据信息（天）
    //    df_u.sample(false,0.002).show(1000,false)
    val base_df = df_u.groupBy("f_date", "f_user_id", "f_terminal", "f_region_id", "f_city_id", "f_province_id", "f_region_name", "f_city_name", "f_province_name", "f_channel_id", "f_channel_name", "f_program_id", "f_program_name", "f_relevance_id", "f_program_start_time")
      .agg("f_play_time" -> "sum").withColumnRenamed("sum(f_play_time)", "f_user_program_play_time").repartition(40)
    //      .withColumn("f_program_name",changName_udf(col("f_program_name")))
    //    base_df.persist(StorageLevel.MEMORY_AND_DISK)
    //    base_df.show(false)
    DBUtils.saveDataFrameToPhoenixNew(base_df, "t_chanel_data_base")
    df_u.unpersist()
    //频道统计==start==========================================
    val year = MOEDateUtils.getYear(day).toString
    //    base_df.unpersist()
    val weekStartTime = MOEDateUtils.getFirstOfWeek(day)
    val monthStartTime = MOEDateUtils.getFirstDayOfMonth(year.toInt, day)
    val latestDay = DateUtils.getNDaysBefore(6, day)
    val arr = Array(weekStartTime, monthStartTime, latestDay, day).sorted
    val smallDay = arr(0)
    val sql=
      s"""
        |(select * from t_chanel_data_base
        |where f_date>='$smallDay' and f_date<='$day')
        |as abc
      """.stripMargin
    val base = DBUtils.loadDataFromPhoenix2(hiveContext, sql)
    //      .persist(StorageLevel.MEMORY_AND_DISK)
    base.persist(StorageLevel.DISK_ONLY)
    groupByCategory_NEW(day, "day", base, hiveContext, sparkContext, "t_channel_report_by_day", year, topN)
    println("day 统计完成")
    groupByCategory_NEW(day, "week", base, hiveContext, sparkContext, "t_channel_report_by_week", year, topN)
    println("week 统计完成")
    groupBylatestDay_new(day, base, hiveContext, sparkContext, "t_channel_report_by_latest", topN)
    println("latest 统计完成")
    groupByCategory_NEW(day, "month", base, hiveContext, sparkContext, "t_channel_report_by_month", year, topN)
    println("month 统计完成")
    base.persist()
  }

  def groupByCategory_NEW(date: String, types: String, base_df: DataFrame, hiveContext: SQLContext, sparkContext: SparkContext, tableName: String, year: String, topN: Int): Unit = {
    var starttime = ""
    var endtime = ""
    var f_date = ""
    val tmep_table = types + "_table"
    if (types.equals("day")) {
      f_date = date
    } else if (types.equals("week")) {
      starttime = MOEDateUtils.getFirstOfWeek(date)
      endtime = MOEDateUtils.getLastOfWeek(date)
      f_date = starttime
    } else if (types.equals("month")) {
      starttime = MOEDateUtils.getFirstDayOfMonth(year.toInt, date)
      endtime = MOEDateUtils.getLastDayOfMonth(year.toInt, date)
      f_date = starttime.substring(0, 6)
    } else if (types.equals("year")) {
      starttime = MOEDateUtils.getFirstDayOfYear(year.toInt)
      endtime = MOEDateUtils.getLastDayOfYear(year.toInt)
      f_date = year
    }
    var timegrap = 0L
    //    var userCntDF = hiveContext.emptyDataFrame
    //    var df = hiveContext.emptyDataFrame
    if (types.equals("day")) {
      base_df.filter(col("f_date") === date).registerTempTable(s"$tmep_table")
      //      userCntDF = getUserCnt(hiveContext, base_df, false).cache()
      timegrap = 1
    } else {
      base_df.filter(col("f_date") >= starttime and col("f_date") <= endtime)
        .registerTempTable(s"$tmep_table")
      //      timegrap = df.select("f_date").distinct().count()
      //      userCntDF = getUserCnt(hiveContext, df, true).cache()
      timegrap = (DateUtils.dateToUnixtime(date, "yyyyMMdd") - DateUtils.dateToUnixtime(starttime, "yyyyMMdd")) / (24 * 3600) + 1
    }
    //    hiveContext.cacheTable(s"$tmep_table")
    //按region 统计
    val df_0_region = hiveContext.sql(
      s"""
         |select f_terminal,f_region_id,f_city_id,f_province_id,
         |count(distinct f_user_id) as f_uv
         |from $tmep_table
         |group by f_terminal,f_region_id,f_city_id,f_province_id
      """.stripMargin)
    df_0_region.cache()
    val df_1_region = hiveContext.sql(
      s"""
         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name,
         |sum(if((f_user_channel_play_time/$timegrap)<600,1,0)) as f_browse_user_count,
         |sum(if((f_user_channel_play_time/$timegrap)>=600 and (f_user_channel_play_time/$timegrap) < 3600,1,0)) as f_hobby_user_count,
         |sum(if((f_user_channel_play_time/$timegrap)>=3600 ,1,0)) as f_steady_user_count
         |from
         |(
         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name,f_user_id,
         |sum(f_user_program_play_time) as f_user_channel_play_time
         |from $tmep_table
         |group by f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name,f_user_id
         |)t
         |group by f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name
      """.stripMargin)
    df_1_region.cache()
    //    val df_1_region = hiveContext.sql(
    //      s"""
    //         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name,
    //         |concat('1:',f_browse_user_count,',','2:',f_hobby_user_count,',','3:',f_steady_user_count) as f_user_type
    //         |from
    //         |(
    //         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name,
    //         |sum(if((f_user_channel_play_time/$timegrap)<600,1,0)) as f_browse_user_count,
    //         |sum(if((f_user_channel_play_time/$timegrap)>=600 and (f_user_channel_play_time/$timegrap) < 3600,1,0)) as f_hobby_user_count,
    //         |sum(if((f_user_channel_play_time/$timegrap)>=3600 ,1,0)) as f_steady_user_count
    //         |from
    //         |(
    //         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name,f_user_id,
    //         |sum(f_user_program_play_time) as f_user_channel_play_time
    //         |from $tmep_table
    //         |group by f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name,f_user_id
    //         |)t
    //         |group by f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name
    //         |)tt
    //      """.stripMargin)

    val df_2_region = hiveContext.sql(
      s"""
         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,
         |sum(f_user_program_play_time) as f_program_play_time,f_program_start_time
         |from $tmep_table
         |group by f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,f_program_start_time
      """.stripMargin)
    df_2_region.cache()
    df_0_region.join(df_2_region, Seq("f_terminal", "f_region_id", "f_city_id", "f_province_id"))
      .registerTempTable("category_table_region")

    //    userCntDF.join(df_2_region, userCntDF.col("terminal") === df_2_region.col("f_terminal")
    //      && userCntDF.col("f_code") === df_2_region.col("f_region_id")).drop("f_code").drop("terminal")
    //      .registerTempTable("category_table_region")

    val df_3_region = hiveContext.sql(
      s"""
         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,concat_ws(',',f_rating_arr) as f_rating_arr
         |from
         |(
         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,collect_set(concat_ws('&',f_relevance_id,f_program_id,f_program_name,rating,f_program_start_time)) as f_rating_arr
         |from
         |(
         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,rating,f_relevance_id,f_program_start_time
         |from
         |(
         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,cast(rating as string) as rating,f_relevance_id,f_program_start_time,
         |dense_rank() over(partition by f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name order by rating desc ) as f_rank
         |from
         |(
         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,round(f_program_play_time/(f_uv*$timegrap*24*60*60),8) as rating,f_relevance_id,f_program_start_time
         |from category_table_region
         |)t
         |where rating>0
         |)tt
         |where f_rank<=$topN
         |)ttt
         |group by f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name
         |)tttt
      """.stripMargin)

    val df_4_region = df_1_region.join(df_3_region, Seq("f_terminal", "f_region_id", "f_city_id", "f_province_id", "f_region_name", "f_city_name", "f_province_name", "f_channel_id", "f_channel_name"))
      .selectExpr(s"cast($f_date as string) as f_date", "f_terminal", "f_region_id", "f_city_id", "f_province_id", "f_region_name", "f_city_name", "f_province_name",
        "f_channel_id", "f_channel_name", "f_rating_arr", "concat('1:',f_browse_user_count,',','2:',f_hobby_user_count,',','3:',f_steady_user_count) as f_user_type")
    //    df_4_region.show(false)

    //按city 统计
    //    val df_0_city = hiveContext.sql(
    //      s"""
    //         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
    //         |count(distinct f_user_id) as f_uv
    //         |from $tmep_table
    //         |group by f_terminal,f_city_id,f_province_id,f_city_name,f_province_name
    //      """.stripMargin)
    val df_0_city = df_0_region.groupBy("f_terminal", "f_city_id", "f_province_id").sum("f_uv")
      .withColumnRenamed("sum(f_uv)", "f_uv")

    val df_1_city = df_1_region.groupBy("f_terminal", "f_city_id", "f_province_id", "f_channel_id")
      .sum("f_browse_user_count", "f_hobby_user_count", "f_steady_user_count")
      .withColumnRenamed("sum(f_browse_user_count)", "f_browse_user_count")
      .withColumnRenamed("sum(f_hobby_user_count)", "f_hobby_user_count")
      .withColumnRenamed("sum(f_steady_user_count)", "f_steady_user_count")
    //    val df_1_city = hiveContext.sql(
    //      s"""
    //         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,f_channel_id,f_channel_name,
    //         |concat('1:',f_browse_user_count,',','2:',f_hobby_user_count,',','3:',f_steady_user_count) as f_user_type
    //         |from
    //         |(
    //         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,f_channel_id,f_channel_name,
    //         |sum(if((f_user_channel_play_time/$timegrap)<600,1,0)) as f_browse_user_count,
    //         |sum(if((f_user_channel_play_time/$timegrap)>=600 and (f_user_channel_play_time/$timegrap) < 3600,1,0)) as f_hobby_user_count,
    //         |sum(if((f_user_channel_play_time/$timegrap)>=3600 ,1,0)) as f_steady_user_count
    //         |from
    //         |(
    //         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,f_channel_id,f_channel_name,f_user_id,
    //         |sum(f_user_program_play_time) as f_user_channel_play_time
    //         |from $tmep_table
    //         |group by f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,f_channel_id,f_channel_name,f_user_id
    //         |)t
    //         |group by f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,f_channel_id,f_channel_name
    //         |)tt
    //      """.stripMargin)

    //    val df_2_city = hiveContext.sql(
    //      s"""
    //         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
    //         |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,
    //         |sum(f_user_program_play_time) as f_program_play_time,f_program_start_time
    //         |from $tmep_table
    //         |group by f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
    //         |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,f_program_start_time
    //      """.stripMargin)
    val df_2_city = df_2_region.groupBy("f_terminal", "f_city_id", "f_province_id", "f_city_name", "f_province_name",
      "f_channel_id", "f_channel_name", "f_program_id", "f_program_name", "f_relevance_id", "f_program_start_time").sum("f_program_play_time")
      .withColumnRenamed("sum(f_program_play_time)", "f_program_play_time")

    df_0_city.join(df_2_city, Seq("f_terminal", "f_city_id", "f_province_id"))
      .registerTempTable("category_table_city")

    //    userCntDF.join(df_2_city, userCntDF.col("terminal") === df_2_city.col("f_terminal")
    //      && userCntDF.col("f_code") === df_2_city.col("f_city_id")).drop("f_code").drop("terminal")
    //      .registerTempTable("category_table_city")

    val df_3_city = hiveContext.sql(
      s"""
         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,concat_ws(',',f_rating_arr) as f_rating_arr
         |from
         |(
         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,collect_set(concat_ws('&',f_relevance_id,f_program_id,f_program_name,rating,f_program_start_time)) as f_rating_arr
         |from
         |(
         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,rating,f_relevance_id,f_program_start_time
         |from
         |(
         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,cast(rating as string) as rating,f_relevance_id,f_program_start_time,
         |dense_rank() over(partition by f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name order by rating desc ) as f_rank
         |from
         |(
         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,round(f_program_play_time/(f_uv*$timegrap*24*60*60),8) as rating,f_relevance_id,f_program_start_time
         |from category_table_city
         |)t
         |where rating>0
         |)tt
         |where f_rank<=$topN
         |)ttt
         |group by f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name
         |)tttt
      """.stripMargin)

    val df_4_city = df_1_city.join(df_3_city, Seq("f_terminal", "f_city_id", "f_province_id", "f_channel_id"))
      .selectExpr(s"cast($f_date as string) as f_date", "f_terminal", "'-1' as f_region_id", "f_city_id", "f_province_id", "'-1' as f_region_name", "f_city_name", "f_province_name",
        "f_channel_id", "f_channel_name", "f_rating_arr", "concat('1:',f_browse_user_count,',','2:',f_hobby_user_count,',','3:',f_steady_user_count) as f_user_type")
    //    df_4_city.show(false)
    //按province 统计
    //    val df_0_province = hiveContext.sql(
    //      s"""
    //         |select f_terminal,f_province_id,f_province_name,
    //         |count(distinct f_user_id) as f_uv
    //         |from $tmep_table
    //         |group by f_terminal,f_province_id,f_province_name
    //      """.stripMargin)
    val df_0_province = df_0_region.groupBy("f_terminal", "f_province_id").sum("f_uv")
      .withColumnRenamed("sum(f_uv)", "f_uv")
    df_0_region.unpersist()
    val df_1_province = df_1_region.groupBy("f_terminal", "f_province_id", "f_channel_id")
      .sum("f_browse_user_count", "f_hobby_user_count", "f_steady_user_count")
      .withColumnRenamed("sum(f_browse_user_count)", "f_browse_user_count")
      .withColumnRenamed("sum(f_hobby_user_count)", "f_hobby_user_count")
      .withColumnRenamed("sum(f_steady_user_count)", "f_steady_user_count")
    df_1_region.unpersist()
    //    val df_1_province = hiveContext.sql(
    //      s"""
    //         |select f_terminal,f_province_id,f_province_name,f_channel_id,f_channel_name,
    //         |concat('1:',f_browse_user_count,',','2:',f_hobby_user_count,',','3:',f_steady_user_count) as f_user_type
    //         |from
    //         |(
    //         |select f_terminal,f_province_id,f_province_name,f_channel_id,f_channel_name,
    //         |sum(if((f_user_channel_play_time/$timegrap)<600,1,0)) as f_browse_user_count,
    //         |sum(if((f_user_channel_play_time/$timegrap)>=600 and (f_user_channel_play_time/$timegrap) < 3600,1,0)) as f_hobby_user_count,
    //         |sum(if((f_user_channel_play_time/$timegrap)>=3600 ,1,0)) as f_steady_user_count
    //         |from
    //         |(
    //         |select f_terminal,f_province_id,f_province_name,f_channel_id,f_channel_name,f_user_id,
    //         |sum(f_user_program_play_time) as f_user_channel_play_time
    //         |from $tmep_table
    //         |group by f_terminal,f_province_id,f_province_name,f_channel_id,f_channel_name,f_user_id
    //         |)t
    //         |group by f_terminal,f_province_id,f_province_name,f_channel_id,f_channel_name
    //         |)tt
    //      """.stripMargin)

    //    val df_2_province = hiveContext.sql(
    //      s"""
    //         |select f_terminal,f_province_id,f_province_name,
    //         |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,
    //         |sum(f_user_program_play_time) as f_program_play_time,f_program_start_time
    //         |from $tmep_table
    //         |group by f_terminal,f_province_id,f_province_name,
    //         |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,f_program_start_time
    //      """.stripMargin)
    val df_2_province = df_2_region.groupBy("f_terminal", "f_province_id", "f_province_name",
      "f_channel_id", "f_channel_name", "f_program_id", "f_program_name", "f_relevance_id", "f_program_start_time").sum("f_program_play_time")
      .withColumnRenamed("sum(f_program_play_time)", "f_program_play_time")
    df_2_region.unpersist()
    df_0_province.join(df_2_province, Seq("f_terminal", "f_province_id"))
      .registerTempTable("category_table_province")

    //    userCntDF.join(df_2_province, userCntDF.col("terminal") === df_2_province.col("f_terminal")
    //      && userCntDF.col("f_code") === df_2_province.col("f_province_id")).drop("f_code").drop("terminal")
    //      .registerTempTable("category_table_province")
    //    userCntDF.unpersist()

    val df_3_province = hiveContext.sql(
      s"""
         |select f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name,concat_ws(',',f_rating_arr) as f_rating_arr
         |from
         |(
         |select f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name,collect_set(concat_ws('&',f_relevance_id,f_program_id,f_program_name,rating,f_program_start_time)) as f_rating_arr
         |from
         |(
         |select f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,rating,f_relevance_id,f_program_start_time
         |from
         |(
         |select f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,cast(rating as string) as rating,f_relevance_id,f_program_start_time,
         |dense_rank() over(partition by f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name order by rating desc ) as f_rank
         |from
         |(
         |select f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,round(f_program_play_time/(f_uv*$timegrap*24*60*60),8) as rating,f_relevance_id,f_program_start_time
         |from category_table_province
         |)t
         |where rating>0
         |)tt
         |where f_rank<=$topN
         |)ttt
         |group by f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name
         |)tttt
      """.stripMargin)

    val df_4_province = df_1_province.join(df_3_province, Seq("f_terminal", "f_province_id", "f_channel_id"))
      .selectExpr(s"cast($f_date as string) as f_date", "f_terminal", "'-1' as f_region_id", "'-1' as f_city_id", "f_province_id", "'-1' as f_region_name", "'-1' as f_city_name", "f_province_name",
        "f_channel_id", "f_channel_name", "f_rating_arr", "concat('1:',f_browse_user_count,',','2:',f_hobby_user_count,',','3:',f_steady_user_count) as f_user_type")
    //    df_4_province.show(false)
    val final_df = df_4_region.unionAll(df_4_city).unionAll(df_4_province)
    //    hiveContext.uncacheTable(s"$tmep_table")
    DBUtils.saveDataFrameToPhoenixNew(final_df, tableName)
    //    df.unpersist()
  }

  def groupBylatestDay_new(day: String, base_df: DataFrame, hiveContext: SQLContext, arkContext: SparkContext, tableName: String, topN: Int): Unit = {
    val latestDay = DateUtils.getNDaysBefore(6, day)
    var timegrap = 7L
    val tmep_table = "latest_table"
    val df = base_df.filter(col("f_date") >= latestDay and col("f_date") <= day)
    //    val userCntDF = getUserCnt(hiveContext, df, true).cache()
    df.registerTempTable(s"$tmep_table")
    //    hiveContext.cacheTable(s"$tmep_table")
    timegrap = df.select("f_date").distinct().count()
    //    println("latest:" + timegrap)
    //按region 统计
    val df_0_region = hiveContext.sql(
      s"""
         |select f_terminal,f_region_id,f_city_id,f_province_id,
         |count(distinct f_user_id) as f_uv
         |from $tmep_table
         |group by f_terminal,f_region_id,f_city_id,f_province_id
      """.stripMargin)
    df_0_region.cache()
    val df_1_region = hiveContext.sql(
      s"""
         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name,
         |sum(if((f_user_channel_play_time/$timegrap)<600,1,0)) as f_browse_user_count,
         |sum(if((f_user_channel_play_time/$timegrap)>=600 and (f_user_channel_play_time/$timegrap) < 3600,1,0)) as f_hobby_user_count,
         |sum(if((f_user_channel_play_time/$timegrap)>=3600 ,1,0)) as f_steady_user_count
         |from
         |(
         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name,f_user_id,
         |sum(f_user_program_play_time) as f_user_channel_play_time
         |from $tmep_table
         |group by f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name,f_user_id
         |)t
         |group by f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name
      """.stripMargin)
    df_1_region.cache()
    //    val df_1_region = hiveContext.sql(
    //      s"""
    //         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name,
    //         |concat('1:',f_browse_user_count,',','2:',f_hobby_user_count,',','3:',f_steady_user_count) as f_user_type
    //         |from
    //         |(
    //         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name,
    //         |sum(if((f_user_channel_play_time/$timegrap)<600,1,0)) as f_browse_user_count,
    //         |sum(if((f_user_channel_play_time/$timegrap)>=600 and (f_user_channel_play_time/$timegrap) < 3600,1,0)) as f_hobby_user_count,
    //         |sum(if((f_user_channel_play_time/$timegrap)>=3600 ,1,0)) as f_steady_user_count
    //         |from
    //         |(
    //         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name,f_user_id,
    //         |sum(f_user_program_play_time) as f_user_channel_play_time
    //         |from $tmep_table
    //         |group by f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name,f_user_id
    //         |)t
    //         |group by f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_channel_id,f_channel_name
    //         |)tt
    //      """.stripMargin)

    val df_2_region = hiveContext.sql(
      s"""
         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,
         |sum(f_user_program_play_time) as f_program_play_time,f_program_start_time
         |from $tmep_table
         |group by f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,f_program_start_time
      """.stripMargin)
    df_2_region.cache()
    df_0_region.join(df_2_region, Seq("f_terminal", "f_region_id", "f_city_id", "f_province_id"))
      .registerTempTable("category_table_region")
    //    userCntDF.join(df_2_region, userCntDF.col("f_code") === df_2_region.col("f_region_id") &&
    //      userCntDF.col("terminal") === df_2_region.col("f_terminal")).drop("terminal").drop("f_code")
    //      .registerTempTable("category_table_region")

    val df_3_region = hiveContext.sql(
      s"""
         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,concat_ws(',',f_rating_arr) as f_rating_arr
         |from
         |(
         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,collect_set(concat_ws('&',f_relevance_id,f_program_id,f_program_name,rating,f_program_start_time)) as f_rating_arr
         |from
         |(
         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,rating,f_relevance_id,f_program_start_time
         |from
         |(
         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,cast(rating as string) as rating,f_relevance_id,f_program_start_time,
         |dense_rank() over(partition by f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name order by rating desc ) as f_rank
         |from
         |(
         |select f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,round(f_program_play_time/(f_uv*$timegrap*24*60*60),8) as rating,f_relevance_id,f_program_start_time
         |from category_table_region
         |)t
         |where rating>0
         |)tt
         |where f_rank<=$topN
         |)ttt
         |group by f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name
         |)tttt
      """.stripMargin)

    val df_4_region = df_1_region.join(df_3_region, Seq("f_terminal", "f_region_id", "f_city_id", "f_province_id", "f_region_name", "f_city_name", "f_province_name", "f_channel_id", "f_channel_name"))
      .selectExpr(s"cast($latestDay as string) as f_start_date", s"cast($day as string) as f_end_date", "f_terminal", "f_region_id", "f_city_id", "f_province_id", "f_region_name", "f_city_name", "f_province_name",
        "f_channel_id", "f_channel_name", "f_rating_arr", "concat('1:',f_browse_user_count,',','2:',f_hobby_user_count,',','3:',f_steady_user_count) as f_user_type")
    //    df_4_region.printSchema()

    //按city 统计
    //    val df_0_city = hiveContext.sql(
    //      s"""
    //         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
    //         |count(distinct f_user_id) as f_uv
    //         |from $tmep_table
    //         |group by f_terminal,f_city_id,f_province_id,f_city_name,f_province_name
    //      """.stripMargin)
    val df_0_city = df_0_region.groupBy("f_terminal", "f_city_id", "f_province_id").sum("f_uv")
      .withColumnRenamed("sum(f_uv)", "f_uv")
    val df_1_city = df_1_region.groupBy("f_terminal", "f_city_id", "f_province_id", "f_channel_id")
      .sum("f_browse_user_count", "f_hobby_user_count", "f_steady_user_count")
      .withColumnRenamed("sum(f_browse_user_count)", "f_browse_user_count")
      .withColumnRenamed("sum(f_hobby_user_count)", "f_hobby_user_count")
      .withColumnRenamed("sum(f_steady_user_count)", "f_steady_user_count")

    //    val df_1_city = hiveContext.sql(
    //      s"""
    //         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,f_channel_id,f_channel_name,
    //         |concat('1:',f_browse_user_count,',','2:',f_hobby_user_count,',','3:',f_steady_user_count) as f_user_type
    //         |from
    //         |(
    //         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,f_channel_id,f_channel_name,
    //         |sum(if((f_user_channel_play_time/$timegrap)<600,1,0)) as f_browse_user_count,
    //         |sum(if((f_user_channel_play_time/$timegrap)>=600 and (f_user_channel_play_time/$timegrap) < 3600,1,0)) as f_hobby_user_count,
    //         |sum(if((f_user_channel_play_time/$timegrap)>=3600 ,1,0)) as f_steady_user_count
    //         |from
    //         |(
    //         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,f_channel_id,f_channel_name,f_user_id,
    //         |sum(f_user_program_play_time) as f_user_channel_play_time
    //         |from $tmep_table
    //         |group by f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,f_channel_id,f_channel_name,f_user_id
    //         |)t
    //         |group by f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,f_channel_id,f_channel_name
    //         |)tt
    //      """.stripMargin)

    //    val df_2_city = hiveContext.sql(
    //      s"""
    //         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
    //         |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,
    //         |sum(f_user_program_play_time) as f_program_play_time,f_program_start_time
    //         |from $tmep_table
    //         |group by f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
    //         |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,f_program_start_time
    //      """.stripMargin)
    val df_2_city = df_2_region.groupBy("f_terminal", "f_city_id", "f_province_id", "f_city_name", "f_province_name",
      "f_channel_id", "f_channel_name", "f_program_id", "f_program_name", "f_relevance_id", "f_program_start_time").sum("f_program_play_time")
      .withColumnRenamed("sum(f_program_play_time)", "f_program_play_time")
    df_0_city.join(df_2_city, Seq("f_terminal", "f_city_id", "f_province_id"))
      .registerTempTable("category_table_city")
    //    userCntDF.join(df_2_city, userCntDF.col("f_code") === df_2_city.col("f_city_id") &&
    //      userCntDF.col("terminal") === df_2_city.col("f_terminal")).drop("terminal").drop("f_code")
    //      .registerTempTable("category_table_city")

    val df_3_city = hiveContext.sql(
      s"""
         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,concat_ws(',',f_rating_arr) as f_rating_arr
         |from
         |(
         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,collect_set(concat_ws('&',f_relevance_id,f_program_id,f_program_name,rating,f_program_start_time)) as f_rating_arr
         |from
         |(
         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,rating,f_relevance_id,f_program_start_time
         |from
         |(
         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,cast(rating as string) as rating,f_relevance_id,f_program_start_time,
         |dense_rank() over(partition by f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name order by rating desc ) as f_rank
         |from
         |(
         |select f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,round(f_program_play_time/(f_uv*$timegrap*24*60*60),8) as rating,f_relevance_id,f_program_start_time
         |from category_table_city
         |)t
         |where rating>0
         |)tt
         |where f_rank<=$topN
         |)ttt
         |group by f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name
         |)tttt
      """.stripMargin)

    val df_4_city = df_1_city.join(df_3_city, Seq("f_terminal", "f_city_id", "f_province_id", "f_channel_id"))
      .selectExpr(s"cast($latestDay as string) as f_start_date", s"cast($day as string) as f_end_date", "f_terminal", "'-1' as f_region_id", "f_city_id", "f_province_id", "'-1' as f_region_name", "f_city_name", "f_province_name",
        "f_channel_id", "f_channel_name", "f_rating_arr", "concat('1:',f_browse_user_count,',','2:',f_hobby_user_count,',','3:',f_steady_user_count) as f_user_type")
    //    df_4_region.printSchema()
    //    df_4.show(false)
    //按province 统计
    //    val df_0_province = hiveContext.sql(
    //      s"""
    //         |select f_terminal,f_province_id,f_province_name,
    //         |count(distinct f_user_id) as f_uv
    //         |from $tmep_table
    //         |group by f_terminal,f_province_id,f_province_name
    //      """.stripMargin)
    val df_0_province = df_0_region.groupBy("f_terminal", "f_province_id").sum("f_uv")
      .withColumnRenamed("sum(f_uv)", "f_uv")
    val df_1_province = df_1_region.groupBy("f_terminal", "f_province_id", "f_channel_id")
      .sum("f_browse_user_count", "f_hobby_user_count", "f_steady_user_count")
      .withColumnRenamed("sum(f_browse_user_count)", "f_browse_user_count")
      .withColumnRenamed("sum(f_hobby_user_count)", "f_hobby_user_count")
      .withColumnRenamed("sum(f_steady_user_count)", "f_steady_user_count")
    df_1_region.unpersist()
    //    val df_1_province = hiveContext.sql(
    //      s"""
    //         |select f_terminal,f_province_id,f_province_name,f_channel_id,f_channel_name,
    //         |concat('1:',f_browse_user_count,',','2:',f_hobby_user_count,',','3:',f_steady_user_count) as f_user_type
    //         |from
    //         |(
    //         |select f_terminal,f_province_id,f_province_name,f_channel_id,f_channel_name,
    //         |sum(if((f_user_channel_play_time/$timegrap)<600,1,0)) as f_browse_user_count,
    //         |sum(if((f_user_channel_play_time/$timegrap)>=600 and (f_user_channel_play_time/$timegrap) < 3600,1,0)) as f_hobby_user_count,
    //         |sum(if((f_user_channel_play_time/$timegrap)>=3600 ,1,0)) as f_steady_user_count
    //         |from
    //         |(
    //         |select f_terminal,f_province_id,f_province_name,f_channel_id,f_channel_name,f_user_id,
    //         |sum(f_user_program_play_time) as f_user_channel_play_time
    //         |from $tmep_table
    //         |group by f_terminal,f_province_id,f_province_name,f_channel_id,f_channel_name,f_user_id
    //         |)t
    //         |group by f_terminal,f_province_id,f_province_name,f_channel_id,f_channel_name
    //         |)tt
    //      """.stripMargin)

    //    val df_2_province = hiveContext.sql(
    //      s"""
    //         |select f_terminal,f_province_id,f_province_name,
    //         |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,
    //         |sum(f_user_program_play_time) as f_program_play_time,f_program_start_time
    //         |from $tmep_table
    //         |group by f_terminal,f_province_id,f_province_name,
    //         |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,f_program_start_time
    //      """.stripMargin)
    val df_2_province = df_2_region.groupBy("f_terminal", "f_province_id", "f_province_name",
      "f_channel_id", "f_channel_name", "f_program_id", "f_program_name", "f_relevance_id", "f_program_start_time").sum("f_program_play_time")
      .withColumnRenamed("sum(f_program_play_time)", "f_program_play_time")
    df_2_region.unpersist()
    df_0_province.join(df_2_province, Seq("f_terminal", "f_province_id"))
      .registerTempTable("category_table_province")
    //    userCntDF.join(df_2_province, userCntDF.col("f_code") === df_2_province.col("f_province_id") &&
    //      userCntDF.col("terminal") === df_2_province.col("f_terminal")).drop("terminal").drop("f_code")
    //      .registerTempTable("category_table_province")
    df_0_region.unpersist()
    val df_3_province = hiveContext.sql(
      s"""
         |select f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name,concat_ws(',',f_rating_arr) as f_rating_arr
         |from
         |(
         |select f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name,collect_set(concat_ws('&',f_relevance_id,f_program_id,f_program_name,rating,f_program_start_time)) as f_rating_arr
         |from
         |(
         |select f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,rating,f_relevance_id,f_program_start_time
         |from
         |(
         |select f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,cast(rating as string) as rating,f_relevance_id,f_program_start_time,
         |dense_rank() over(partition by f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name order by rating desc ) as f_rank
         |from
         |(
         |select f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,round(f_program_play_time/(f_uv*$timegrap*24*60*60),8) as rating,f_relevance_id,f_program_start_time
         |from category_table_province
         |)t
         |where rating > 0
         |)tt
         |where f_rank<=$topN
         |)ttt
         |group by f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name
         |)tttt
      """.stripMargin)

    val df_4_province = df_1_province.join(df_3_province, Seq("f_terminal", "f_province_id", "f_channel_id"))
      .selectExpr(s"cast($latestDay as string) as f_start_date", s"cast($day as string) as f_end_date", "f_terminal", "'-1' as f_region_id", "'-1' as f_city_id", "f_province_id", "'-1' as f_region_name", "'-1' as f_city_name", "f_province_name",
        "f_channel_id", "f_channel_name", "f_rating_arr", "concat('1:',f_browse_user_count,',','2:',f_hobby_user_count,',','3:',f_steady_user_count) as f_user_type")
    //    hiveContext.uncacheTable(s"$tmep_table")
    val final_df = df_4_province.unionAll(df_4_city).unionAll(df_4_region)
    DBUtils.saveDataFrameToPhoenixNew(final_df, tableName)
    df.unpersist()
  }

  def groupbyHalfHour(df_u: DataFrame, hiveContext: SQLContext, tableName: String, topN: Int) = {
    df_u.registerTempTable("table_halfhour")
    //    val userCntDF = getUserCnt(hiveContext, df_u, false).cache()
    //    hiveContext.cacheTable("table_halfhour")
    //按region 统计
    val df_1_region = hiveContext.sql(
      """
        |select f_date,f_hour,f_time_range,f_terminal,f_region_id,f_city_id,f_province_id,
        |count(distinct f_user_id) as f_uv
        |from table_halfhour
        |group by f_date,f_hour,f_time_range,f_terminal,f_region_id,f_city_id,f_province_id
      """.stripMargin)
    df_1_region.cache()

    val df_2_region = hiveContext.sql(
      """
        |select f_date,f_hour,f_time_range,f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
        |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,
        |sum(f_play_time) as f_program_play_time,f_program_start_time
        |from table_halfhour
        |group by f_date,f_hour,f_time_range,f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
        |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,f_program_start_time
      """.stripMargin)
    df_2_region.cache()
    df_1_region.join(df_2_region, Seq("f_date", "f_hour", "f_time_range", "f_terminal", "f_region_id", "f_city_id", "f_province_id"))
      .registerTempTable("table_halfhour_1_region")

    //    userCntDF.join(df_2_region, userCntDF.col("f_code") === df_2_region.col("f_region_id") &&
    //      userCntDF.col("terminal") === df_2_region.col("f_terminal")).drop("f_code").drop("terminal")
    //      .registerTempTable("table_halfhour_1_region")

    val halfHour_df_region = hiveContext.sql(
      s"""
         |select f_date,f_hour,f_time_range,f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,concat_ws(',',f_rating_arr) as f_rating_arr,f_user_type
         |from
         |(
         |select f_date,f_hour,f_time_range,f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,collect_set(concat_ws('&',f_relevance_id,f_program_id,f_program_name,rating,f_program_start_time)) as f_rating_arr,"" as f_user_type
         |from
         |(
         |select f_date,f_hour,f_time_range,f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,rating,f_relevance_id,f_program_start_time
         |from
         |(
         |select f_date,f_hour,f_time_range,f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,cast(rating as string) as rating ,f_relevance_id,f_program_start_time,
         |dense_rank() over(partition by f_date,f_hour,f_time_range,f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name order by rating desc ) as f_rank
         |from
         |(
         |select f_date,f_hour,f_time_range,f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,round(f_program_play_time/(f_uv*1800),8) as rating,f_relevance_id,f_program_start_time
         |from table_halfhour_1_region
         |)t
         |where rating > 0
         |)tt
         |where f_rank<=$topN
         |)ttt
         |group by f_date,f_hour,f_time_range,f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name
         |)tttt
      """.stripMargin)
    //    halfHour_df_region.printSchema()
    //    halfHour_df_region.printSchema()
    //    halfHour_df_region.show(false)
    //按city 统计
    //    val df_1_city = hiveContext.sql(
    //      """
    //        |select f_date,f_hour,f_time_range,f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
    //        |count(distinct f_user_id) as f_uv
    //        |from table_halfhour
    //        |group by f_date,f_hour,f_time_range,f_terminal,f_city_id,f_province_id,f_city_name,f_province_name
    //      """.stripMargin)

    val df_1_city = df_1_region.groupBy("f_date", "f_hour", "f_time_range", "f_terminal", "f_city_id", "f_province_id").sum("f_uv")
      .withColumnRenamed("sum(f_uv)", "f_uv")

    //    val df_2_city = hiveContext.sql(
    //      """
    //        |select f_date,f_hour,f_time_range,f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
    //        |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,
    //        |sum(f_play_time) as f_program_play_time,f_program_start_time
    //        |from table_halfhour
    //        |group by f_date,f_hour,f_time_range,f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
    //        |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,f_program_start_time
    //      """.stripMargin)
    val df_2_city = df_2_region.groupBy("f_date", "f_hour", "f_time_range", "f_terminal", "f_city_id", "f_province_id", "f_city_name", "f_province_name",
      "f_channel_id", "f_channel_name", "f_program_id", "f_program_name", "f_relevance_id", "f_program_start_time").sum("f_program_play_time")
      .withColumnRenamed("sum(f_program_play_time)", "f_program_play_time")
    df_1_city.join(df_2_city, Seq("f_date", "f_hour", "f_time_range", "f_terminal", "f_city_id", "f_province_id"))
      .registerTempTable("table_halfhour_1_city")

    //    userCntDF.join(df_2_city, userCntDF.col("f_code") === df_2_city.col("f_city_id") &&
    //      userCntDF.col("terminal") === df_2_city.col("f_terminal")).drop("f_code").drop("terminal")
    //      .registerTempTable("table_halfhour_1_city")

    val halfHour_df_city = hiveContext.sql(
      s"""
         |select f_date,f_hour,f_time_range,f_terminal,"-1" as f_region_id,f_city_id,f_province_id,"-1" as f_region_name,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,concat_ws(',',f_rating_arr) as f_rating_arr,f_user_type
         |from
         |(
         |select f_date,f_hour,f_time_range,f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,collect_set(concat_ws('&',f_relevance_id,f_program_id,f_program_name,rating,f_program_start_time)) as f_rating_arr,"" as f_user_type
         |from
         |(
         |select f_date,f_hour,f_time_range,f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,rating,f_relevance_id,f_program_start_time
         |from
         |(
         |select f_date,f_hour,f_time_range,f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,cast(rating as string) as rating ,f_relevance_id,f_program_start_time,
         |dense_rank() over(partition by f_date,f_hour,f_time_range,f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name order by rating desc ) as f_rank
         |from
         |(
         |select f_date,f_hour,f_time_range,f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,round(f_program_play_time/(f_uv*1800),8) as rating,f_relevance_id,f_program_start_time
         |from table_halfhour_1_city
         |)t
         |where rating > 0
         |)tt
         |where f_rank<=$topN
         |)ttt
         |group by f_date,f_hour,f_time_range,f_terminal,f_city_id,f_province_id,f_city_name,f_province_name,
         |f_channel_id,f_channel_name
         |)tttt
      """.stripMargin)

    //按province 统计
    //    val df_1_province = hiveContext.sql(
    //      """
    //        |select f_date,f_hour,f_time_range,f_terminal,f_province_id,f_province_name,
    //        |count(distinct f_user_id) as f_uv
    //        |from table_halfhour
    //        |group by f_date,f_hour,f_time_range,f_terminal,f_province_id,f_province_name
    //      """.stripMargin)

    val df_1_province = df_1_region.groupBy("f_date", "f_hour", "f_time_range", "f_terminal", "f_province_id").sum("f_uv")
      .withColumnRenamed("sum(f_uv)", "f_uv")
    df_1_region.unpersist()
    //    val df_2_province = hiveContext.sql(
    //      """
    //        |select f_date,f_hour,f_time_range,f_terminal,f_province_id,f_province_name,
    //        |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,
    //        |sum(f_play_time) as f_program_play_time,f_program_start_time
    //        |from table_halfhour
    //        |group by f_date,f_hour,f_time_range,f_terminal,f_province_id,f_province_name,
    //        |f_channel_id,f_channel_name,f_program_id,f_program_name,f_relevance_id,f_program_start_time
    //      """.stripMargin)
    val df_2_province = df_2_region.groupBy("f_date", "f_hour", "f_time_range", "f_terminal", "f_province_id", "f_province_name",
      "f_channel_id", "f_channel_name", "f_program_id", "f_program_name", "f_relevance_id", "f_program_start_time").sum("f_program_play_time")
      .withColumnRenamed("sum(f_program_play_time)", "f_program_play_time")
    df_2_region.unpersist()
    df_1_province.join(df_2_province, Seq("f_date", "f_hour", "f_time_range", "f_terminal", "f_province_id"))
      .registerTempTable("table_halfhour_1_province")

    //    userCntDF.join(df_2_province, userCntDF.col("f_code") === df_2_province.col("f_province_id") &&
    //      userCntDF.col("terminal") === df_2_province.col("f_terminal")).drop("f_code").drop("terminal")
    //      .registerTempTable("table_halfhour_1_province")
    //    userCntDF.unpersist()
    val halfHour_df_province = hiveContext.sql(
      s"""
         |select f_date,f_hour,f_time_range,f_terminal,"-1" as f_region_id,"-1" as f_city_id,f_province_id,"-1" as f_region_name,"-1" as f_city_name,f_province_name,
         |f_channel_id,f_channel_name,concat_ws(',',f_rating_arr) as f_rating_arr,f_user_type
         |from
         |(
         |select f_date,f_hour,f_time_range,f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name,collect_set(concat_ws('&',f_relevance_id,f_program_id,f_program_name,rating,f_program_start_time)) as f_rating_arr,"" as f_user_type
         |from
         |(
         |select f_date,f_hour,f_time_range,f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,rating,f_relevance_id,f_program_start_time
         |from
         |(
         |select f_date,f_hour,f_time_range,f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,cast(rating as string) as rating ,f_relevance_id,f_program_start_time,
         |dense_rank() over(partition by f_date,f_hour,f_time_range,f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name order by rating desc ) as f_rank
         |from
         |(
         |select f_date,f_hour,f_time_range,f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name,f_program_id,f_program_name,round(f_program_play_time/(f_uv*1800),8) as rating,f_relevance_id,f_program_start_time
         |from table_halfhour_1_province
         |)t
         |where rating > 0
         |)tt
         |where f_rank<=$topN
         |)ttt
         |group by f_date,f_hour,f_time_range,f_terminal,f_province_id,f_province_name,
         |f_channel_id,f_channel_name
         |)tttt
      """.stripMargin)

    //    halfHour_df.show(false)
    val final_df = halfHour_df_region.unionAll(halfHour_df_city).unionAll(halfHour_df_province)
    //    final_df.printSchema()
    //    final_df.show(false)
    //    hiveContext.uncacheTable("table_halfhour")
    DBUtils.saveDataFrameToPhoenixNew(final_df, tableName)
  }

  def getUserCnt(hiveContext: SQLContext, base_df: DataFrame, isFromPHOENIX: Boolean): DataFrame = {
    val arr0 = Array("F_TERMINAL", "F_REGION_ID", "F_CITY_ID", "F_PROVINCE_ID", "F_REGION_NAME", "F_CITY_NAME", "F_PROVINCE_NAME", "F_USER_ID")
    val arr1 = Array("f_terminal", "f_region_id", "f_city_id", "f_province_id", "f_region_name", "f_city_name", "f_province_name", "f_user_id")
    var arr = Array.empty[String]
    if (isFromPHOENIX) arr = arr0 else arr = arr1
    import hiveContext.implicits._
    val user_cnt = base_df.map(x => {
      val f_terminal = x.getAs[Int](arr(0))
      val f_region_id = x.getAs[String](arr(1))
      val f_city_id = x.getAs[String](arr(2))
      val f_province_id = x.getAs[String](arr(3))
      val f_region_name = x.getAs[String](arr(4))
      val f_city_name = x.getAs[String](arr(5))
      val f_province_name = x.getAs[String](arr(6))
      val f_user_id = x.getAs[String](arr(7))
      val key = f_terminal + "," + f_region_id + "," + f_city_id + "," + f_province_id + "," + f_region_name + "," + f_city_name + "," + f_province_name
      (key, Set(f_user_id))
    }).reduceByKey((a, b) => {
      (a ++ b)
    }).map(x => {
      val key_arr = x._1.split(",")
      val f_terminal = key_arr(0)
      val f_region_id = key_arr(1)
      val f_city_id = key_arr(2)
      val f_province_id = key_arr(3)
      val f_city_name = key_arr(5)
      val f_province_name = key_arr(6)
      val user_region_set = x._2
      val region_user_info = f_terminal + ":" + f_region_id + ":" + user_region_set.size
      val key = f_terminal + "," + f_city_id + "," + f_province_id + "," + f_city_name + "," + f_province_name
      (key, (user_region_set, Set(region_user_info)))
    }).reduceByKey((a, b) => {
      val user_city_set = a._1 ++ b._1
      val user_region_info_set = a._2 ++ b._2
      (user_city_set, user_region_info_set)
    }).map(x => {
      val key_arr = x._1.split(",")
      val f_terminal = key_arr(0)
      val f_city_id = key_arr(1)
      val f_province_id = key_arr(2)
      val f_province_name = key_arr(4)
      val tuples = x._2
      val user_city_set = tuples._1
      val user_region_info_set = tuples._2
      val user_city_info = f_terminal + ":" + f_city_id + ":" + user_city_set.size
      val key = f_terminal + "," + f_province_id + "," + f_province_name
      (key, (user_city_set, Set(user_city_info), user_region_info_set))
    }).reduceByKey((a, b) => {
      val user_province_set = a._1 ++ b._1
      val user_city_info_set = a._2 ++ b._2
      val user_region_info_set = a._3 ++ b._3
      (user_province_set, user_city_info_set, user_region_info_set)
    }).map(x => {
      val key_arr = x._1.split(",")
      val f_terminal = key_arr(0)
      val f_province_id = key_arr(1)
      val tuples = x._2
      val user_province_set = tuples._1
      val user_city_info_set = tuples._2.toArray
      val user_region_info_set = tuples._3.toArray
      val list = new ListBuffer[(Int, String, Long)]
      for (e <- user_city_info_set) {
        val arr = e.split(":")
        val terminal = arr(0).toInt
        val city_id = arr(1)
        val user_cnt = arr(2).toLong
        list.append((terminal, city_id, user_cnt))
      }
      for (e <- user_region_info_set) {
        val arr = e.split(":")
        val terminal = arr(0).toInt
        val region_id = arr(1)
        val user_cnt = arr(2).toLong
        list.append((terminal, region_id, user_cnt))
      }
      list.append((f_terminal.toInt, f_province_id, user_province_set.size))
      list.toIterator
    }).flatMap(x => x).toDF("terminal", "f_code", "f_uv")
    user_cnt
  }

  def groupByHourEvent(day: String, base_df: DataFrame, hiveContext: SQLContext, tableName: String) = {
    val changName_udf = udf((program_name: String) => {
      val program_name_new = program_name.replaceAll("(\\(.*\\))", "")
      program_name_new
    })

    base_df.withColumn("f_program_name", changName_udf(col("f_program_name")))
      .registerTempTable(s"event_hour_table")
    val event_df = hiveContext.sql(
      s"""
         |select
         |cast($day as string) as f_date,
         |f_hour,
         |f_channel_id,f_channel_name,f_program_name,f_terminal,
         |f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_play_time_total
         |from
         |(
         |select
         |f_hour,f_channel_id,f_channel_name,f_program_name,f_terminal,
         |f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_play_time_total,
         |dense_rank() over(partition by f_channel_id ,f_channel_name,
         |f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name order by f_play_time_total desc ) as f_rank
         |from
         |(
         |select f_hour, f_channel_id,f_channel_name,f_program_name,f_terminal,
         |f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |sum(f_user_playtime) as f_play_time_total
         |from event_hour_table
         |group by f_hour,f_channel_id,f_channel_name,f_program_name,f_terminal,
         |f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name
         |)t
         |)tt
         |where f_rank <4
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(event_df, tableName)
  }

  def groupBylatestDayEvent(base_df: DataFrame, hiveContext: SQLContext, day: String, tableName: String) = {
    val latestDay = DateUtils.getNDaysBefore(6, day)
    val changName_udf = udf((program_name: String) => {
      val program_name_new = program_name.replaceAll("(\\(.*\\))", "")
      program_name_new
    })
    base_df.filter(col("f_date") >= latestDay and col("f_date") <= day)
      .withColumn("f_program_name", changName_udf(col("f_program_name")))
      .registerTempTable(s"event_latest_table")

    val event_df = hiveContext.sql(
      s"""
         |select
         |cast($latestDay as string) as f_start_date,
         |cast($day as string) as f_end_date,
         |f_channel_id,f_channel_name,f_program_name,f_terminal,
         |f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_play_time_total
         |from
         |(
         |select
         |f_channel_id,f_channel_name,f_program_name,f_terminal,
         |f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_play_time_total,
         |dense_rank() over(partition by f_channel_id ,f_channel_name,
         |f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name order by f_play_time_total desc ) as f_rank
         |from
         |(
         |select f_channel_id,f_channel_name,f_program_name,f_terminal,
         |f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |sum(f_user_playtime) as f_play_time_total
         |from event_latest_table
         |group by f_channel_id,f_channel_name,f_program_name,f_terminal,
         |f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name
         |)t
         |)tt
         |where f_rank <4
      """.stripMargin)
    DBUtils.saveDataFrameToPhoenixNew(event_df, tableName)
  }

  def groupByRangeTimeEvent(base_df: DataFrame, hiveContext: SQLContext, date: String, types: String, year: String, tableName: String): Unit = {
    var starttime = ""
    var endtime = ""
    var f_date = ""
    val tmep_table = types + "_table"
    if (types.equals("day")) {
      f_date = date
    } else if (types.equals("week")) {
      starttime = MOEDateUtils.getFirstOfWeek(date)
      endtime = MOEDateUtils.getLastOfWeek(date)
      f_date = starttime
    } else if (types.equals("month")) {
      starttime = MOEDateUtils.getFirstDayOfMonth(year.toInt, date)
      endtime = MOEDateUtils.getLastDayOfMonth(year.toInt, date)
      f_date = starttime.substring(0, 6)
    } else if (types.equals("year")) {
      starttime = MOEDateUtils.getFirstDayOfYear(year.toInt)
      endtime = MOEDateUtils.getLastDayOfYear(year.toInt)
      f_date = year
    }

    val changName_udf = udf((program_name: String) => {
      val program_name_new = program_name.replaceAll("(\\(.*\\))", "")
      program_name_new
    })
    if (types.equals("day")) {
      base_df.withColumn("f_program_name", changName_udf(col("f_program_name"))).registerTempTable(s"$tmep_table")
    } else {
      base_df.filter(col("f_date") >= starttime and col("f_date") <= endtime)
        .withColumn("f_program_name", changName_udf(col("f_program_name")))
        .registerTempTable(s"$tmep_table")
    }

    val event_df = hiveContext.sql(
      s"""
         |select cast($f_date as string) as f_date,
         |f_channel_id,f_channel_name,f_program_name,f_terminal,
         |f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_play_time_total
         |from
         |(
         |select
         |f_channel_id,f_channel_name,f_program_name,f_terminal,
         |f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,f_play_time_total,
         |dense_rank() over(partition by f_channel_id ,f_channel_name,
         |f_terminal,f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name order by f_play_time_total desc ) as f_rank
         |from
         |(
         |select f_channel_id,f_channel_name,f_program_name,f_terminal,
         |f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name,
         |sum(f_user_playtime) as f_play_time_total
         |from $tmep_table
         |group by f_channel_id,f_channel_name,f_program_name,f_terminal,
         |f_region_id,f_city_id,f_province_id,f_region_name,f_city_name,f_province_name
         |)t
         |)tt
         |where f_rank <4
      """.stripMargin)
    //    event_df.printSchema()
    //    event_df.show(false)
    DBUtils.saveDataFrameToPhoenixNew(event_df, tableName)
  }

  def groupByHour(day: String, base_df: DataFrame, hiveContext: SQLContext, tableName: String) = {
    base_df.registerTempTable("channel_hour_table")
    val chanel_df = hiveContext.sql(
      s"""
         |select
         |cast($day as string) as f_date,
         |f_hour,
         |f_channel_id,f_channel_name,f_province_id,f_city_id,f_region_id,f_province_name,f_city_name,f_region_name,f_terminal,
         |sum(f_user_playtime) as f_play_time_total,
         |count(distinct f_user_id) as f_uv,
         |sum(f_pv) as f_pv
         |from channel_hour_table
         |group by f_date,f_hour,f_channel_id,f_channel_name,f_province_id,f_city_id,f_region_id,f_province_name,f_city_name,f_region_name,f_terminal
      """.stripMargin)
    //    chanel_df.printSchema()
    //    chanel_df.show(false)
    DBUtils.saveDataFrameToPhoenixNew(chanel_df, tableName)
  }

  def groupBylatestDay(day: String, base_df: DataFrame, hiveContext: SQLContext, tableName: String): Unit = {
    val latestDay = DateUtils.getNDaysBefore(6, day)
    var timegrap = 7
    val tmep_table = "latest_table"
    base_df.filter(col("f_date") >= latestDay and col("f_date") <= day).groupBy("f_user_id", "f_terminal", "f_region_id", "f_city_id", "f_province_id", "f_region_name", "f_city_name", "f_province_name", "f_channel_id",
      "f_channel_name").agg("f_pv" -> "sum", "f_user_playtime" -> "sum").withColumnRenamed("sum(f_user_playtime)", "f_user_playtime").withColumnRenamed("sum(f_pv)", "f_pv")
      .registerTempTable(s"$tmep_table")
    val chanel_df = hiveContext.sql(
      s"""
         |select
         |cast($latestDay as string) as f_start_date,
         |cast($day as string) as f_end_date,
         |f_channel_id,f_channel_name,f_province_id,f_city_id,f_region_id,f_province_name,f_city_name,f_region_name,f_terminal,
         |sum(f_user_playtime) as f_play_time_total,
         |count(distinct f_user_id) as f_uv,
         |sum(f_pv) as f_pv,
         |sum(if((f_user_playtime/$timegrap)<600,1,0)) as f_browse_user_count,
         |sum(if((f_user_playtime/$timegrap)>=600 and (f_user_playtime/$timegrap) < 3600,1,0)) as f_hobby_user_count,
         |sum(if((f_user_playtime/$timegrap)>=3600 ,1,0)) as f_steady_user_count
         |from $tmep_table
         |group by f_channel_id,f_channel_name,f_province_id,f_city_id,f_region_id,f_province_name,f_city_name,f_region_name,f_terminal
      """.stripMargin)

    //    chanel_df.printSchema()
    //    chanel_df.show(false)
    DBUtils.saveDataFrameToPhoenixNew(chanel_df, tableName)

  }

  def groupByCategory(date: String, base_df: DataFrame, types: String, hiveContext: SQLContext, begin: String, year: String, tableName: String) = {
    var starttime = ""
    var endtime = ""
    var f_date = ""
    val tmep_table = types + "_table"
    if (types.equals("day")) {
      f_date = date
    } else if (types.equals("week")) {
      starttime = MOEDateUtils.getFirstOfWeek(date)
      endtime = MOEDateUtils.getLastOfWeek(date)
      f_date = starttime
    } else if (types.equals("month")) {
      starttime = MOEDateUtils.getFirstDayOfMonth(year.toInt, date)
      endtime = MOEDateUtils.getLastDayOfMonth(year.toInt, date)
      f_date = starttime.substring(0, 6)
    } else if (types.equals("year")) {
      starttime = MOEDateUtils.getFirstDayOfYear(year.toInt)
      endtime = MOEDateUtils.getLastDayOfYear(year.toInt)
      f_date = year
    }
    var timegrap = 0L
    if (types.equals("day")) {
      base_df.groupBy("f_date", "f_user_id", "f_terminal", "f_region_id", "f_city_id", "f_province_id", "f_region_name", "f_city_name", "f_province_name", "f_channel_id",
        "f_channel_name").agg("f_pv" -> "sum", "f_user_playtime" -> "sum").withColumnRenamed("sum(f_user_playtime)", "f_user_playtime").withColumnRenamed("sum(f_pv)", "f_pv")
        .registerTempTable(s"$tmep_table")
      timegrap = 1
    } else {
      base_df.filter(col("f_date") >= starttime and col("f_date") <= endtime).groupBy("f_user_id", "f_terminal", "f_region_id", "f_city_id", "f_province_id", "f_region_name", "f_city_name", "f_province_name", "f_channel_id",
        "f_channel_name").agg("f_pv" -> "sum", "f_user_playtime" -> "sum").withColumnRenamed("sum(f_user_playtime)", "f_user_playtime").withColumnRenamed("sum(f_pv)", "f_pv")
        .registerTempTable(s"$tmep_table")
      timegrap = (DateUtils.dateToUnixtime(date, "yyyyMMdd") - DateUtils.dateToUnixtime(starttime, "yyyyMMdd")) / (24 * 3600) + 1

    }

    val chanel_df = hiveContext.sql(
      s"""
         |select cast($f_date as string) as f_date,f_channel_id,f_channel_name,f_province_id,f_city_id,f_region_id,f_province_name,f_city_name,f_region_name,f_terminal,
         |sum(f_user_playtime) as f_play_time_total,
         |count(distinct f_user_id) as f_uv,
         |sum(f_pv) as f_pv,
         |sum(if((f_user_playtime/$timegrap)<600,1,0)) as f_browse_user_count,
         |sum(if((f_user_playtime/$timegrap)>=600 and (f_user_playtime/$timegrap) < 3600,1,0)) as f_hobby_user_count,
         |sum(if((f_user_playtime/$timegrap)>=3600 ,1,0)) as f_steady_user_count
         |from $tmep_table
         |group by f_channel_id,f_channel_name,f_province_id,f_city_id,f_region_id,f_province_name,f_city_name,f_region_name,f_terminal
      """.stripMargin)
    //    chanel_df.printSchema()
    //    chanel_df.show(false)
    DBUtils.saveDataFrameToPhoenixNew(chanel_df, tableName)
  }

  def groupByDay(df: DataFrame, userFeatrue: DataFrame, hiveContext: SQLContext, day: String) = {
    df.groupBy("f_date", "f_user_id", "f_terminal", "f_region_id", "f_city_id", "f_province_id", "f_region_name", "f_city_name", "f_province_name", "f_channel_id",
      "f_channel_name").sum("f_pv").withColumnRenamed("`sum(f_pv)`", "f_pv").join(userFeatrue, "f_user_id").registerTempTable("chanel_day_table")
    val chanel_dy_df = hiveContext.sql(
      """
        |select f_date,f_channel_id,f_channel_name,f_province_id,f_city_id,f_region_id,f_province_name,f_city_name,f_region_name,
        |round(stb/user_play_time_total,3) as f_stb_per,round(ca/user_play_time_total,3) as f_ca_per,round(mobile/user_play_time_total,3) as f_mobile_per,
        |round(pad/user_play_time_total,3) as f_pad_per,round(pc/user_play_time_total,3) as f_pc_per,round(other/user_play_time_total,3) as f_other_per,
        |round(live_pay/user_total_consume,3) as f_live_pay_per,round(demand_pay/user_total_consume,3) as f_demand_pay_per,
        |round(lookbace_pay/user_total_consume,3) as f_lookbace_pay_per,round(app_pay/user_total_consume,3) as f_app_pay_per,
        |round(timeshift_pay/user_total_consume,3) as f_timeshift_pay_per,round(zonghe_pay/user_total_consume,3) as f_zonghe_pay_per,
        |round(monitor_pay/user_total_consume,3) as f_monitor_pay_per,round(audio_pay/user_total_consume,3) as f_audio_pay_per,round(package_pay/user_total_consume,3) as f_package_pay_per,
        |round(demand/user_play_time_total,3) as f_demand_per ,round(live/user_play_time_total,3) as f_live_per,
        |round(timeshift/user_play_time_total,3) as f_timeshift_per,round(lookback/user_play_time_total,3) as f_lookback_per,
        |f_uv,f_pv,
        |round(man/f_uv,3) as man,
        |round(won/f_uv,3) as won,
        |round(24below/f_uv,3) as 24below,
        |round(24to45/f_uv,3) as 24to45,
        |round(45over/f_uv,3) as 45over
        |from
        |(
        |select f_date,f_channel_id,f_channel_name,f_province_id,f_city_id,f_region_id,f_province_name,f_city_name,f_region_name,
        |sum(stb) as stb,sum(ca) as ca ,sum(mobile) as mobile,sum(pad) as pad,sum(pc) as pc,sum(other) as other,
        |sum(live_pay) as live_pay,sum(demand_pay) as demand_pay,sum(lookbace_pay) as lookbace_pay,sum(app_pay) as app_pay,sum(timeshift_pay) as timeshift_pay,
        |sum(zonghe_pay) as zonghe_pay,sum(monitor_pay) as monitor_pay,sum(audio_pay) as audio_pay,sum(package_pay) as package_pay,
        |sum(demand) as demand,sum(live) as live,sum(timeshift) as timeshift,sum(lookback) as lookback,
        |sum(user_play_time_total) as user_play_time_total,
        |if(sum(user_total_consume)==0,1,sum(user_total_consume)) as user_total_consume,
        |count(distinct f_user_id) as f_uv,
        |sum(f_pv) as f_pv,
        |sum(case sex when '1' then 1 else 0 end ) as man,
        |sum(case sex when '2' then 1 else 0 end ) as won,
        |sum(case ageRange when 1 then 1 else 0 end) as 24below,
        |sum(case ageRange when 2 then 1 else 0 end) as 24to45,
        |sum(case ageRange when 3 then 1 else 0 end) as 45over
        |from chanel_day_table
        |group by f_date,f_channel_id,f_channel_name,f_province_id,f_city_id,f_region_id,f_province_name,f_city_name,f_region_name
        |)t
      """.stripMargin)

    DBUtils.saveDataFrameToPhoenixNew(chanel_dy_df, "t_chanel_report_by_day")
  }

  //获取不同时间段不同频道下节目信息
  def getChannelEventData(sqlContext: SQLContext): DataFrame = {
    val timeudf = udf((start_time: String, duration: String) => {
      DateUtils.get_end_time(start_time, duration)
    })
    val sql = """(select homed_service_id as channel_id ,f_relevance_id,event_id as program_id,f_series_id ,event_name as program_name ,cast(start_time as char) as start_time ,cast(duration as char) as duration from homed_eit_schedule_history) as a"""
    val channel_event_df = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    val channel_event_final_df = channel_event_df.withColumn("end_time", timeudf(col("start_time"), col("duration"))).drop("duration")
    val channelSql = "(select channel_id ,chinese_name as channel_name from channel_store) as a"
    val channelDF = DBUtils.loadMysql(sqlContext, channelSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS).distinct()
    channel_event_final_df.join(channelDF, "channel_id")

  }

  def getUserFeatrue(sqlContext: SQLContext, day: String): DataFrame = {
    //获取用户性别（1：男，2：女）、年龄段（1：24岁以下，2：24岁到45岁，3：45岁以上）
    val userAgeAndSexSql = "(select DA as f_user_id,cast(sex as char) as sex,case when (age < 24) then 1 when  (age>=24 and age <=45) then 2 when (age>45) then 3 else 2 end as ageRange from account_info ) as a"
    val userAgeAndSexDF = DBUtils.loadMysql(sqlContext, userAgeAndSexSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()

    //用户总在线时长及平均在线时长和终端播放时长（30天）
    val beginDay = DateUtils.getNDaysBefore(30, day)
    val userWatchTimeSql = s"(select * from t_user_time where f_date >='$beginDay' and f_date <='$day') as a "
    val userWatchTimeDF = DBUtils.loadDataFromPhoenix2(sqlContext, userWatchTimeSql)
    userWatchTimeDF.groupBy("f_user_id", "f_terminal").agg("f_play_time" -> "sum")
      .selectExpr("f_user_id", "f_terminal", "`sum(f_play_time)` as terminal_playtime").registerTempTable("userWatchTimeDF_table")
    val userWatchTimeDF_2 = sqlContext.sql(
      """
        |select f_user_id,
        |sum(case f_terminal when '1' then terminal_playtime else 0 end ) as stb,
        |sum(case f_terminal when '2' then terminal_playtime else 0 end) as ca,
        |sum(case f_terminal when '3' then terminal_playtime else 0 end) as mobile,
        |sum(case f_terminal when '4' then terminal_playtime else 0 end) as pad,
        |sum(case f_terminal when '5' then terminal_playtime else 0 end) as pc,
        |sum(case f_terminal when '0' then terminal_playtime else 0 end) as other,
        |avg(terminal_playtime) as user_play_time_avg,
        |sum(terminal_playtime) as user_play_time_total
        |from userWatchTimeDF_table
        |group by f_user_id
      """.stripMargin)

    //用户总消费及按类型消费情况
    val day1 = DateUtils.transformDateStr(beginDay) + " 00:00:00"
    val day2 = DateUtils.transformDateStr(day) + " 23:59:59"
    val userConsumeSql = s"(select * from t_billing where f_time >= '$day1' and f_time <='$day2') as a"
    val userConsumeDF = DBUtils.loadDataFromPhoenix2(sqlContext, userConsumeSql)
    userConsumeDF.groupBy("f_user_id", "f_billing_type").sum("f_money")
      .selectExpr("f_user_id", "f_billing_type", "`sum(f_money)` as type_consume").registerTempTable("userConsumeDF_table")
    val userConsumeDF_2 = sqlContext.sql(
      """
        |select f_user_id,
        |sum(case f_billing_type when 1 then type_consume else 0 end) as live_pay,
        |sum(case f_billing_type when 2 then type_consume else 0 end) as demand_pay,
        |sum(case f_billing_type when 3 then type_consume else 0 end) as lookbace_pay,
        |sum(case f_billing_type when 4 then type_consume else 0 end) as app_pay,
        |sum(case f_billing_type when 5 then type_consume else 0 end) as timeshift_pay,
        |sum(case f_billing_type when 6 then type_consume else 0 end) as zonghe_pay,
        |sum(case f_billing_type when 7 then type_consume else 0 end) as monitor_pay,
        |sum(case f_billing_type when 8 then type_consume else 0 end) as audio_pay,
        |sum(case f_billing_type when 0 then type_consume else 0 end) as package_pay,
        |sum(type_consume) as user_total_consume
        |from userConsumeDF_table
        |group by f_user_id
      """.stripMargin)

    //用户对直播时移点播回看观看情况
    val userPlayTypeSql = s"(select f_user_id,f_play_type,f_play_time from t_user_watch_detail where f_date>='$beginDay' and f_date<='$day') as a"
    val userPlayTypeDF = DBUtils.loadDataFromPhoenix2(sqlContext, userPlayTypeSql)
    userPlayTypeDF.groupBy("f_user_id", "f_play_type").agg("f_play_time" -> "sum")
      .selectExpr("f_user_id", "f_play_type", "`sum(f_play_time)` as user_playtype_time_total").registerTempTable("userPlayTypeDF_table")

    val userPlayTypeDF_2 = sqlContext.sql(
      """
        |select f_user_id,
        |sum(case f_play_type when 'demand' then user_playtype_time_total else 0 end) as demand,
        |sum(case f_play_type when 'live' then user_playtype_time_total else 0 end) as live,
        |sum(case f_play_type when 'timeshift' then user_playtype_time_total else 0 end) as timeshift,
        |sum(case f_play_type when 'lookback' then user_playtype_time_total else 0 end) as lookback
        |from userPlayTypeDF_table
        |group by f_user_id
      """.stripMargin)

    val final_df = userWatchTimeDF_2
      .join(userPlayTypeDF_2, "f_user_id")
      .join(userConsumeDF_2, Seq("f_user_id"), "left")
      .join(userAgeAndSexDF, "f_user_id").na.fill(0)

    final_df.distinct()
  }

  def getAdressInfo(hiveContext: SQLContext): DataFrame = {
    //省信息
    val provincesql =
      """(SELECT cast(province_id as char)as f_province_id,province_name as f_province_name from province) as c""".stripMargin
    //城市信息
    val citysql =
      """(SELECT cast(city_id  as char) as f_city_id,city_name as f_city_name,cast(province_id as char) as f_province_id from city) as d """.stripMargin
    //区域信息
    val regionsql =
      """(SELECT cast(area_id as char) as f_region_id,area_name as f_region_name ,cast(city_id as char) as f_city_id from area) as e """.stripMargin
    val provinceDF = DBUtils.loadMysql(hiveContext, provincesql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()
    val cityDF = DBUtils.loadMysql(hiveContext, citysql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()
    val regionDF = DBUtils.loadMysql(hiveContext, regionsql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()

    val adress_DF = provinceDF.join(cityDF, "f_province_id").join(regionDF, "f_city_id").distinct()
    adress_DF
  }

  /*select cast($f_date as string) as f_date,f_channel_id,f_channel_name,f_province_id,f_city_id,f_region_id,f_province_name,f_city_name,f_region_name,f_terminal,
  round(stb/user_play_time_total,3) as f_stb_per,round(ca/user_play_time_total,3) as f_ca_per,round(mobile/user_play_time_total,3) as f_mobile_per,
  round(pad/user_play_time_total,3) as f_pad_per,round(pc/user_play_time_total,3) as f_pc_per,round(other/user_play_time_total,3) as f_other_per,
  round(live_pay/user_total_consume,3) as f_live_pay_per,round(demand_pay/user_total_consume,3) as f_demand_pay_per,
  round(lookbace_pay/user_total_consume,3) as f_lookbace_pay_per,round(app_pay/user_total_consume,3) as f_app_pay_per,
  round(timeshift_pay/user_total_consume,3) as f_timeshift_pay_per,round(zonghe_pay/user_total_consume,3) as f_zonghe_pay_per,
  round(monitor_pay/user_total_consume,3) as f_monitor_pay_per,round(audio_pay/user_total_consume,3) as f_audio_pay_per,round(package_pay/user_total_consume,3) as f_package_pay_per,
  round(demand/user_play_time_total,3) as f_demand_per ,round(live/user_play_time_total,3) as f_live_per,
  round(timeshift/user_play_time_total,3) as f_timeshift_per,round(lookback/user_play_time_total,3) as f_lookback_per,
  f_uv,f_pv,
  round(man/f_uv,3) as man_per,
  round(won/f_uv,3) as won_per,
  round(24below/f_uv,3) as below24_per,
  round(24to45/f_uv,3) as between24and45_per,
  round(45over/f_uv,3) as over45_per
  from
  (*/
  /*  val chanel_df = hiveContext.sql(
      s"""
         |select cast($f_date as string) as f_date,f_channel_id,f_channel_name,f_province_id,f_city_id,f_region_id,f_province_name,f_city_name,f_region_name,f_terminal,
         |sum(stb) as f_stb_play_time,sum(ca) as f_ca_play_time ,sum(mobile) as f_mobile_play_time,sum(pad) as f_pad_play_time,sum(pc) as f_pc_play_time,sum(other) as f_other_play_time,
         |sum(live_pay) as f_live_pay,sum(demand_pay) as f_demand_pay,sum(lookbace_pay) as f_lookbace_pay,sum(app_pay) as f_app_pay,sum(timeshift_pay) as f_timeshift_pay,
         |sum(zonghe_pay) as f_zonghe_pay,sum(monitor_pay) as f_monitor_pay,sum(audio_pay) as f_audio_pay,sum(package_pay) as f_package_pay,
         |sum(demand) as f_demand_play_time,sum(live) as f_live_play_time,sum(timeshift) as f_timeshift_play_time,sum(lookback) as f_lookback_play_time,
         |sum(user_play_time_total) as f_user_play_time_total,
         |sum(user_total_consume) as f_user_total_consume,
         |count(distinct f_user_id) as f_uv,
         |sum(if(user_play_time_avg>7200,1,0)) as f_activity_uv,
         |sum(f_pv) as f_pv,
         |sum(case sex when '1' then 1 else 0 end ) as f_man,
         |sum(case sex when '2' then 1 else 0 end ) as f_won,
         |sum(case ageRange when 1 then 1 else 0 end) as f_below24,
         |sum(case ageRange when 2 then 1 else 0 end) as f_between24and45,
         |sum(case ageRange when 3 then 1 else 0 end) as f_over45
         |from $tmep_table
         |group by f_channel_id,f_channel_name,f_province_id,f_city_id,f_region_id,f_province_name,f_city_name,f_region_name,f_terminal
      """.stripMargin)*/

  //区分时间段
  def process(startTime: String, playTimes: Long): ListBuffer[(Integer, Integer, String, String, Long, Integer)] = {
    val list = new ListBuffer[(Integer, Integer, String, String, Long, Integer)]
    var start = DateUtils.transferYYYY_DD_HH_MMHHSSToDate(startTime)
    val end = start.plusSeconds(playTimes.toInt)
    var hour: Integer = 0
    val m = start.getMinuteOfHour
    var timeRange: Integer = 30
    var endTemp = end
    var flag = true
    var plays = 0
    var i = 0
    var playCount = 0
    while (flag) {
      i match {
        case 0 => {
          if (m < 30) {
            endTemp = start.plusMinutes(30 - m).minusSeconds(start.getSecondOfMinute)
          } else {
            endTemp = start.plusMinutes(60 - m).minusSeconds(start.getSecondOfMinute)
          }
          i = 1
          playCount = 1
        }
        case _ => {
          endTemp = start.plusMinutes(30)
          playCount = 0
        }
      }
      hour = start.getHourOfDay
      timeRange = if (start.getMinuteOfHour < 30) 30 else 60
      if (end.compareTo(endTemp) > 0) {
        val startStr = start.toString(DateUtils.YYYY_MM_DD_HHMMSS)
        val endStr = endTemp.toString(DateUtils.YYYY_MM_DD_HHMMSS)
        //跨天
        if (endTemp.getDayOfYear != start.getDayOfYear) {
          plays = 86400 - start.getSecondOfDay
        } else {
          plays = endTemp.getSecondOfDay - start.getSecondOfDay
        }
        list += ((hour, timeRange, startStr, endStr, plays.toLong, playCount))
      } else {
        flag = false
        val startStr = start.toString(DateUtils.YYYY_MM_DD_HHMMSS)
        val endStr = end.toString(DateUtils.YYYY_MM_DD_HHMMSS)
        //跨天
        if (endTemp.getDayOfYear != start.getDayOfYear) {
          plays = 86400 - start.getSecondOfDay
        } else {
          plays = end.getSecondOfDay - start.getSecondOfDay
        }
        list += ((hour, timeRange, startStr, endStr, plays.toLong, playCount))
      }
      start = endTemp
    }
    list
  }
}
