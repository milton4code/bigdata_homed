package cn.ipanel.homed.repots

import cn.ipanel.common.{DBProperties, SparkSession}
import cn.ipanel.utils.{DBUtils, DateUtils, MOEDateUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel

/** *
  * 点播、回看、直播、时移
  * 所选区域、所选时间区间、所选业务（点播、回看、直播、时移 ）播放总次数、独立用户数,以上的运营整体情况的连续展现
  * 平均每次播放时长、平均每户播放时长、按日期趋势、类型占比
  * 携带排行榜：播放量（次数）排行榜、饱和度排行榜、用户排行榜
  */

object PlayCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("请输入参数 如 20180327")
      System.exit(-1)
    }
    val sparkSession = SparkSession("PlayCount")
    val spark = sparkSession.sqlContext
    sparkSession.conf.set("spark.sql.broadcastTimeout", "36000")
    //日期
    val begin = args(0)
    //周
    val week = MOEDateUtils.getWeekOfYear(begin).toString
    //月
    val month = MOEDateUtils.getMonth(begin).toString
    //季度
    val quarter = MOEDateUtils.getSeason(begin).toString
    //年
    val year = MOEDateUtils.getYear(begin).toString

    delMysql("f_day", begin, "t_play_count")
    delMysql("f_category", week, "t_play_count_week")
    delMysql("f_category", month, "t_play_count_month")
    delMysql("f_category", quarter, "t_play_count_quarter")
    delMysql("f_category", year, "t_play_count_year")

    val baseDF_day = getDataFrame("day", begin, spark, year) //统计天及近七天数据
    groupByDay(baseDF_day, spark, begin)

    //    val baseDF_year = getDataFrame("year", year, spark, year) //统计年
    //    baseDF_year.persist(StorageLevel.MEMORY_AND_DISK)
    //    groupByTypes(baseDF_year, spark, "year")

    val baseDF_quarter = getDataFrame("quarter", quarter, spark, year) //统计季度
    baseDF_quarter.persist(StorageLevel.MEMORY_AND_DISK)
    groupByTypes(baseDF_quarter, spark, "quarter")

    //    val baseDF_month = getDataFrame("month", month, spark, year) //统计月
    val baseDF_month = baseDF_quarter.filter(col("month") === month) //拿到月的数据
    groupByTypes(baseDF_month, spark, "month")

    //    val baseDF_week = getDataFrame("week", week, spark, year) //统计周
    val baseDF_week = baseDF_quarter.filter(col("week") === week) //拿到周的数据
    groupByTypes(baseDF_week, spark, "week")

    baseDF_quarter.unpersist()
  }

  /** *
    * 获取DataFrame
    *
    * @param types
    * @param args
    * @param spark
    * @return
    */
  def getDataFrame(types: String, begin: String, spark: HiveContext, year: String): DataFrame = {
    val videoPlay = spark.sql(
      s"""select userid,
         |regionid,
         |playtype,
         |starttime,
         |endtime,
         |case when playtype= 'timeshift' then 'one-key-timeshift' else playtype end  as playtypes,
         |serviceid,
         |cast(year(starttime) as string) as year,
         |cast(floor(substr(starttime,6,2)/3.1)+1 as string) as quarter,
         |cast(month(starttime) as string) as month,
         |cast(weekofyear(starttime) as string) as week,
         |cast(day as string) as day
         |from bigdata.orc_video_play
         |where playtype='demand'or playtype='lookback' or playtype='live'or playtype='timeshift' or playtype='one-key-timeshift'
         |""".stripMargin)
      .drop(col("playtype"))
    videoPlay.registerTempTable("temp")
    var videoDF = spark.emptyDataFrame
    if (s"$types".equals("day")) { // 近七天数据
      val latestDay = DateUtils.getNDaysBefore(6, begin)
      videoDF = spark.sql(
        s"""
           |select * from temp where $types>=$latestDay and $types<=$begin and year=$year
      """.stripMargin)
    } else {
      videoDF = spark.sql(
        s"""
           |select * from temp where $types==$begin and year=$year
      """.stripMargin)
    }
    val timeCountUdf = udf((startTime: String, endTime: String) => {
      val timeLength = DateUtils.dateToUnixtime(endTime) - DateUtils.dateToUnixtime(startTime)
      Math.abs(timeLength)
    })
    val demandCP = getDemandCP(spark)
    //平均每次播放时长、平均每户播放时长、按日期趋势、类型占比
    //携带排行榜：播放量（次数）排行榜、饱和度排行榜、用户排行榜
    val baseDF = videoDF
      .withColumn("timeLength", timeCountUdf(col("starttime"), col("endtime")))
      .drop(col("starttime"))
      .drop(col("endtime"))
      .join(demandCP, videoPlay.col("serviceid") === demandCP.col("f_video_id"), "left")
      .selectExpr("userid", "regionid", "playtypes", "timeLength", "case when f_cp_id is null then 'other' when f_cp_id = '' then 'other' else f_cp_id end as f_cp_id",
        "year", "quarter", "month", "week", "day").cache()
    baseDF
  }

  /** *
    * 根据天及近七天进行统计
    *
    * @param baseDF
    */
  def groupByDay(baseDF: DataFrame, spark: SQLContext, begin: String) = {
    val df = baseDF.where(col("day") === begin).groupBy(col("playtypes"), col("day"), col("f_cp_id"))
      .agg("timeLength" -> "sum", "playtypes" -> "count")
      .selectExpr("f_cp_id", "playtypes", "day", "`sum(timeLength)` as totalPlayTime", "`count(playtypes)` as playCnt")

    val totalPlayTimeDf = df.groupBy("day")
      .sum("totalPlayTime")
      .withColumnRenamed("sum(totalPlayTime)", "allCategoryTime")
      .selectExpr("day", "allCategoryTime")

    val df1 = baseDF.selectExpr("userid", "playtypes", "day", "f_cp_id")
      .distinct()
      .groupBy(col("playtypes"), col("day"), col("f_cp_id"))
      .count()
      .withColumnRenamed("count", "userCnt")

    val df2 = df.join(df1, Seq("playtypes", "day", "f_cp_id")).join(totalPlayTimeDf, "day")
      .withColumn("user_avg", col("totalPlayTime") / col("userCnt"))
      .withColumn("play_avg", col("totalPlayTime") / col("playCnt"))
      .withColumn("persentage", col("totalPlayTime") / col("allCategoryTime"))
      .selectExpr("day as f_day", "playtypes as f_playtypes", "f_cp_id", "totalPlayTime as f_totalPlayTime", "allCategoryTime as f_allCategoryTime", "userCnt as f_userCnt", "playCnt as f_playCnt", "user_avg as f_user_avg", "play_avg as f_play_avg", "persentage as f_persentage")

    //统计近七天
    val latestDay = DateUtils.getNDaysBefore(6, begin)
    val latest_df = baseDF.groupBy(col("playtypes"), col("f_cp_id"))
      .agg("timeLength" -> "sum", "playtypes" -> "count")
      .selectExpr("f_cp_id", "playtypes", "`sum(timeLength)` as totalPlayTime", "`count(playtypes)` as playCnt")

    val latest_totalPlayTimeDf = latest_df.selectExpr("sum(totalPlayTime) as allCategoryTime")

    val latest_df1 = baseDF.selectExpr("userid", "playtypes", "f_cp_id")
      .distinct()
      .groupBy(col("playtypes"), col("f_cp_id"))
      .count()
      .withColumnRenamed("count", "userCnt")

    val latest_df2 = latest_df.join(latest_df1, Seq("playtypes", "f_cp_id")).join(latest_totalPlayTimeDf)
      .withColumn("user_avg", col("totalPlayTime") / col("userCnt"))
      .withColumn("play_avg", col("totalPlayTime") / col("playCnt"))
      .withColumn("persentage", col("totalPlayTime") / col("allCategoryTime"))
      .selectExpr(s"$latestDay as f_starttime", s"$begin as f_endtime", "playtypes as f_playtypes", "f_cp_id", "totalPlayTime as f_totalPlayTime", "allCategoryTime as f_allCategoryTime", "userCnt as f_userCnt", "playCnt as f_playCnt", "user_avg as f_user_avg", "play_avg as f_play_avg", "persentage as f_persentage")
    df2.show()
    latest_df2.show()
    //保存到数据库
    DBUtils.saveToMysql(df2, "t_play_count", DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD) //天
    DBUtils.saveToHomedData_Overwrite(latest_df2, "t_play_count_latest") //近七天
  }

  /** *
    * 根据传入的类型进行分类统计
    *
    * @param baseDF
    * @param spark
    * @param types （周，月，季度，年）
    */
  def groupByTypes(baseDF: DataFrame, spark: SQLContext, types: String) = {
    import spark.implicits._
    val df = baseDF.groupBy(col("playtypes"), col(s"$types"), col("f_cp_id"))
      .agg("timeLength" -> "sum", "playtypes" -> "count", "day" -> "collect_set").rdd.map(x => {
      val playtypes = x.getAs[String]("playtypes")
      val category = x.getAs[String](s"$types")
      val f_cp_id = x.getAs[String]("f_cp_id")
      val totalPlayTime = x.getAs[Long]("sum(timeLength)")
      val playCnt = x.getAs[Long]("count(playtypes)")
      val setArray = x.getAs[Seq[String]]("collect_set(day)").sorted
      val startTime = setArray(0)
      val endTime = setArray(setArray.length - 1)
      (playtypes, category, f_cp_id, totalPlayTime, playCnt, startTime, endTime)
    }).toDF().selectExpr("_3 as f_cp_id", "_1 as playtypes", "_4 as totalPlayTime", "_5 as playCnt", "_2 as category", "_6 as startTime1", "_7 as endTime1")

    val minMaxDf = df.groupBy(col("category")).agg("totalPlayTime" -> "sum", "startTime1" -> "min", "endTime1" -> "max").rdd.map(x => {
      val category = x.getAs[String]("category")
      val allCategoryTime = x.getAs[Long]("sum(totalPlayTime)")
      val startTime = x.getAs[String]("min(startTime1)")
      val endTime = x.getAs[String]("max(endTime1)")
      (startTime, endTime, category, allCategoryTime)
    }).toDF().selectExpr("_1 as startTime", "_2 as endTime", "_3 as category", "_4 as allCategoryTime")

    val df1 = baseDF.selectExpr("userid", "playtypes", s"$types", "f_cp_id")
      .distinct()
      .groupBy(col("playtypes"), col(s"$types"), col("f_cp_id"))
      .count()
      .withColumnRenamed("count", "userCnt")
      .withColumnRenamed(s"$types", "category")

    val df2 = df.join(df1, Seq("playtypes", "category", "f_cp_id")).join(minMaxDf, "category")
      .withColumn("user_avg", col("totalPlayTime") / col("userCnt"))
      .withColumn("play_avg", col("totalPlayTime") / col("playCnt"))
      .withColumn("persentage", col("totalPlayTime") / col("allCategoryTime"))
      .drop("startTime1")
      .drop("endTime1")
      .selectExpr("startTime as f_startTime", "endTime as f_endTime", "category as f_category", "playtypes as f_playtypes", "f_cp_id", "totalPlayTime as f_totalPlayTime", "allCategoryTime as f_allCategoryTime", "userCnt as f_userCnt", "playCnt as f_playCnt", "user_avg as f_user_avg", "play_avg as f_play_avg", "persentage as f_persentage")
    df2.show()
    var tableName = ""
    types match {
      case "week" => tableName = "t_play_count_week"
      case "month" => tableName = "t_play_count_month"
      case "quarter" => tableName = "t_play_count_quarter"
      case "year" => tableName = "t_play_count_year"
    }
    //保存到数据库
    DBUtils.saveToMysql(df2, s"$tableName", DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)
  }

  def getDemandCP(hiveContext: HiveContext): DataFrame = {
    val sql =
      """(
           |SELECT distinct(video_id) as f_video_id,
           |f_cp_id
           |FROM video_info
            ) as vod_info
      """.stripMargin
    val vodMeiZiInfoDF = DBUtils.loadMysql(hiveContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    vodMeiZiInfoDF
  }

  /**
    * 防止重跑导致重复数据
    **/
  def delMysql(types: String, begin: String, table: String): Unit = {
    val del_sql = s"delete from $table where $types='$begin'"
    DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD, del_sql)
  }
}