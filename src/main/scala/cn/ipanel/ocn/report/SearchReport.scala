package cn.ipanel.ocn.report

import cn.ipanel.common.{DBProperties, SparkSession}
import cn.ipanel.ocn.etl.OcnConstant
import cn.ipanel.utils.DBUtils
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * SearchReport<br> 
  * 搜索报表
  *
  * @author liujjy
  *         create 2018/5/26
  * @since 1.0.0
  */
object SearchReport {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("参数错误,请输入日期 : 20180525")
      sys.exit(-1)
    }
    val day = args(0)

    val sparkSession = new SparkSession("SearchReport", "")
    val sqlContext = sparkSession.sqlContext
    val sc = sparkSession.sparkContext
    sqlContext.sql("use bigdata")
    import sqlContext.sql
    // 0122  AR 表示业务搜索范围 K 表示所有关键字 R 用户通过搜索选择的媒资节目ID A 用户行为 P　节目id标识  and params['K'] is not null
    sql(
      s"""
         |select device_type,user_id,params['K'] key_word,params['R'] as result,params['A'] as action,params['P'] as programId
         |from orc_ocn_reported
         |where  day = '$day' and service_type=${OcnConstant.SEARCH}
      """.stripMargin).registerTempTable("t1")
    sqlContext.cacheTable("t1")

    //以搜索词keyword+设备类型 作为唯一标示去统计
    //select key_word as f_key_word,device_type f_device_type, '1' as f_word_type , count(key_word) f_pv,count(DISTINCT(key_word)) f_uv,
    val resultDF1 = sql(
      s"""
         |select key_word as f_key_word,device_type f_device_type, '1' as f_word_type , count(key_word) f_pv,count(DISTINCT(user_id)) f_uv,
         |"" as f_program_id,sum(if(action=2,1,0)) as f_click_count
         |FROM t1 where key_word is not null and programId is null GROUP by key_word,device_type
        """.stripMargin)

    //以点击result+设备类型 作为唯一标示去统计
    //select result series_id , device_type f_device_type ,'2'as  f_word_type, count(result) f_pv, count(DISTINCT(result)) f_uv,result as f_program_id,
    val resultDF2 = sql(
      """
        |select result series_id , device_type f_device_type ,'2'as  f_word_type, count(result) f_pv, count(DISTINCT(user_id)) f_uv,result as f_program_id,
        |sum(if(programId is not null,1,0)) as f_click_count
        |FROM t1 where t1.result is not null GROUP by result,device_type
      """.stripMargin)
    val videoSeriesDF: DataFrame = getVideoSeries(sqlContext)

    val videoSeriesBr = sc.broadcast(videoSeriesDF)

    val resultDF = resultDF2.join(videoSeriesBr.value, Seq("series_id"))
      .selectExpr("f_key_word", "f_device_type", "f_word_type", "f_pv", "f_uv", "f_program_id","f_click_count")
      .unionAll(resultDF1)
      .selectExpr(s"'$day' as f_date", "f_key_word", "f_word_type", "f_pv", "f_uv", "f_device_type", "f_program_id","f_click_count")
    //    resultDF.show(false)
    sqlContext.uncacheTable("t1")
    DBUtils.saveToMysql(resultDF, "t_ocn_search", DBProperties.OCN_JDBC_URL, DBProperties.OCN_USER, DBProperties.OCN_PASSWORD)

    sc.stop()
  }

  def getVideoSeries(sqlContext: SQLContext): DataFrame = {
    //节目Id关联 剧集
    val videoSeriesSql = s" (select f_duplicate_id series_id,f_program_name as f_key_word from t_duplicate_series) as t_video_series"
    val videoSeriesDF = DBUtils.loadMysql(sqlContext, videoSeriesSql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    videoSeriesDF
  }
}
