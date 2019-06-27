package cn.ipanel.homed.repots


import cn.ipanel.common._
import cn.ipanel.common.{CluserProperties, SparkSession}
import cn.ipanel.etl.LogConstant
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime


/**
  * * 筛选报表统计
  * @author Liu gaohui
  * @date 2018/05/01
  */
object FilterDetail {
  def main(args: Array[String]): Unit = {
    var day = DateUtils.getYesterday()
    if (args.size == 1) {
      day = args(0)
    }
    val sparkSession = SparkSession("FilterDetail")
    val hiveContext = sparkSession.sqlContext
    import sparkSession.sqlContext.implicits._
    hiveContext.sql(s"use ${Constant.HIVE_DB}")
    //过滤orc_nginx_log中的数据，过滤不含筛选条件的数据
    val lines_base = hiveContext.sql(
      s"""
         |select report_time,body['accesstoken'] as accesstoken,
         |body['subtype'] as subtype,body['country'] as country,
         |body['contenttype'] as contenttype
         |from orc_nginx_log
         |where day='$day' and key_word like '%/filter/filter%'
            """.stripMargin)
    val lines = lines_base.map(x=>{
      val report_time = x.getAs[String]("report_time")
      val date = report_time.split(" ")(0).replace("-","")
      val hour =  report_time.split(" ")(1).split(":")(0)
      val minute = report_time.split(" ")(1).split(":")(1)
      val f_timerange = ColumnDetail.getTimeRangeByMinute(minute.toInt)
      val accesstoken = x.getAs[String]("accesstoken")
      val subtype = x.getAs[String]("subtype")
      val country = x.getAs[String]("country")
      val contenttype = x.getAs[String]("contenttype")
      val subtypecount = if (subtype != null) 1 else 0
      val countrycount = if (country != null) 1 else 0
      val key = date+","+hour+","+f_timerange+","+accesstoken+","+contenttype+","+subtype+","+country
      (key, (subtypecount,countrycount))
    }).reduceByKey((a,b)=>(a._1+b._1,a._2+b._2)).map(x=>{
      val keyarr = x._1.split(",")
      val f_date = keyarr(0)
      val f_hour = keyarr(1)
      val f_timerange = keyarr(2)
      val accesstoken = keyarr(3)
      val arr = TokenParser.parserAsUser(accesstoken)//userId,deviceId, deviceType, proviceId, cityId, regionId
      val f_userid = arr.DA.toString
      val deviceId = arr.device_id
      val f_terminal = arr.device_type
      val f_province_id = arr.province_id
      val f_city_id = arr.city_id
      val f_region_id = arr.region_id
      val f_contenttype = keyarr(4)
      val f_subtype = keyarr(5)
      val f_country = keyarr(6)
      val f_subtypecount = x._2._1
      val f_countrycount = x._2._2
      filterInfo(f_date,f_hour,f_timerange,f_userid,f_terminal,f_province_id,f_city_id,f_region_id,f_contenttype,f_subtype,f_subtypecount,f_country,f_countrycount)
    }).filter(x=>x.f_contenttype!="null"&&(x.f_country!="null"||x.f_subtype!="null")).filter(x=>x.f_userid!="0").toDF()
    FilterDetail(lines,sparkSession)

    sparkSession.stop()
  }

  /**
    ** 读取dtvs数据库t_filter_info,通过f_subtype_id，f_subtype_id，f_country_id找到筛选的内容类型，地区
    * 读取iusm数据库，通过f_region_id去找到区域信息
    *
    */
  def FilterDetail(detailsDF: DataFrame, sparkSession: SparkSession):Unit={
    val sqlContext = sparkSession.sqlContext
    val provincesql=
      s"""
         |(SELECT province_id as f_province_id,province_name as f_province_name
         |from province
          ) as aa
     """.stripMargin

    val citysql=
      s"""
         |(SELECT city_id as f_city_id,city_name as f_city_name
         |from city
          ) as aa
     """.stripMargin
    val regionsql=
      s"""
         |(SELECT area_id as f_region_id,area_name as f_region_name
         |from area
          ) as aa
     """.stripMargin

    val filtercontenttypesql =
      """(
        |select f_filterid as f_contenttype_id,f_filtername as  f_contenttype_name from t_filter_info) as content_filter
      """.stripMargin
    val filtersubtypesql =
      """(
        |select f_filterid as f_subtype_id,f_filtername as  f_subtype_name from t_filter_info) as subtype_filter
      """.stripMargin

    val filtercountrysql =
      """(
        |select f_filterid as f_country_id,f_filtername as  f_country_name from t_filter_info) as country_filter
      """.stripMargin
    val provinceDF = DBUtils.loadMysql(sqlContext, provincesql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val cityDF = DBUtils.loadMysql(sqlContext, citysql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val regionDF = DBUtils.loadMysql(sqlContext, regionsql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val filterContentDF = DBUtils.loadMysql(sqlContext,filtercontenttypesql,DBProperties.JDBC_URL_DTVS,DBProperties.USER_DTVS,DBProperties.PASSWORD_DTVS)
    val filterSubtypeDF = DBUtils.loadMysql(sqlContext,filtersubtypesql,DBProperties.JDBC_URL_DTVS,DBProperties.USER_DTVS,DBProperties.PASSWORD_DTVS)
    val filterCountryDF = DBUtils.loadMysql(sqlContext,filtercountrysql,DBProperties.JDBC_URL_DTVS,DBProperties.USER_DTVS,DBProperties.PASSWORD_DTVS)
    val results = detailsDF.join(provinceDF,Seq("f_province_id"))
      .join(cityDF,Seq("f_city_id")).join(regionDF,Seq("f_region_id"))
      .join(filterContentDF,detailsDF("f_contenttype")===filterContentDF("f_contenttype_id"),"left")
      .join(filterSubtypeDF,detailsDF("f_subtype")===filterSubtypeDF("f_subtype_id"),"left")
      .join(filterCountryDF,detailsDF("f_country")===filterCountryDF("f_country_id"),"left")
      .drop("f_contenttype_id").drop("f_subtype_id").drop("f_country_id")
    DBUtils.saveToHomedData_2(results,Tables.mysql_user_filter)
  }

  /**
    * 构造map
    * @param paras
    * @return
    */
  def parseMaps(paras: String): scala.collection.mutable.Map[String, String] = {
    import scala.collection.mutable.Map
    val map = Map[String, String]()
    val arr = paras.split(",")
    for (i <- arr) {
      val kv = i.split(":")
      if (kv.length == 2) {
        map += (kv(0) -> kv(1))
      } else {
      }
    }
    map
  }
}
