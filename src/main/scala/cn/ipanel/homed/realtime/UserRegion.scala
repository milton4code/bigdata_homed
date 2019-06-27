package cn.ipanel.homed.realtime

import cn.ipanel.common.{DBProperties, GatherType, SparkSession, Tables}
import cn.ipanel.utils.{DBUtils, DateUtils, RegionUtils}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @author lizhy@20190228
  */
object UserRegion {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("UserRegion")
    val sqlContext = sparkSession.sqlContext
    /*println("开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    updateRegionServiceType(sqlContext)
    println("结束："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))*/
    println("开始："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
    val userDf = getUserRegion(sqlContext)
    println("userDf count:" + userDf.count())
    userDf.show()
    DBUtils.saveDataFrameToPhoenixNew(userDf,Tables.T_USER_REGION)
    println("结束："+ DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
  }

  /**
    * 获取用户区域信息
    * @param sqlContext
    * @return
    */
  def getUserRegion(sqlContext: HiveContext) = {
    import sqlContext.implicits._
    val rootRegionId = RegionUtils.getRootRegion
    var filter_region = ""
    if (rootRegionId.endsWith("0000")) {
      filter_region = rootRegionId.substring(0, 2)
    } else {
      filter_region = rootRegionId.substring(0, 4)
    }
    val regionSql =
      s"""
         |(select cast(a.area_id as char) as f_area_id,a.area_name as f_region_name,
         |cast(a.city_id as char) as f_city_id,c.city_name as f_city_name,
         |cast(c.province_id as char) as f_province_id,p.province_name as f_province_name
         |from (${Tables.T_AREA} a left join ${Tables.T_CITY} c on a.city_id=c.city_id)
         |left join ${Tables.T_PROVINCE} p on c.province_id=p.province_id
         |) as region
        """.stripMargin
    val regionDf = DBUtils.loadMysql(sqlContext, regionSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val userReiondSql =
      s"""
         |(select cast(ac.DA as char) AS f_user_id,CAST(ad.region_id AS CHAR) AS f_region_id
         |from ${Tables.T_ACCOUNT_INFO} ac,${Tables.T_ADDRESS_INFO} ad
         |where ac.home_id = ad.home_id
         |AND ac.status='1' AND ad.status = '1') as user_region
       """.stripMargin
    val userReiondDf = DBUtils.loadMysql(sqlContext, userReiondSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
      //.filter($"f_region_id".startsWith(filter_region))

     userReiondDf.join(regionDf, userReiondDf("f_region_id") === regionDf("f_area_id"))
      .selectExpr("f_user_id","f_province_id","f_province_name","f_city_id",
        "f_city_name","f_region_id","f_region_name")
  }
  /**
    * 获取默认区域码
    * @return
    */
  def getDefaultRegionId(): String = {
    var regionId = RegionUtils.getRootRegion
    if (regionId.endsWith("0000")) {
      regionId = (regionId.toInt + 101).toString
    } else {
      regionId = (regionId.toInt + 1).toString
    }
    println("默认区域 - " + regionId)
    regionId
  }

  def byArea(netss:String, nextDay:String, sqlContext:SQLContext) {

    val sda = s"(SELECT DA,home_id,status,f_reg_source,date_format(create_time,'%Y%m%d %H:%i:%s') as create_time,date_format(f_status_update_time," +
      s"'%Y%m%d %H:%i:%s') as f_status_update_time " +
      s"from account_info WHERE create_time< $nextDay ) as t_account"
  }
  /**
    * 加载区域信息
    *
    * @param sparkSession
    */
  def loadRegionInfoMap(sparkSession: SparkSession): mutable.HashMap[String, (String, String, String,String,String)] = {
    val sqlContext = sparkSession.sqlContext
    val map = new mutable.HashMap[String, (String, String, String,String, String)]()
    //项目部署地code(省或者地市)
//    val regionCode = RegionUtils.getRootRegion
//    val regionBC: Broadcast[String] = sparkSession.sparkContext.broadcast(regionCode)
    //区域信息表
    val region =
      s"""
         |(select cast(a.area_id as char) as f_area_id,a.area_name as f_region_name,
         |cast(a.city_id as char) as f_city_id,c.city_name as f_city_name,
         |cast(c.province_id as char) as f_province_id,p.province_name as f_province_name
         |from (area a left join city c on a.city_id=c.city_id)
         |left join province p on c.province_id=p.province_id
         |) as region
        """.stripMargin
    val regionDF = DBUtils.loadMysql(sqlContext, region, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    regionDF.collect.foreach(row => {
      val regionCode = row.getAs[String]("f_area_id")
      val regionName = row.getAs[String]("f_region_name")
      val cityCode = row.getAs[String]("f_city_id")
      val cityName = row.getAs[String]("f_city_name")
      val provCode = row.getAs[String]("f_province_id")
      val provName = row.getAs[String]("f_province_name")
      map += (regionCode -> (provCode,provName,cityCode, cityName,regionName))
    })
    map
  }
  /**
    * 更新区域终端统计业务类型配置表
    * @param sqlContext
    */
  /*def updateRegionServiceType(sqlContext:HiveContext): Unit ={
    import sqlContext.implicits._
    val backSql = s"(select * from ${Tables.T_REGION_TERMINAL_SERVICE_TYPE} ) as bk"
    val backupRegServTypeDf = DBUtils.loadMysql(sqlContext, backSql, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD).cache()
    val serviceTypeList = ListBuffer((GatherType.LOOK_BACK_NEW),GatherType.LIVE_NEW,GatherType.DEMAND_NEW,"other") //统计业务类型
    val terminalList = ListBuffer((1),(3),(4),(5)) //终端类型
    val regionSql =
      s"""
         |(select cast(a.area_id as char) as f_area_id,a.area_name as f_region_name,
         |cast(a.city_id as char) as f_city_id,c.city_name as f_city_name,
         |cast(c.province_id as char) as f_province_id,p.province_name as f_province_name
         |from (${Tables.T_AREA} a left join ${Tables.T_CITY} c on a.city_id=c.city_id)
         |left join ${Tables.T_PROVINCE} p on c.province_id=p.province_id
         |) as region
        """.stripMargin
    val configSql =
      """
        |(SELECT f_code FROM `t_area`
        |WHERE f_id != f_parent_id) as config
      """.stripMargin
    val regionDf = DBUtils.loadMysql(sqlContext, regionSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
    val configRegionDf = DBUtils.loadMysql(sqlContext, configSql, DBProperties.JDBC_URL_BIGDATA_NEW_PROFILE, DBProperties.USER_BIGDATA_NEW_PROFILE, DBProperties.PASSWORD_BIGDATA_NEW_PROFILE)
    val updateDf = regionDf.join(configRegionDf,configRegionDf("f_code")===regionDf("f_area_id"),"inner")
      .map(x => {
        val list =new  ListBuffer[(String,String,String,String,String,String,Int,String)] //
        val provinceId = x.getAs[String]("f_province_id")
        val provinceName = x.getAs[String]("f_province_name")
        val cityId = x.getAs[String]("f_city_id")
        val cityName = x.getAs[String]("f_city_name")
        val regionId = x.getAs[String]("f_area_id")
        val regionName = x.getAs[String]("f_region_name")
        serviceTypeList.foreach(serviceType =>{
          terminalList.foreach(terminal => {
            list += ((provinceId,provinceName,cityId,cityName,regionId,regionName,terminal,serviceType))
          })
        })
        list.toList
      }).flatMap(x => x).toDF()
      .withColumnRenamed("_1","f_province_id")
      .withColumnRenamed("_2","f_province_name")
      .withColumnRenamed("_3","f_city_id")
      .withColumnRenamed("_4","f_city_name")
      .withColumnRenamed("_5","f_region_id")
      .withColumnRenamed("_6","f_region_name")
      .withColumnRenamed("_7","f_terminal")
      .withColumnRenamed("_8","f_service_type")
    val deleteSql = s"delete from ${Tables.T_REGION_TERMINAL_SERVICE_TYPE}"
    var deleteFlag = 0
    try{
      DBUtils.executeSql(DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD,deleteSql) //清空表
      deleteFlag = 1
      println("成功清空表")
    }catch {
      case e:Exception =>{
        println("清空表失败")
        e.printStackTrace()
        deleteFlag = 0
      }
    }
    try{
      DBUtils.saveToMysql(updateDf, Tables.T_REGION_TERMINAL_SERVICE_TYPE, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD) //保存点播节目统计信息
    }catch {
      case e:Exception =>{
        println("区域终端统计业务类型配置更新失败")
        if(deleteFlag == 1){
          println("插入备份数据:" + DateUtils.getNowDate(DateUtils.YYYY_MM_DD_HHMMSS))
          DBUtils.saveToMysql(backupRegServTypeDf, Tables.T_REGION_TERMINAL_SERVICE_TYPE, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD) //插入备份数据
        }
        e.printStackTrace()
      }
    }
  }*/
  /**
    * 加载区域信息
    *
    * @param sqlContext
    */
  def getRegionDf(sqlContext:HiveContext) ={
    //项目部署地code(省或者地市)
    val regionCode = RegionUtils.getRootRegion
    //区域信息表
    val region =
    s"""
       |(select cast(a.area_id as char) as f_area_id,a.area_name as f_region_name,
       |cast(a.city_id as char) as f_city_id,c.city_name as f_city_name,
       |cast(c.province_id as char) as f_province_id,p.province_name as f_province_name
       |from (area a left join city c on a.city_id=c.city_id)
       |left join province p on c.province_id=p.province_id
       |where a.city_id=${regionCode} or c.province_id=${regionCode}
       |) as region
        """.stripMargin
    DBUtils.loadMysql(sqlContext, region, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
  }
}
