package cn.ipanel.common

import cn.ipanel.utils.{DBUtils, RegionUtils}
import org.apache.spark.sql.hive.HiveContext

object GetAddressInfo {

  def getRegionInfo(hiveContext: HiveContext) = {
    var regionCode=RegionUtils.getRootRegion
    if(regionCode.endsWith("0000")){
      regionCode=(regionCode.toInt+101).toString
    }else{
      regionCode=(regionCode.toInt+1).toString
    }
    val sql=
      //status 1为有效状态 9为无效状态
      s"""
         |(SELECT address_id,address_name as f_address_name,home_id as f_home_id,
         |province_id as f_province_id,city_id as f_city_id,
         | if(length(cast(region_id as char))=6,region_id,${regionCode.toInt}) as f_region_id
         |from address_info where status=1
          ) as aa
     """.stripMargin
    val addressInfoDF=DBUtils.loadMysql(hiveContext,sql, DBProperties.JDBC_URL_IUSM,DBProperties.USER_IUSM,DBProperties.PASSWORD_IUSM)
    val sql1=
      s"""
         |(SELECT province_id as f_province_id,province_name as f_province_name
         |from province
          ) as aa
     """.stripMargin
    val provinceInfoDF=DBUtils.loadMysql(hiveContext,sql1, DBProperties.JDBC_URL_IUSM,DBProperties.USER_IUSM,DBProperties.PASSWORD_IUSM)
    val sql2=
      s"""
         |(SELECT city_id as f_city_id,city_name as f_city_name
         |from city
          ) as aa
     """.stripMargin
    val cityInfoDF2=DBUtils.loadMysql(hiveContext,sql2,DBProperties.JDBC_URL_IUSM,DBProperties.USER_IUSM,DBProperties.PASSWORD_IUSM)
    val sql3=
      s"""
         |(SELECT area_id as f_region_id,area_name as f_region_name
         |from area
          ) as aa
     """.stripMargin

    var filterCode =RegionUtils.getRootRegion
    val areaInfoDF3=DBUtils.loadMysql(hiveContext,sql3,DBProperties.JDBC_URL_IUSM,DBProperties.USER_IUSM,DBProperties.PASSWORD_IUSM)
    val result = addressInfoDF.join(provinceInfoDF,Seq("f_province_id"),"left")
      .join(cityInfoDF2,Seq("f_city_id"),"left")
      .join(areaInfoDF3,Seq("f_region_id"),"left")
        .filter(s"f_province_id='$filterCode' or f_city_id= '$filterCode' ")
    result
  }

}
