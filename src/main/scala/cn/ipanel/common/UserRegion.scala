package cn.ipanel.common

import cn.ipanel.utils.RegionUtils
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  *用户区域信息
  *
  * @author liujjy  
  * @date 2018/01/25 05:00
  */

object UserRegion {

  /**
    * 加载用户区域信息
    * @return DA|province_id|city_id|region_id
    */
  def loadUserRegion(sqlContext:SQLContext): DataFrame = {
    val regionTalbe =
      s"""(SELECT a.DA, cast( b.province_id as CHAR) as province_id ,
         | cast( b.city_id as CHAR)  city_id,
         | cast( b.region_id as CHAR)  region_id
         |from homed_iusm.account_info  a
         |INNER JOIN homed_iusm.address_info b
         |ON  a.home_id = b.home_id ) as region """
        .stripMargin

    val prop = new java.util.Properties
    prop.setProperty("user", DBProperties.USER_IUSM)
    prop.setProperty("password", DBProperties.PASSWORD_IUSM)

    val regionDF = sqlContext.read.jdbc(DBProperties.JDBC_URL_IUSM, regionTalbe,
      "DA", 1, 999999, 10, prop)
    regionDF
  }

  /**
    * 获取区域码
    * 获取的是默认区域码，如果区域码为省级，则改为省|01|01
    * 如果区域码为市级，则改为省|市|01
    */
  def getRegionCode(): String = {
    var regionCode = RegionUtils.getRootRegion
    println("regionCode=="+regionCode)
    if (regionCode.endsWith("0000")) {
      regionCode = (regionCode.toInt + 101).toString
    } else {
      regionCode = (regionCode.toInt + 1).toString
    }
    regionCode
  }
}
