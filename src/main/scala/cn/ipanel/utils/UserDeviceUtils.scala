package cn.ipanel.utils

import cn.ipanel.common.{DBProperties, Tables}
import org.apache.spark.sql.hive.HiveContext

/**
  * 用户/设备/家庭等相关公共查询工具类
  * by lizhy@20190510
  */
object UserDeviceUtils {
  /**
    * 获取设备号信息
    * @param sqlContext
    */
  def getDeviceNumDf(sqlContext: HiveContext)={
    val deviceSql =
      s"""
         |(select dv.device_id,
         |CASE WHEN dv.device_type = 1 THEN dv.stb_id
         | WHEN dv.device_type = 2 THEN dv.cai_id
         | WHEN dv.device_type = 3 THEN dv.mobile_id
         | WHEN dv.device_type = 4 THEN dv.pad_id
         | WHEN dv.device_type = 5 THEN dv.mac_address else 'unknown' end as device_num
         |from ${Tables.T_DEVICE_INFO} dv where status = 1) as device
       """.stripMargin
    DBUtils.loadMysql(sqlContext, deviceSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
  }
}
