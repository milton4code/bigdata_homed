package cn.ipanel.etl

import cn.ipanel.common.{DBProperties, SparkSession}
import cn.ipanel.utils.DBUtils
import org.apache.spark.sql.hive.HiveContext

object CaGetDa {

   def fromCaGetDa(sqlContext:HiveContext,partionNums:Int)={
     val deviceCa =  fromCaGetDeviceId(sqlContext: HiveContext)
     val deviceDa = fromDeviceIdGetDa(sqlContext: HiveContext)
     deviceCa.registerTempTable("deviceCa")
     deviceDa.registerTempTable("deviceDa")
     sqlContext.sql(
       """
         |select b.deviceid,b.DA,a.ca_id
         |from
         |deviceCa a join deviceDa b
         |on
         |a. deviceid = b. deviceid
       """.stripMargin).repartition(partionNums).registerTempTable("t_saved")
     sqlContext.sql(s"use bigdata")
     sqlContext.sql(
       s"""
          |insert overwrite  table bigdata.t_ca_device
          |select * from t_saved
        """.stripMargin)
   }


  def fromDeviceIdGetDa(sqlContext: HiveContext) = {
    val dc_sql =
      """( select device_id as  deviceid,max(DA) as DA
        |from device_account
        |where device_id != 0
        |group by device_id
        | ) as d""".stripMargin
    val dc_df = DBUtils.loadMysql(sqlContext, dc_sql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()
    dc_df
  }


  def fromCaGetDeviceId(sqlContext: HiveContext) = {
    val dc_sql =
      """( select device_id as deviceid,
        |(case device_type when 1 then stb_id
        |when 2 then cai_id
        |when 3 then mobile_id
        |when 4 then pad_id
        |when 5 then mac_address else 'unknown'   end) as ca_id
        |from homed_iusm.device_info where status=1 ) as d""".stripMargin
    val dc_df = DBUtils.loadMysql(sqlContext, dc_sql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM).distinct()
    dc_df
  }

}
