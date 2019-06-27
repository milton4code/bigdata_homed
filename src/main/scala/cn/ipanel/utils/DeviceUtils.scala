package cn.ipanel.utils

import cn.ipanel.common.{CluserProperties, DBProperties, Tables}
import cn.ipanel.etl.LogConstant
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * 设备 相关工具类
  * create  liujjy
  * time    2019-05-04 0004 19:44
  */
object DeviceUtils {



  /**
    * 将设备类型转成数字
    *
    * @param deviceType 设备类型
    * @return 设备类型对应的数字
    */
  def deviceTypeToNum(deviceType: String): String = {
    deviceType.toLowerCase match {
      case "0" => LogConstant.TERMINAL
      case "机顶盒" => "1"
      case "stb" => "1"
      case "smartcard" => "2"
      case "ca" => "2"
      case "mobile" => "3"
      case "pad" => "4"
      case "pc" => "5"
      case "unknown" => LogConstant.TERMINAL
      case "" => LogConstant.TERMINAL
      case _ => deviceType
    }
  }
  /**
    * 设备ID映射为设备类型
    * @param deviceId 　设备ID
    * @return 设备类型 终端（0 其他, 1stb,2 CA卡,3mobile,4 pad, 5pc）
    */
  def deviceIdMapToDeviceType(deviceId: Long): String = {
    //    var deviceType = "unknown"
    var deviceType = CluserProperties.TERMINAL
    try {
      if (deviceId >= 1000000000l && deviceId <= 1199999999l) {
        deviceType = "1"
      } else if (deviceId >= 1400000000l && deviceId <= 1599999999l) {
        deviceType = "2"
      } else if (deviceId >= 1800000000l && deviceId < 1899999999l) {
        deviceType = "4"
      } else if (deviceId >= 2000000000l && deviceId <= 2999999999l) {
        deviceType = "3"
      } else if (deviceId >= 3000000000l && deviceId < 3999999999l) {
        deviceType = "5"
      }
    } catch {
        case ex: Exception =>  ex.printStackTrace()
    }
    deviceType
  }


  /**
    * Ca和Da对应关系
    * @param hiveContext
    * @param nextDay  YYYY-MM-dd 格式
    * @return device_id","uniq_id","home_id","address_id","device_type","user_id"
    */
  def getCaDaInfo(hiveContext: HiveContext,nextDay:String): DataFrame = {
   //device_id|user_id
    val devcieDA = getDeviceDaInfo(hiveContext)

    //device_id|uniq_id|home_id|address_id|device_type
    val deviceCA = getDeviceCaInfo(hiveContext,nextDay)

    devcieDA.join(deviceCA,Seq("device_id"))
      .selectExpr("device_id","uniq_id","home_id","address_id","device_type","user_id")
      .dropDuplicates(Seq("user_id","device_id"))
  }


  /**
    * 设备和DA之间的关系
    * <p> 此方法只能用于数据容错场景.也就是DA和设备ID二缺一方可调用此方法.
    * <p>一个设备可能绑定多个DA,在这里只取一个设备绑定最大da
    *
    * @return device_id|user_id
    */
  def getDeviceDaInfo(sqlContext: HiveContext): DataFrame = {

    val sql = "(SELECT device_id ,cast(max(DA) as char )  user_id  from device_account where device_id  group by device_id ) t"
    DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)

  }

  /**
    * 获取设备对应的唯一id信息
    *
    * @param nextDay YYYY-MM-dd 格式
    * @return device_id|uniq_id|home_id|address_id|device_type
    */
  def getDeviceCaInfo(sqlContext: HiveContext,nextDay:String): DataFrame = {
    val deviceSql =
      s"""
         |(select dv.device_id,
         |CASE WHEN dv.device_type = 1 THEN dv.stb_id
         | WHEN dv.device_type = 2 THEN dv.cai_id
         | WHEN dv.device_type = 3 THEN dv.mobile_id
         | WHEN dv.device_type = 4 THEN dv.pad_id
         | WHEN dv.device_type = 5 THEN dv.mac_address else 'unknown' end as uniq_id,
         | home_id,address_id,device_type
         |from ${Tables.T_DEVICE_INFO} dv
         |where status = 1 and f_create_time < '$nextDay') as device
       """.stripMargin
    DBUtils.loadMysql(sqlContext, deviceSql, DBProperties.JDBC_URL_IUSM, DBProperties.USER_IUSM, DBProperties.PASSWORD_IUSM)
  }


  /**
    * 获取设备类型英文代码
    */
  def getDeviceType(deviceString: String): String = {
    val deviceType = deviceString.substring(0, 1).toInt match {
      case 1 => "stb"
      case 2 => "smartcard"
      case 3 => "mobile"
      case 4 => "pad"
      case 5 => "pc"
      case _ => "unknown"
    }
    deviceType
  }

}
