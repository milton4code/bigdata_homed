package cn.ipanel.common

import java.lang.{Long => JavaLong}

import cn.ipanel.utils.RegionUtils
import com.mysql.jdbc.StringUtils


/**
  *
  * token解析器
  *
  * @author liujjy
  * @date 2018/02/07 15:32
  */
case class User2(DA: String="", device_id: Long = 0, device_type: String = "", province_id: String = "", city_id: String = "", region_id: String = "")
case class User(DA: Long = 0, device_id: Long = 0, device_type: String = "", province_id: String = "", city_id: String = "", region_id: String = "")

object TokenParser {

  /**
    * 返回user_id 和deviceId 的解析方法
    *
    * @param tokenString
    * @return
    */
  def parser(tokenString: String): String = {
    if (check(tokenString)) {
      val deviceId = JavaLong.valueOf(findStrByKey(tokenString, "K", "I"), 16)
      val userId = JavaLong.valueOf(findStrByKey(tokenString, "M", "V"), 16)
      userId + "_" + deviceId
    } else {
      ""
    }
  }

  /**
    * 返回User对象的token的解析方法
    *
    * @param tokenString
    * @return
    */
  def parserAsUser(tokenString: String): User2 = {
    if (check(tokenString)) {
      val deviceType = getDeviceNum(findStrByKey(tokenString, "U", "K"))
      val deviceId = JavaLong.valueOf(findStrByKey(tokenString, "K", "I"), 16)
      val userId = JavaLong.valueOf(findStrByKey(tokenString, "M", "V"), 16).toString

      var regionIdStr = ""

      var proviceId = ""
      var cityId = ""
      var regionId = ""
      try {
        if (tokenString.indexOf("I") > -1) {
          regionIdStr = JavaLong.valueOf(findStrByKey(tokenString, "Z", "W"), 16).toString
        }

        proviceId = regionIdStr.substring(0, 2) +"0000"
        cityId = regionIdStr.substring(0, 4) + "00"
        regionId = regionIdStr.substring(0, 6)

      } catch {
        case ex: Exception => {
          //异常,就赋默认值
          regionIdStr = RegionUtils.getRootRegion
          if(regionIdStr.endsWith("0000")){
            proviceId = regionIdStr
            cityId = regionIdStr.substring(0, 2) + "01"
            regionId = regionIdStr.substring(0, 2) + "0101"
          } else{
            proviceId = regionIdStr.substring(0, 2) +"0000"
            cityId = regionIdStr
            regionId = regionIdStr.substring(0, 4) + "01"
          }
        }
      }

      User2(userId, deviceId, deviceType, proviceId, cityId, regionId)
    } else {
      User2()
    }

  }

  def parserAsUserNew(tokenString: String): User = {
    try {
      if (check(tokenString)) {
        val deviceType = getDeviceNum(findStrByKeyNew(tokenString, "U", "K"))
        val deviceId = JavaLong.valueOf(findStrByKeyNew(tokenString, "K", "I"), 16)
        val userId = JavaLong.valueOf(findStrByKeyNew(tokenString, "M", "V"), 16)

        var regionIdStr = ""
        try {
          if (tokenString.indexOf("I") > -1) {
            regionIdStr = JavaLong.valueOf(findStrByKey(tokenString, "Z", "W"), 16).toString
          }
        } catch {
          case ex: Exception => {
            regionIdStr = RegionUtils.getRootRegion
            ex.printStackTrace()
          }
        }

        val proviceId = regionIdStr.substring(0, 2) + "0000"
        val cityId = regionIdStr.substring(0, 4) + "00"
        val regionId = regionIdStr.substring(0, 6)

        User(userId, deviceId, deviceType, proviceId, cityId, regionId)
      } else if (checkNew(tokenString)) { //数据缺少区域信息 添加默认信息
        val deviceType = getDeviceNum(findStrByKeyNew(tokenString, "U", "K"))
        val deviceId = JavaLong.valueOf(findStrByKeyNew(tokenString, "K", "I"), 16)
        val userId = JavaLong.valueOf(findStrByKeyNew(tokenString, "M", "W"), 16)

        val regionCode = RegionUtils.getRootRegion
        var provinceId = ""
        var cityId = ""
        var regionId = ""
        if (regionCode.endsWith("0000")) { //省
          provinceId = regionCode
          cityId = (regionCode.toInt + 100).toString
          regionId = (regionCode.toInt + 101).toString
        } else {
          // 市
          provinceId = regionCode.substring(0, 2) + "0000"
          cityId = regionCode
          regionId = (regionCode.toInt + 1).toString
        }
        User(userId, deviceId, deviceType, provinceId, cityId, regionId)
      } else {
        User()
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        User()
      }
    }

  }

  def getRegionString(token: String): String = {
    var regionString = Constant.EMPTY_CODE
    try {
      //主要是为了解决token中非法数
      if (check(token)) {
        regionString = JavaLong.valueOf(findStrByKey(token, "Z", "W"), 16).toString
      }
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
    regionString
  }


  private def findStrByKey(str: String, start: String, end: String): String = {
    if (!StringUtils.isNullOrEmpty(end)) {
      str.substring(str.indexOf(start) + 1, str.indexOf(end))
    } else {
      str.substring(str.indexOf(start))
    }
  }

  private def findStrByKeyNew(str: String, start: String, end: String): String = {
    if (!StringUtils.isNullOrEmpty(end) && str.length > str.indexOf(start) + 1) {
      str.substring(str.indexOf(start) + 1, str.indexOf(end))
    } else {
      str.substring(str.indexOf(start))
    }
  }

  private def check(token: String): Boolean = {
    !StringUtils.isNullOrEmpty(token) &&
      token.contains("U") &&
      token.contains("K") && token.contains("I") &&
      token.contains("M") && token.contains("V") &&
      token.contains("Z") && token.contains("W")
  }

  private def checkNew(token: String): Boolean = {
    !StringUtils.isNullOrEmpty(token) &&
      token.contains("U") &&
      token.contains("K") && token.contains("I") &&
      token.contains("M") && token.contains("W") &&
      !token.contains("V") && !token.contains("Z")
  }

  /**
    * 获取设备类型数字代码
    *
    * @param deviceString
    * @return
    */
  private def getDeviceNum(deviceString: String): String = {
    deviceString.substring(0, 1)
  }

  def getUserIdAndTerminalType(token: String) = {
    val userId = JavaLong.valueOf(findStrByKey(token, "M", "V"), 16)
    val terminalStr = findStrByKey(token, "U", "K")
    val terminal = terminalStr.substring(0, 1).toInt match {
      case 1 => 1
      case 2 => 2
      case 3 => 3
      case 4 => 4
      case 5 => 5
      case _ => 0
    }
    (userId, terminal)
  }

  /**
    * 获取设备类型英文代码
    *
    * @param deviceString
    * @return
    */
  private def getDeviceType(deviceString: String): String = {
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

  def tokenParser(tokenString: String): String = {
    if (check(tokenString)) {
      val f_terminal = getDeviceType(findStrByKey(tokenString, "U", "K"))
      val f_deviceid = JavaLong.valueOf(findStrByKey(tokenString, "K", "I"), 16)
      val f_userid = JavaLong.valueOf(findStrByKey(tokenString, "M", "V"), 16)
      val regionIdStr = JavaLong.valueOf(findStrByKey(tokenString, "Z", "W"), 16).toString
      val f_province_id = regionIdStr.substring(0, 2) + "0000"
      val f_city_id = regionIdStr.substring(0, 4) + "00"
      val f_region_id = regionIdStr.substring(0, 6) //
      //      userId + "_" + deviceId
      f_userid + "_" + f_terminal + "_" + f_province_id + "_" + f_city_id + "_" + f_region_id
    } else {
      ""
    }
  }

  def main(args: Array[String]): Unit = {
    val token = "R5ACD5E4BU10940023K3B9AD61DI611AA8C0P8M2FAF295V10402Z6B7EFWDDAF63351B1"
    println(parser(token))

  }

}





