package cn.ipanel.homed.realtime

import java.lang.{Long => JavaLong}

import cn.ipanel.common.Constant
import cn.ipanel.utils.RegionUtils
import com.mysql.jdbc.StringUtils


/**
  *
  * token解析器
  *
  * @author liujjy
  * @date 2018/02/07 15:32
  */

private[realtime] object TokenParser {


  def parser(tokenString: String): User = {
//
    if(check(tokenString)){
      val deviceType = getDeviceNum(findStrByKey(tokenString,"U","K"))
      val deviceId = JavaLong.valueOf(findStrByKey(tokenString,"K","I"), 16)
      val userId = JavaLong.valueOf(findStrByKey(tokenString,"M","V"), 16)

      var regionIdStr =""
      if(tokenString.indexOf("I") > -1){
         regionIdStr = JavaLong.valueOf(findStrByKey(tokenString,"Z","W"),16).toString
      }else{
        regionIdStr = RegionUtils.getRootRegion
      }
      val proviceId = regionIdStr.substring(0, 2) + "0000"
      val cityId = regionIdStr.substring(0, 4) + "00"
      val regionId = regionIdStr.substring(0, 6)

      User( userId,deviceId, deviceType, proviceId, cityId, regionId)
    }else{
      User()
    }

  }

  def getRegionString(token:String):String={
    var regionString = Constant.EMPTY_CODE
    try{//主要是为了解决token中非法数
      if(check(token)){
        regionString = JavaLong.valueOf(findStrByKey(token,"Z","W"),16).toString
      }
    }catch {
      case ex:Exception => ex.printStackTrace()
    }
    regionString
  }


  private def findStrByKey(str:String,start:String,end:String):String={
    if(!StringUtils.isNullOrEmpty(end)){
      str.substring(str.indexOf(start)+1,str.indexOf(end))}else{
      str.substring(str.indexOf(start))
    }
  }

  private def check(token: String): Boolean = {
    !(token.length < 40 || token.indexOf("R") == -1  || token.indexOf("M") == -1  || token.indexOf("W") == -1)
  }

  /**
    * 获取设备类型数字代码
    * @param deviceString
    * @return
    */
  private def getDeviceNum(deviceString: String):String={
     deviceString.substring(0,1)
  }

  /**
    * 获取设备类型英文代码
    * @param deviceString
    * @return
    */
  private def getDeviceType(deviceString: String): String = {
    val deviceType =  deviceString.substring(0,1).toInt
    match {
      case 1 => "stb"
      case 2 => "smartcard"
      case 3 => "mobile"
      case 4 => "pad"
      case 5 => "pc"
      case _ => "unknown"
    }
    deviceType
  }

  def main(args: Array[String]): Unit = {
//    val token = "R5A97C6A2U10987023K3BA040FDI7C11A8C0P8M34C01CAV10401ZFFFFFFFFFFFFFFFFWDD2C8BF1821"
    val errorToken = "R5A97C6A2U10987023K3BA040FDI7C11A8C0P8M34C01CAV10401DD2C8BF1821"
    val token2 = "R5AACD69DU319EB123K773B3C7EIB722A8C0P8M2FFB43EV10400Z61DD358560WDD5FF7F54D3"
    println(parser(errorToken))

  }

  case class User(DA:Long=0,device_id:Long=0,device_type:String="",province_id:String="",city_id:String="",region_id:String="")
}





