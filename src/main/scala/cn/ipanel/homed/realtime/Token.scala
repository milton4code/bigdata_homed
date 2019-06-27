package cn.ipanel.homed.realtime

import java.lang.{Long => JavaLong}

import cn.ipanel.common.Constant
import com.mysql.jdbc.StringUtils


/**
  *
  * token解析器
  *
  * @author liujjy
  * @date 2018/02/07 15:32
  */

object Token {


  def parserobject(tokenString: String): User = {
//
    if(check(tokenString)){
      val deviceType = getDeviceType(findStrByKey(tokenString,"U","K"))
      val deviceId = JavaLong.valueOf(findStrByKey(tokenString,"K","I"), 16)
      val userId = JavaLong.valueOf(findStrByKey(tokenString,"M","V"), 16)
      val regionIdStr = JavaLong.valueOf(findStrByKey(tokenString,"Z","W"),16).toString
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
    !StringUtils.isNullOrEmpty(token)  &&
      token.contains("U")  &&
      token.contains("K") && token.contains("I") &&
      token.contains("M") && token.contains("V") &&
      token.contains("Z")  && token.contains("W")
  }

  private def getDeviceType(deviceString: String): String = {
    var _deviceTypeBuffer = new StringBuilder(Integer.toBinaryString(Integer.valueOf(deviceString, 16)))
    if (_deviceTypeBuffer.length < 32) {
      for (i <- 1 to 32 - _deviceTypeBuffer.length) { //不足32位，高位补0
        _deviceTypeBuffer.insert(0,0)
      }
    }
    val deviceType = Integer.valueOf(_deviceTypeBuffer.substring(0, 4)).intValue().toString
//    match {
//      case 1 => "stb"
//      case 2 => "smartcard"
//      case 3 => "mobile"
//      case 4 => "pad"
//      case 5 => "pc"
//      case _ => "unknown"
//    }
    deviceType
  }

  def main(args: Array[String]): Unit = {
//    val token = "R5A97C6A2U10987023K3BA040FDI7C11A8C0P8M34C01CAV10401ZFFFFFFFFFFFFFFFFWDD2C8BF1821"
    val errorToken = "R5A97C6A2U10987023K3BA040FDI7C11A8C0P8M34C01CAV10401DD2C8BF1821"
    val token2 = "R5AF2C8EFU10957023K3BEC90EBI310FA8C0P8M2FFAFA6V10402Z6B7EFW11EB9DD37B7F1"
   val start = System.currentTimeMillis();
    for(i <- 0 to 10000000){
      parserobject(token2)
    }
//    println(  System.currentTimeMillis() -start)
    println(parserobject(token2))
////    val st = JavaLong.valueOf(findStrByKey(tokenString,"Z","W"),16)
////    println(st)
//    println(JavaLong.valueOf("FFFFFF",16))

  }

  case class User(DA:Long=0,device_id:Long=0,device_type:String="",province_id:String="",city_id:String="",region_id:String="")
}





