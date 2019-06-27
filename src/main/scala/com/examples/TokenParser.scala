package com.examples


/**
  *
  *
  * @author liujjy  
  * @date 2018/02/07 14:09
  */

object TokenParser {

  /*
  R+当前UTC时间(32b)
U设备类型(4b)+平台(4b)+版本(4b)+随机数(8b)+应用入口(4b)+服务器IP(8b)
K+设备ID(32b)+I+登录IP(32b)
P+用户分组ID(16b)
M+账号ID(32b)
V+服务器版本号(32b)
Z+用于区域ID(64b)
W+索引(32b)

R5A7A92AC
U10972024
K3B9AF421I292BA8C0
P3DE2
M2FB4914
V10200
Z6B7EF
W11EA785701EC1
   */
  def main(args: Array[String]): Unit = {
//    val token = "R5A7A92ACU10972024K3B9AF421I292BA8C0P3DE2M2FB4914V10200Z6B7EFW11EA785701EC1"
    val token = "R59910D6CU10875023K3BA0432BIB1AA8C0P8M2FFB4BDWDAAB21C4AC"
//    val deviceTypeStr = Integer.toBinaryString(Integer.valueOf(token.substring(token.indexOf("U")+1,token.lastIndexOf("K")),16))
    val str = "10000100001110101000000100011"
    var deviceTypeStr =  new StringBuilder("10000100001110101000000100011")
    if(str.length < 32){
      for(i <- 0 until  32- str.length){
        deviceTypeStr.insert(0,"0")
      }
    }

    val dev = Integer.valueOf(deviceTypeStr.substring(0, 4)).intValue() match {
      case 1 => "stb"
      case 2 => "smartcard"
      case 3 => "mobile"
      case 4 => "pad"
      case 5 => "pc"
      case _ => "unknown"
    }
    println(dev)
//    println(Integer.valueOf(deviceTypeStr.substring(0,4)))
//    println(java.lang.Long.parseLong(token.substring(token.indexOf("K")+1,token.lastIndexOf("I")),16))
//    println(token.substring(token.indexOf("K")+1,token.lastIndexOf("I"),16))

  /*  println("ss =" + deviceTypeStr)
    println( " 设备类型 " +  deviceTypeStr)
    println( " 设备类型 " +  deviceTypeStr.reverse)
    println( " 设备ID " + Integer.valueOf(token.substring(token.indexOf("K")+1,token.lastIndexOf("I")),16))*/
//    println( " 账号ID " +  Integer.valueOf(token.substring(token.indexOf("M")+1,token.lastIndexOf("V")),16))
//    println( " 区域ID " +  Integer.valueOf(token.substring(token.indexOf("Z")+1,token.lastIndexOf("W")),16).toString)
  }
}
