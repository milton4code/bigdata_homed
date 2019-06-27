package com.examples

import cn.ipanel.common.SparkSession

/**
  *
  *
  * @author liujjy  
  * @date 2018/01/24 08:48
  */

object ScalaTest extends App {
  val terminalType = new StringBuilder()
  val deviceNo = "041010118010159110"
  val venderCode = deviceNo.slice(0, 2)
  val deviceType = deviceNo.slice(2, 3)
  terminalType.append(deviceType).append("-").append("Hi3798MV200").append("-").append(deviceNo.slice(3, 4))

  println(terminalType.toString())
}


