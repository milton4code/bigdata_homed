package com.examples

import org.apache.commons.lang.StringUtils

/**
  *
  *
  * @author liujjy  
  * @date 2018/02/07 22:04
  */

object StringTest extends App {
  val str = "recommend/get_top_recommend?scc&accesstoken=R5A7ACDE1U30969123K773B3E5AI2D28A8C0P8M2FF8B1EV10400WDCE5D0A2511&label=37839"
  println(StringUtils.substringBetween(str,"accesstoken=","&"))
}
