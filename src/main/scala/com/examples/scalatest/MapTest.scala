package com.examples.scalatest

import  java.util.{HashMap=>javaMap}
object MapTest {

  def main(args: Array[String]): Unit = {
    println("ssss".concat(",aaaa"))
  }

  def testJavaMaptoScalaMap(): Unit = {
    import scala.collection.JavaConversions._
    var map = new  javaMap[String, AnyRef]()
    map.put("name", "zhangsan")
    map.put("sex", "male")
    map.put("age", "23")
//    map += ("ciyt" -> "beijing")

    for (v <- map.keySet) println(v + "====" + map(v))

  }
  def testMapScala(): Unit = {
    val map = Map("name" -> "zhangsan", "sex" -> "man", "age" -> 23)
    val map2 = scala.collection.mutable.Map("name" -> "li", "sex" -> "women", "age" -> 18)
    map2("name") = "lisimeimei"
    map2 += ("city" -> "beijing", "nickname" -> "wangshisuuifeng")

    map2 += ""->""

    for (v <- map2.keySet) println(v + "====" + map2(v))

    println(map2.keys.head)


    println("=============================")
    //map的keyvalue反转
    val map3 = for ((k, v) <- map2) yield (v, k)
    for (v <- map3.keySet) println(v + "-====" + map3(v))
  }
}
