package com.examples.scalatest

import scala.collection.mutable.ListBuffer

/**
  * https://blog.csdn.net/pzw_0612/article/details/47700593
  */
object ListTest {
  def main(args: Array[String]) {
  /*  val data = 1 to 20000000
    val currntTime =System.currentTimeMillis()
    increase_ListBuffer(data.toList)
    println("used time=" + (System.currentTimeMillis() - currntTime))
    val currntTime2 =System.currentTimeMillis()
    increase_for2(data.toList)
    println("used time=" + (System.currentTimeMillis() - currntTime2))*/

    var listBuffer = ListBuffer[String]()
    listBuffer += "aa"
    listBuffer += "aa"
    listBuffer += "aa"
    val listBuffer2 = ListBuffer[String]()
    listBuffer2 += "aa"
    listBuffer2 += "aa"
    listBuffer2 += "aa"
    println(listBuffer)
    println(listBuffer2)
  }

  //listBuffer
  def increase_ListBuffer(list:List[Int]) :List[Int]={
    println("listBuffer --")
    import scala.collection.mutable.ListBuffer
    var result = ListBuffer[Int]()
    for(element <- list){
      result += element+1
    }
    result.toList
  }


  def increase(list:List[Int]):List[Int] = list match {
    case List() => List()
    case head2 :: tail => (head2 + 1) :: increase(tail)
  }
  //循环 效率低
  def increase_for(list:List[Int]) :List[Int] = {
    var result = List[Int]()
    for(element <- list){
      result = result::: List(element)
    }
    result
  }

  //list 的map function
  def increase_for2(list:List[Int]) :List[Int] ={
    println("list map ")
    list map(el => el +1)
  }
}
