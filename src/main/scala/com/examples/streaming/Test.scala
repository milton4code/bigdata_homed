package com.examples.streaming

/**
  *
  * create  liujjy
  * time    2019-05-10 0010 16:23
  */
object Test {

  def main(args: Array[String]): Unit = {
    val len1 = "sss,sss,sa|xxx,ss".split("\\|").length
    val len2 = "sss,sss,sa,xxx,ss".split("\\|").length

    println(len1)
    println(len2)
  }

}
