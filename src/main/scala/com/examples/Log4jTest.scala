package com.examples

import org.apache.log4j.Logger

object Log4jTest extends App {
  Log4jPrintStream.redirectSystemOut()
  val logger= Logger.getLogger(this.getClass)

  logger.error("xxxxxxxxxxxxx")
  logger.warn("warn")
  println("aaaaaaa")
  println("accc")
 /* public void printLogger(){
    logger.error("直接输出吧");
    try {

    } catch (Exception e) {
      e.printStackTrace();
    }
  }*/
}
