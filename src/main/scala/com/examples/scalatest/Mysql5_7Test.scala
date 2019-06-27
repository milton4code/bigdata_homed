package com.examples.scalatest

import cn.ipanel.common.SparkSession
import cn.ipanel.utils.DBUtils

object Mysql5_7Test {

  def main(args: Array[String]): Unit = {

    val sparkSession = new SparkSession("ss", "")
    val sqlContext = sparkSession.sqlContext
    val url = "jdbc:mysql://127.0.0.1:3306/sakila?useUnicode=true&characterEncoding=utf8"
    val user = "root"
    val passwrod = "root"
    val table ="actor"
    DBUtils.loadMysql(sqlContext,table,url,user,passwrod).show(200,false)

  }
}
