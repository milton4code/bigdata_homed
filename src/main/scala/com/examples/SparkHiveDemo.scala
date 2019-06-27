package com.examples

import cn.ipanel.common.CluserProperties
import cn.ipanel.utils.DBUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  *spark 读取hive 数据源
  *
  * @author liujjy  
  * @date 2018/01/04 19:13
  */

object SparkHiveDemo  {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\study\\hadoop\\winutils-master\\hadoop-2.6.4")
    val jobName = "SparkHiveDemo"
//   val master_url= args(0)
//   val master_url="spark://master:7077"
    val conf = new SparkConf()  .setMaster("local[2]")setAppName(jobName)
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)


    val json  = hiveContext.read.json("file:///D:\\ideaProjects\\homed\\bigdata_homed\\src\\main\\scala\\com\\examples\\dev.json")
    json.foreach(println(_))

//    hiveContext.sql("use bigdata")

//    val df = hiveContext.sql(s"select  unix_timestamp() as time, count(1) as count from userdata_1 where day=20180319")
//    DBUtils.saveDataFrameToPhoenixNew(df,"t_save_test")
//   DBUtils.saveDataFrameToPhoenixOld(df,"t_save_test")
//    df.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "t_save_test", "zkUrl" ->CluserProperties.ZOOKEEPER_URL))


  }


}
