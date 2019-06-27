package cn.ipanel.etl

import cn.ipanel.common.{GatherType, SparkSession}
import cn.ipanel.utils.{LogUtils, RegionUtils}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext

/**
  * 从用户上报日志导入用户上报的实际开机用户数据
  */
object UserExport {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("请输入开始日期,结束日期，分区数 例如 20190101,20190102,20")
      sys.exit(-1)
    }
    val sparkSession = SparkSession("UserExport")
    val hiveContext = sparkSession.sqlContext
    val sc=sparkSession.sparkContext
    val startDay = args(0)
    val endDay = args(1)
    val partitions = args(2).toInt
    process(startDay,endDay,partitions,hiveContext)

//    sc.stop()
  }

  def process(startDay:String,endDay:String,partitions:Int=20, hiveContext: HiveContext) = {
    val time = new LogUtils.Stopwatch("UserExport 用户上报导入数据")

    println("start---" + startDay)
    println("endDay---" + endDay)

    //province_id, province_name,city_id,city_name,area_id as region_id,area_name region_name
    val regionDF = RegionUtils.getRegions(hiveContext)
//    regionDF.show()
    val userDF = hiveContext.sql(
      s"""
         |select userid user_id,regionid region_id,deviceid device_id,devicetype device_type,
         |reporttime report_time ,ca as uniq_id
         |from  bigdata.orc_report_behavior
         |where day >= '$startDay' and day <='$endDay'   and reporttype='${GatherType.SYSTEM_OPEN}'
              """.stripMargin)
      .dropDuplicates(Seq("device_id", "user_id", "uniq_id"))

    hiveContext.sql("use user")
    userDF.join(regionDF, Seq("region_id"))
      .selectExpr("report_time", "user_id", "device_id", "device_type"
        , "province_id", "province_name", "city_id", "city_name", "region_id", "region_name", "uniq_id", s"'$startDay' as day")
      .dropDuplicates(Seq("user_id", "device_id"))
      .repartition(partitions)
      .write.format("orc").mode(SaveMode.Overwrite).partitionBy("day")
      .insertInto("t_user")

    print(time)
  }
}
