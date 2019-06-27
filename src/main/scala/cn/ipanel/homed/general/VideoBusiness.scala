package cn.ipanel.homed.general

import cn.ipanel.common._
import cn.ipanel.utils.{DBUtils, DateUtils, LogTools}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * 视频业务
  * 入口参数：日期,数据源路径，是否删除当天数据
  * 日志格式：服务|服务|入口时间戳|出口时间戳|入口时间|用户ID|地区ID|设备类型|参考采集参数定义
  *
  * author liujjy
  * date 2017/12/26 14:14
  */

object VideoBusiness extends App {

  if (args.length != 3) {
    println("请输入日期 ,路径,删除标识")
    System.exit(1)
  } else {
      val day = args(0)
    val filePath = args(1)
    val isDel = args(2).toBoolean
//    val day = "20170307"
//    val filePath = "file:///D:\\tmp\\data\\20170307"
//    val isDel = "false".toBoolean
    val sparkSession = SparkSession("VedioBusiness", "local[*]")
    val sc = sparkSession.sparkContext

    if (isDel) {
      //ToDo 完善jdbcUtils
    }

    val lines = sc.textFile(filePath).map(_.split(","))
      .filter(arr => arr.length == 9 && !arr(5).toLowerCase.contains(GatherType.GUEST))

    process(lines, sparkSession)

  }

  /**
    * 处理逻辑
    */
  private def process(lines: RDD[Array[String]], session: SparkSession) = {
    val sqlContext = session.sqlContext
    import sqlContext.implicits._

    val logData = lines.filter(line => {
      //过滤直播101 时移102 回看103 点播104数据
      val datas = LogTools.parseMaps(line(8))
      val service = line(0)
      (service.contains(GatherType.LIVE) ||
        service.contains(GatherType.LOOK_BACK) ||
        service.contains(GatherType.TIME_SHIFT) ||
        service.contains(GatherType.DEMAND)) && datas.contains("A") && datas("A") == UserActionType.SWITCH_OR_PLAY
    })


    //服务|服务|入口时间戳|出口时间戳|入口时间|用户ID|地区ID|设备类型|参考采集参数定义
   // 各业务总访问量
    val totalAcessDF = totalAcess(logData).toDF()
    //单独业务访问量
      val separateAcessDF = separateAcess(totalAcessDF, sqlContext)
//     // 点播回看时移的用户数统计，以及那些用户使用过
     val userDetailStatisticsDF = userDetailStatistics(logData).toDF()

     DBUtils.saveToHomedData_2(totalAcessDF, "t_user_bus_access_terminal")
     DBUtils.saveToHomedData_2(separateAcessDF, "t_user_bus_access")
     DBUtils.saveToHomedData_2(userDetailStatisticsDF, "t_user_detail")

  }

  /**
    * 用户访问明细统计
    */
  private def userDetailStatistics(logData: RDD[Array[String]]) = {
    logData.map(x => {
      val service = x(0)
      val userid = x(5)
      var date = DateUtils.getYesterday()
      try {
        date = x(4).split(" ")(0).replace("-", "")
      } catch {
        case e: ArrayIndexOutOfBoundsException => e.printStackTrace()
          date = DateUtils.getYesterday()
      }
      val key = date + "," + service + "," + userid
      (key, 1)
    }).reduceByKey(_ + _)
      .filter(_._1.split(",").length == 3)
      .map(x => {
        val keyArr = x._1.split(",")
        val cnt = x._2
        UserDetailStatistics(keyArr(0), keyArr(1), keyArr(2), cnt)
      })
  }

  /**
    * 单独业务访问量
    */
  private def separateAcess(df: DataFrame, sqlContext: SQLContext) = {
    df.registerTempTable("t_totalAcess")
    sqlContext.sql(
      s"""select f_daytime,f_sevice_type,sum(f_access_cnt) as f_access_cnt
         |from t_totalAcess
         |group by f_sevice_type,f_daytime
         |""".stripMargin
    )
  }

  /**
    * 四种类型业务总访问量
    */
  private def totalAcess(logData: RDD[Array[String]]) = {
    logData.map(x =>
      (x(0) + "," + x(7) + "," + x(4).split(" ")(0).replace("-", "") /*+ ","
        + {
        if (x(6).length == 0) "0" else x(6)
      }*/,
        1)).reduceByKey(_ + _)
      .map(x => {
        val keys = x._1.split(",")
        val service = keys(0).trim
        val terminal = keys(1)
        val date = keys(2)
//        val region = keys(3)
        val access_cnt = x._2
       /* val business = service match {
          case GatherType.LIVE => "直播"
          case GatherType.LOOK_BACK => "回看"
          case GatherType.TIME_SHIFT => "时移"
          case GatherType.DEMAND => "点播"
        }*/
        AccessTotal(date, service, terminal, access_cnt)
      })
  }
}
