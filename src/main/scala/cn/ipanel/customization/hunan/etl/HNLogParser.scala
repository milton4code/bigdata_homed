package cn.ipanel.customization.hunan.etl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  *湖南日志解析定制逻辑
  * create  liujjy
  * time    2019-05-27 0027 13:39
  */
object HNLogParser {
  /**
    * 处理OnlineTimePatch时,湖南需要过滤凤凰高清用户
    * @param userDeviceDF
    * @param base
    * @param sqlContext
    * @return
    */
  def getVaidateTimerDF(userDeviceDF: DataFrame, base: DataFrame, sqlContext: HiveContext,defaultRegion:String):DataFrame ={
    import sqlContext.implicits._
    sqlContext.udf.register("userid", (userid: String) => {
      var flag = true
      try {
        val _userid = userid.toLong
        if (_userid >= 50000000L && _userid <= 70000000L) { //正常DA取值范围
          flag = false
        }
      } catch {
        case ex: Exception => flag = true
      }
      flag
    })

    val userRegionDF = userDeviceDF .selectExpr("user_id", "region_id","device_id","device_type","uniq_id")

    base.filter("userid(userid)").show()
    val df2 = base.filter("userid(userid)")
    df2.alias("a")
      .join(userRegionDF.alias("c"), $"a.userid" === $"c.uniq_id", "left")
      .selectExpr("a.service", "a.timehep", "c.user_id userid", s"nvl(c.region_id,$defaultRegion) region",
        "nvl(c.device_type ,deviceType(a.deviceid)) terminal", "c.device_id deviceid")
  }


  /**
    * 获取有效播放行为用户
    *
    * @param day   yyyyMMdd
    * @param logDF  日志信息
    * @param sqlContext  sqlContext
    * @return
    */
  def getValidateVideoPlay(day: String, logDF: DataFrame, sqlContext: HiveContext):DataFrame = {

     // t.userid,t.deviceType,t.deviceid,t.regionid
//    val nextDay = DateUtils.dateStrToDateTime(day,DateUtils.YYYYMMDD).plusDays(1).toString(DateUtils.YYYY_MM_DD)
    val userDF = sqlContext.sql(
      s"""
        |select user_id as userid,device_id deviceid
        |from bigdata.orc_user
        |where day='$day'
      """.stripMargin)// .selectExpr("user_id as userid","device_id deviceid")

    logDF.join(userDF,Seq("userid","deviceid"))
  }

  /**
    * 过滤凤凰高清用户
    *
    * @param sqlContext
    * @param logDF
    * @param caDeviceDF
    * @return
    */
  def fiterHdUser(sqlContext: HiveContext, logDF: DataFrame, caDeviceDF: DataFrame) = {
    sqlContext.udf.register("userid", (userid: String) => {
      var flag = true
      try {
        val _userid = userid.toLong
        if (_userid >= 50000000 && _userid <= 70000000L) { //正常DA取值范围
          flag = false
        }
      } catch {
        case ex: Exception => flag = true
      }
      flag
    })
    import sqlContext.implicits._

    logDF.filter("userid(userid)").alias("a")
  }

}
