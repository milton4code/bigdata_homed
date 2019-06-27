package cn.ipanel.homed.general

import cn.ipanel.common._
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.sql.DataFrame

/**
  * 媒资库存（1点播，2回看）
  *
  * @author ZouBo
  * @date 2017/12/27 0027 
  */

object MediaRepertory {

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("参数异常,格式[date]")
      System.exit(1)
    } else {
      val dateTime = args(0)

      val sparkSession = SparkSession("MediaRepertory")


      val endTime = DateUtils.getAfterDay(dateTime)
      // 统计点播库存
      onDemondProgram(sparkSession, dateTime, endTime)

      // 统计回看库存
      lookBackProgram(sparkSession, dateTime, endTime)


      sparkSession.stop()
    }

  }

  /**
    * 点播库存
    *
    * @param sparkSession
    * @param dateTime 统计日期
    * @param endTime  截止时间
    */
  def onDemondProgram(sparkSession: SparkSession, dateTime: String, endTime: String) = {
    val sqlContext = sparkSession.sqlContext

    val mediaOnDemond = UserActionType.MEDIA_ON_DEMOND.toInt

    val sql =
      s"""(select '$dateTime' as f_date, sum(a.video_time) as f_video_time,sum(a.cnt) as f_video_count ,$mediaOnDemond as f_type
         |from (
         |select TIME_TO_SEC(video_time) AS video_time,1 as cnt
         |from  video_info
         |where f_upload_complete_time <='$endTime'
         |) a ) as t_user_video
      """.stripMargin

    val videoInfoDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    saveMediaRepertoryToMysql(videoInfoDF)

  }

  /**
    * 回看库存
    *
    * @param sparkSession
    * @param dateTime 统计日期
    * @param endTime  截止时间
    */
  def lookBackProgram(sparkSession: SparkSession, dateTime: String, endTime: String) = {
    val sqlContext = sparkSession.sqlContext
    val mediaLookBack = UserActionType.MEDIA_LOOK_BACK

    val sql =
      s"""(select '$dateTime' as f_date,sum(a.duration) as f_video_time,sum(a.cnt) as f_video_count,$mediaLookBack as f_type
         |from(
         |select TIME_TO_SEC(duration) AS duration,1 as cnt
         |from homed_eit_schedule_history
         |where start_time<='$endTime'
         |) as a ) as t_user_video
      """.stripMargin

    val lookBackInfoDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    saveMediaRepertoryToMysql(lookBackInfoDF)
  }

  def saveMediaRepertoryToMysql(dataFrame: DataFrame) = {
    DBUtils.saveToHomedData_2(dataFrame, Tables.t_media_repertory)
  }

}
