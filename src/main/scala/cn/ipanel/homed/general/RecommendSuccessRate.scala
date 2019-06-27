package cn.ipanel.homed.general

import cn.ipanel.common._
import cn.ipanel.utils.{DBUtils, DateUtils}
import org.apache.spark.rdd.RDD

/**
  * 推荐成功率
  *
  * @author ZouBo
  * @date 2017/12/26 0026 
  */
object RecommendSuccessRate {


  def main(args: Array[String]): Unit = {
    if (args.length != 2) {

      println("参数异常，格式[filePaht date]")
      System.exit(1)
    } else {
      val sparkSession = SparkSession("recommendSuccessRate")
      val sparkContext = sparkSession.sparkContext

      val filePath = args(0)
      val date = args(1)


      val lines = sparkContext.textFile(filePath).map(_.split(",")).filter(x => x(5).toLowerCase.equals(GatherType.RECOMMEND_TYPE) && x(6).toLowerCase.equals(GatherType.VOD_TYPE))
      calSuccessRate(lines, date, sparkSession)
    }
  }

  /**
    * 计算推荐成功率
    *
    * @param lines
    * @param date
    * @param sparkSession
    */
  def calSuccessRate(lines: RDD[Array[String]], date: String, sparkSession: SparkSession) = {

    import sparkSession.sqlContext.implicits._
    val sqlContext = sparkSession.sqlContext
    val sql = "(select distinct video_id ,f_series_id from video_info) as series "
    val videoInfoDF = DBUtils.loadMysql(sqlContext, sql, DBProperties.JDBC_URL_DTVS, DBProperties.USER_DTVS, DBProperties.PASSWORD_DTVS)
    val videoInfo = videoInfoDF.map(x => (x.getAs[Int]("video_id").toString, x.getAs[Int]("f_series_id").toString))
    val recommendRecord = lines.map(x => (x(4), x(3)))
    val recommendRecordCount = recommendRecord.count()


    val successCount = recommendRecord.leftOuterJoin(videoInfo).map(x => (x._2._1, x._2._2.getOrElse(""))).filter(x => x._1.contains(x._2)).count()
    val successRateDF = sqlContext.createDataset(Seq(RecommendSuccessRateRec(date, recommendRecordCount, successCount))).toDF()
    DBUtils.saveToMysql(successRateDF, Tables.t_recommend_success_rate, DBProperties.JDBC_URL, DBProperties.USER, DBProperties.PASSWORD)
  }

}
