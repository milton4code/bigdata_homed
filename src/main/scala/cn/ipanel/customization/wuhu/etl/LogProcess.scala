package cn.ipanel.customization.wuhu.etl

import cn.ipanel.customization.wuhu.users.UsersProcess
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel

object LogProcess {

  //只保留有效用户的播放行为
  def getEffVidPlayDf(date:String,sourceVideoPlayDf:DataFrame,sqlContext: SQLContext)={
    val effUserDf = UsersProcess.getEffectedUserDf(date,sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    effUserDf.join(sourceVideoPlayDf,sourceVideoPlayDf("userid")===effUserDf("da"),"inner")
      .selectExpr("userid","deviceType","deviceid","regionid","startTime","endTime1","endTime","playTime","column_id","playType","serviceid")
  }
  def getEffBehaviorDf(date:String,sourceBehaviorDf:DataFrame,sqlContext: SQLContext)={
//    userId: String, deviceId: Long, deviceType: String, reportType: String, reportTime: String, exts: scala.collection.Map[String, String]
    val effUserDf = UsersProcess.getEffectedUserDf(date,sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)
    effUserDf.join(sourceBehaviorDf,sourceBehaviorDf("userid")===effUserDf("da"),"inner")
      .selectExpr("userId","deviceId","deviceType","reportType","reportTime","exts")
  }
  def saveReportLogBehavior(date:String,sourceBehaviorDf:DataFrame,sqlContext: SQLContext):String ={
    sqlContext.sql("use bigdata")
    val behaviorTmpName = "t_wuhu_behavior"
    sourceBehaviorDf.registerTempTable("t_behavior")
    UsersProcess.getEffectedUserDf(date,sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER).registerTempTable("t_effect_user")
    sqlContext.sql(
      s"""
         |select tb.*
         |from t_behavior tb
         |inner join t_effect_user teu on tb.userid=teu.da
       """.stripMargin
    ).registerTempTable(behaviorTmpName)
    behaviorTmpName
  }
}
