package cn.ipanel.etl

import cn.ipanel.common.{GatherType, SparkSession, Tables}
import org.apache.spark.sql.hive.HiveContext

//处理日志延迟问题 包括日志上报延迟以及播放时间延迟问题
//数据第二步预处理
/**
  * 延迟日志补丁程序<p>
  *   1.过滤serviceId为空记录
  *   2.以用户为唯一分组条件,错位比较上一条开始时间和下一条的开始时间
  *   3.存数据到orc_video_play
  */
object LogDelayPatch {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("LogDelayPatch")
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    val date = args(0)
    val num = args(1).toInt
    processDelayLog(date, sqlContext,num)
    sc.stop()
  }

  def processDelayLog(day: String, sqlContext: HiveContext,partitions:Int) = {
    sqlContext.sql("use bigdata")
    val df = sqlContext.sql(
      s"""
         |select t.userid,t.deviceType,t.deviceid,t.regionid,
         |t.startTime,t.endTime1,
         |if((t.endTime>t.endTime1),t.endTime1,t.endTime) as endTime,
         |t.playTime,
         |t.ext['column_id']  as column_id,
         |t.playType,t.serviceid
         |from
         |(select *,
         |lead(startTime) over (partition  by userId,deviceId order by startTime) endTime1
         |from ${Tables.ORC_VIDEO_PLAY_TMP}
         |where day='$day') t
       """.stripMargin)
       df.distinct().coalesce(partitions).registerTempTable("t_tmp")

    sqlContext.sql(
      s"""
          |insert overwrite table ${Tables.ORC_VIDEO_PLAY} partition(day='$day')
          |select userid,deviceid,devicetype,
          |regionid,playtype,
          |nvl(cast(serviceid as string) ,'unknown') as serviceid,
          |starttime,endtime,
          |if((((unix_timestamp(endTime)-unix_timestamp(startTime)))>playtime),playtime,(unix_timestamp(endTime)-unix_timestamp(startTime))) as playtime,
          |str_to_map(concat_ws(":", "column_id", column_id)) as exts
          |from t_tmp
""".stripMargin)
    println("写入数据成功")
  }
}
