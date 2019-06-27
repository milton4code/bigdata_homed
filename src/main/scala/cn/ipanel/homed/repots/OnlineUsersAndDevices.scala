package cn.ipanel.homed.repots

import cn.ipanel.common.{GatherType, SparkSession}
import cn.ipanel.utils.{DBUtils, DateUtils, RegionUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * 在线用户数和在线设备数
  * 得到每半个小时点播 时移 回看 直播 的人数和设备id
  * 按天统计  直播 点播 回看 时移 人数和设备数
  */
@Deprecated
object OnlineUsersAndDevices {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession("OnlineUsersAndDevices")
    val sc = sparkSession.sparkContext
    val hiveContext = sparkSession.sqlContext
    val regionCode = RegionUtils.getRootRegion
    val regionInfo = Lookback.getRegionInfo(hiveContext, regionCode)
    val time = if (args.length == 1) args(0) else DateUtils.getYesterday()
    getDeviceUserCount(hiveContext, time, regionInfo)
    sparkSession.stop()
  }

  def getDeviceUserCount(hiveContext: HiveContext, time: String, regionInfo: DataFrame) = {
    import hiveContext.implicits._
    hiveContext.sql("use bigdata")
    val user_basic = hiveContext.sql(
      s"""
         |select
         |if(playType='${GatherType.DEMAND_NEW}',userid,null) as f_demand_userid,
         |if(playType='${GatherType.LOOK_BACK_NEW}',userid,null) as f_lookback_userid,
         |if(playType='${GatherType.LIVE_NEW}',userid,null) as f_live_userid,
         |if((playType='${GatherType.TIME_SHIFT_NEW}' or playType='${GatherType.one_key_timeshift}'),userid,null) as f_time_shift_userid,
         |if(playType='${GatherType.DEMAND_NEW}',cast(deviceid as string),null) as f_demand_deviceid,
         |if(playType='${GatherType.LOOK_BACK_NEW}',cast(deviceid as string) ,null) as f_lookback_deviceid,
         |if(playType='${GatherType.LIVE_NEW}',cast(deviceid as string) ,null) as f_live_deviceid,
         |if((playType='${GatherType.TIME_SHIFT_NEW}' or playType='${GatherType.one_key_timeshift}'),cast(deviceid as string),null) as f_time_shift_deviceid,
         |deviceType as f_terminal,regionId as f_region_id
         |from orc_video_play
         |where day='$time'
            """.stripMargin)
    user_basic.join(regionInfo, Seq("f_region_id")).registerTempTable("user_online_rate_day")
    val user_df3 = hiveContext.sql(
      s"""
         |select '$time' as f_date,
         |f_terminal,f_province_id,f_province_name,f_city_id,f_city_name,
         |f_region_id,f_region_name,
         |concat_ws(',',collect_set(f_demand_userid)) as f_demand_user_ids,
         |concat_ws(',',collect_set(f_lookback_userid)) as f_lookback_user_ids,
         |concat_ws(',',collect_set(f_live_userid)) as f_live_user_ids,
         |concat_ws(',',collect_set(f_time_shift_userid)) as f_time_shift_user_ids,
         |concat_ws(',',collect_set(f_demand_deviceid)) as f_demand_device_ids,
         |concat_ws(',',collect_set(f_lookback_deviceid)) as f_lookback_device_ids,
         |concat_ws(',',collect_set(f_live_deviceid)) as f_live_device_ids,
         |concat_ws(',',collect_set(f_time_shift_deviceid)) as f_time_shift_device_ids
         |from user_online_rate_day
         |where f_city_name is not null
         |group by
         |f_region_id,f_terminal,f_province_name,
         |f_province_id,f_city_name,f_city_id,
         |f_region_name
             """.stripMargin)
    val user_df4 = user_df3.map(x => {
      val f_date = x.getAs[String]("f_date")
      val f_terminal = x.getAs[Int]("f_terminal").toString
      val f_province_id = x.getAs[Long]("f_province_id")
      val f_province_name = x.getAs[String]("f_province_name")
      val f_city_id = x.getAs[Long]("f_city_id")
      val f_city_name = x.getAs[String]("f_city_name")
      val f_region_id = x.getAs[Int]("f_region_id")
      val f_region_name = x.getAs[String]("f_region_name")
      val f_demand_user_id_count = getCount(x.getAs[String]("f_demand_user_ids"))
      val f_lookback_user_id_count = getCount(x.getAs[String]("f_lookback_user_ids"))
      val f_live_user_id_count = getCount(x.getAs[String]("f_live_user_ids"))
      val f_time_shift_user_id_count = getCount(x.getAs[String]("f_time_shift_user_ids"))
      val f_demand_device_id_count = getCount(x.getAs[String]("f_demand_device_ids"))
      val f_lookback_device_id_count = getCount(x.getAs[String]("f_lookback_device_ids"))
      val f_live_device_id_count = getCount(x.getAs[String]("f_live_device_ids"))
      val f_time_shift_device_id_count = getCount(x.getAs[String]("f_time_shift_device_ids"))
      UserDevice(f_date, f_terminal, f_province_id, f_province_name, f_city_id, f_city_name, f_region_id, f_region_name, f_demand_user_id_count, f_lookback_user_id_count, f_live_user_id_count, f_time_shift_user_id_count, f_demand_device_id_count, f_lookback_device_id_count, f_live_device_id_count, f_time_shift_device_id_count)
    }).toDF()
    DBUtils.saveToHomedData_2(user_df4, cn.ipanel.common.Tables.t_day_online_rate)
  }

  def getCount(string: String) = {
    if ("" == string || null == string) {
      0
    } else {
      string.split(",").length
    }
  }

}
