package cn.ipanel.homed.realtime

private[realtime] case class Log(DA: Long, channel_id: Long, device_type: String, device_id: String,
                                 report_time: Long, receive_time: Long, keyword: String,
                                 province_id: String, city_id: String, region_id: String,play_type:String,
                                 status:Int)

/**
  * 缓存用户信息
  */
/*private [realtime] case class CachUser(device_id: Long = 0,DA:Long =0, play_type:String="",device_type:String="",
                                       province_id: String = "", city_id: String = "", region_id: String = "",
                                      report_time:Long=0,receive_time:Long=0)*/
/**
  * 用户状态
  */
private[realtime]  case class UserSatus(device_id: Long = 0, channel_id: Long = 0, program_id: Int = 0, report_time: Long = 0,
                     device_type: String = "", province_id: String = "", city_id: String = "", region_id: String = "",
                     DA: Long = 0)

/**
  * 计算结果
  */
private[realtime]  case class Result(province_id: String="", city_id: String="", region_id: String="",
                                     device_type: String="",play_type:String="",user_count:Int=0)