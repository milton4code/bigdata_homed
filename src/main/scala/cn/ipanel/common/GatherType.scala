package cn.ipanel.common

/**
  * 采集类型常量
  *
  * @author liujjy  
  * @date 2017/12/26 10:37
  */

object GatherType {
  //-----------------系统状态码-----------------------
  /** 开机*/
  val SYSTEM_OPEN = "301"
  /** 待机 */
  val SYSTEM_STANDBY = "302"
  /**309 系统故障 */
  val SYSTEM_FAULT  = "309"
  /**701 心跳 */
  val SYSTEM_HEARTBEAT = "701"
  /** 跨天*/
  val SYSTEM_NONE="NONE"


  //--------------业务类型码-------------------------
  // guest 游客
  val GUEST = "guest"
  val GUEST2 = "游客"

  //直播101 时移102 回看103 点播104
  /** 直播 */
  val LIVE ="101"
  /** 时移*/
  val TIME_SHIFT ="102"
  /** 回看*/
  val LOOK_BACK = "103"
  /** 点播*/
  val DEMAND = "104"
  /**栏目131*/
  val PROGRAM_DETAIL="131"

  /** 直播 */
  val LIVE_NEW ="live"
  /** 时移*/
  val TIME_SHIFT_NEW ="timeshift"
  /** 一键时移*/
  val one_key_timeshift ="one-key-timeshift"
  /** 回看*/
  val LOOK_BACK_NEW = "lookback"
  /** 点播*/
  val DEMAND_NEW= "demand"
  // 推荐
  val RECOMMEND_TYPE = "recommend"
  /** 未知频道 */
  val UNKNOWN_CHANNEL="unknown"

  //
  val VOD_TYPE="vod"
  /**搜索*/
  val SEARCH = "122"

  //日志上报类型
  /** 推流日志 */
  val PUSH_LOG = 0
  /** 用户上报日志 */
  val REPORT_LOG = 1
  /** 混合日志 */
  val MIXED_LOG = 2

  /** 各业务场景采取的日志方式 added by lizhy@201903101*/
  val RUN_LOG_GTHER_TYPE = List(DEMAND_NEW,LOOK_BACK_NEW,TIME_SHIFT_NEW)
  val USER_LOG_GATHER_TYPE = List(LIVE_NEW)
  /** 各业务场景采取的日志方式 ended by lizhy@201903101*/

  /** 用户上报业务类别编码 by lizhy@20190304*/
  //心跳0701 直播0101 时移0102 回看0103 点播0104
  /** 心跳 */
  val USER_LOG_HEARTBREATH ="0701"
  /** 直播 */
  val USER_LOG_LIVE ="0101"
  /** 时移*/
  val USER_LOG_TIME_SHIFT ="0102"
  /** 回看*/
  val USER_LOG_LOOK_BACK = "0103"
  /** 点播*/
  val USER_LOG_DEMAND = "0104"
  /** 广告*/
  val USER_LOG_AD = "0121"
  /** 用户上报日志心跳扩展字段业务场景编码S值 by lizhy@20190304*/
  //-----心跳场景	参数S取值-----
  /** 直播	1 */
  val BREAK_SCEN_LIVE = "1"
  /** 时移	2 */
  val BREAK_SCEN_TIEMSFT = "2"
  /** 回看	3 */
  val BREAK_SCEN_LOOKBK = "3"
  /** 点播	4 */
  val BREAK_SCEN_DEMAND = "4"
  /** 应用	5 */
  val BREAK_SCEN_APPL = "5"
  /** 栏目	6  */
  val BREAK_SCEN_PROG = "6"
}
